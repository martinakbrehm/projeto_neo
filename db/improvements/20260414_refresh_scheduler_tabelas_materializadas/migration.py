"""
Migration: 20260414_refresh_scheduler_tabelas_materializadas
Documenta e registra a estratégia de refresh seguro das tabelas materializadas
usadas pelo dashboard de macros.

Contexto:
  Problema original: o dashboard executava queries pesadas (ROW_NUMBER + JOINs em
  tabelas com ~200K+ registros) diretamente a cada callback do Dash, travando o
  carregamento por 2+ minutos. Tentativas de popular tabelas materializadas
  resultaram em queries órfãs no RDS (13 queries acumuladas com metadata locks
  em cascata), sobrecarregando o banco.

Solução implementada:
  1. Tabelas materializadas (já criadas por migrations anteriores):
     - dashboard_macros_agg    → 135 linhas, atualizada via sp_refresh_dashboard_macros_agg
     - dashboard_arquivos_agg  → 4 arquivos, atualizada via temp table + INSERT

  2. Scheduler seguro (dashboard_macros/refresh_scheduler.py):
     - Loop configurável (padrão 1h) ou execução única (--once)
     - Lock file impede execução simultânea (verifica PID do processo)
     - Limpeza automática de queries órfãs via SHOW PROCESSLIST + KILL
     - Timeouts explícitos: connect_timeout=10, read_timeout=120-180s
     - Logging completo com timestamp para auditoria

  3. Estratégia de refresh do dashboard_arquivos_agg:
     a) CREATE TEMPORARY TABLE tmp_cpf_status SEM índice
     b) Bulk INSERT dos ~196K registros (rápido sem manutenção de índice)
     c) ALTER TABLE ADD INDEX após bulk (uma única passagem — O(n log n))
     d) TRUNCATE + INSERT INTO dashboard_arquivos_agg via JOIN com temp indexada
     Tempo total: ~10s

  4. Integração com ETL:
     - etl/load/macro/04_processar_retorno_macro.py chama refresh_dashboard_macros_agg()
     - Scheduler roda independente para manter ambas as tabelas atualizadas

Modos de uso:
    python -m dashboard_macros.refresh_scheduler              # loop 1h
    python -m dashboard_macros.refresh_scheduler --once       # única vez
    python -m dashboard_macros.refresh_scheduler --interval N # a cada N segundos

Arquivos criados/modificados:
    - dashboard_macros/refresh_scheduler.py   (novo — scheduler com proteções)
    - dashboard_macros/iniciar_refresh.bat    (novo — atalho Windows)
    - dashboard_macros/data/loader.py         (atualizado — refresh_dashboard_arquivos_agg
                                               delegado ao refresh_scheduler)
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from config import db_destino
import pymysql


def verificar_estado():
    """Verifica o estado atual das tabelas materializadas e do banco."""
    conn = pymysql.connect(**db_destino(), connect_timeout=10, read_timeout=30)
    cur = conn.cursor()

    print("=" * 60)
    print("Verificação: Tabelas Materializadas + Saúde do Banco")
    print("=" * 60)

    # 1. Verificar tabelas existem
    for tabela in ("dashboard_macros_agg", "dashboard_arquivos_agg"):
        cur.execute(f"SHOW TABLES LIKE %s", (tabela,))
        existe = bool(cur.fetchone())
        if existe:
            cur.execute(f"SELECT COUNT(*) FROM {tabela}")
            n = cur.fetchone()[0]
            cur.execute(
                f"SELECT MAX(atualizado_em) FROM {tabela}"
            )
            ultima = cur.fetchone()[0]
            print(f"  {tabela}: {n} linhas (última atualização: {ultima})")
        else:
            print(f"  {tabela}: NÃO EXISTE — execute as migrations anteriores")

    # 2. Verificar queries ativas
    print()
    cur.execute("SHOW PROCESSLIST")
    processos = cur.fetchall()
    queries_longas = [
        p for p in processos
        if p[4] == "Query" and int(p[5]) > 30
    ]
    print(f"  Conexões ativas: {len(processos)}")
    print(f"  Queries longas (>30s): {len(queries_longas)}")
    for p in queries_longas:
        print(f"    pid={p[0]} time={p[5]}s info={str(p[7])[:80]}")

    # 3. Verificar stored procedure existe
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.ROUTINES "
        "WHERE ROUTINE_NAME = 'sp_refresh_dashboard_macros_agg' "
        "AND ROUTINE_SCHEMA = DATABASE()"
    )
    proc_macros = cur.fetchone()[0]
    print(f"\n  sp_refresh_dashboard_macros_agg: {'OK' if proc_macros else 'NÃO EXISTE'}")

    # 4. Verificar lock file do scheduler
    from pathlib import Path
    lock_file = (
        Path(__file__).resolve().parents[3]
        / "dashboard_macros"
        / ".refresh_scheduler.lock"
    )
    if lock_file.exists():
        pid = lock_file.read_text().strip()
        print(f"\n  Lock file: ATIVO (PID {pid})")
    else:
        print(f"\n  Lock file: livre (scheduler não está rodando)")

    conn.close()
    print("\n" + "=" * 60)


def executar_refresh_unico():
    """Executa um refresh único delegando ao scheduler."""
    print("\nExecutando refresh único via scheduler...")
    from dashboard_macros.refresh_scheduler import executar_refresh
    ok = executar_refresh()
    if ok:
        print("Refresh concluído com sucesso.")
    else:
        print("Refresh concluído com falhas. Verifique os logs acima.")
    return ok


def run(dry_run: bool = False):
    verificar_estado()

    if not dry_run:
        executar_refresh_unico()
    else:
        print("\n[DRY-RUN] Nenhuma alteração aplicada.")
        print("Para executar o refresh: python migration.py")
        print("Para iniciar o scheduler em loop: python -m dashboard_macros.refresh_scheduler")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Verifica e opcionalmente executa refresh das tabelas materializadas"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Apenas verifica estado, sem executar refresh")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
