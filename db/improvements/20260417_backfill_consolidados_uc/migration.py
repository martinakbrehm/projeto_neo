"""
migration.py
============
Melhoria 20260417 — Backfill cliente_uc_id em consolidados

Problema:
    Registros em tabela_macros com status='consolidado' e resposta_id=3
    (Titularidade confirmada com contrato ativo) possuem cliente_uc_id=NULL.

    Isso ocorreu porque:
    1. A coluna cliente_uc_id foi adicionada (20260410) após a importação
       dos dados históricos — o backfill daquela migração não resolveu
       todos os registros ambíguos (clientes com múltiplas UCs).
    2. O 04_processar_retorno_macro.py copiava cliente_uc_id do registro
       original (que podia ser NULL) ao invés de resolver a UC que foi
       efetivamente consultada na API.

    A correção do 04 já foi feita; este script corrige os registros
    históricos existentes.

O que este script faz:
    Fase única — Clientes com 1 única UC na distribuidora:
        Vincula diretamente (resolução 100% segura, sem ambiguidade).
    Registros de clientes com múltiplas UCs ficam com NULL — devem ser
    corrigidos via re-importação dos arquivos originais de entrada
    (que contêm os pares CPF+UC reais).
    Relatório — Imprime quantos foram resolvidos e quantos permanecem NULL.

Impacto:
    Apenas UPDATEs em tabela_macros.cliente_uc_id (nullable).
    Sem risco de perda de dados — campos não-NULL não são alterados.

Uso:
    python db/improvements/20260417_backfill_consolidados_uc/migration.py
    python db/improvements/20260417_backfill_consolidados_uc/migration.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP = "=" * 70


def log(msg: str):
    print(msg)


# ---------------------------------------------------------------------------
# Diagnóstico inicial
# ---------------------------------------------------------------------------

SQL_DIAGNOSTICO = """
    SELECT
        COUNT(*)                                AS total_consolidados,
        SUM(cliente_uc_id IS NOT NULL)          AS com_uc,
        SUM(cliente_uc_id IS NULL)              AS sem_uc
    FROM tabela_macros
    WHERE status = 'consolidado'
      AND resposta_id = 3
"""

# ---------------------------------------------------------------------------
# Fase única: clientes com exatamente 1 UC por distribuidora (seguro)
# ---------------------------------------------------------------------------

SQL_COUNT_SINGLE_UC = """
    SELECT COUNT(*)
    FROM tabela_macros tm
    JOIN (
        SELECT cliente_id, distribuidora_id, MIN(id) AS uc_id
        FROM cliente_uc
        GROUP BY cliente_id, distribuidora_id
        HAVING COUNT(*) = 1
    ) single ON single.cliente_id      = tm.cliente_id
            AND single.distribuidora_id = tm.distribuidora_id
    WHERE tm.cliente_uc_id IS NULL
      AND tm.status = 'consolidado'
      AND tm.resposta_id = 3
"""

SQL_BACKFILL_SINGLE_UC = """
    UPDATE tabela_macros tm
    JOIN (
        SELECT cliente_id, distribuidora_id, MIN(id) AS uc_id
        FROM cliente_uc
        GROUP BY cliente_id, distribuidora_id
        HAVING COUNT(*) = 1
    ) single ON single.cliente_id      = tm.cliente_id
            AND single.distribuidora_id = tm.distribuidora_id
    SET tm.cliente_uc_id = single.uc_id
    WHERE tm.cliente_uc_id IS NULL
      AND tm.status = 'consolidado'
      AND tm.resposta_id = 3
"""

# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------

SQL_RESUMO = """
    SELECT
        COUNT(*)                                AS total_consolidados_r3,
        SUM(cliente_uc_id IS NOT NULL)          AS com_uc,
        SUM(cliente_uc_id IS NULL)              AS sem_uc,
        ROUND(SUM(cliente_uc_id IS NOT NULL) / COUNT(*) * 100, 1) AS pct_vinculado
    FROM tabela_macros
    WHERE status = 'consolidado'
      AND resposta_id = 3
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260417 -- Backfill cliente_uc_id em consolidados (resposta_id=3)")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    conn.autocommit(False)
    cur = conn.cursor()

    # ── Diagnóstico ─────────────────────────────────────────────────────
    log("\n[1/3] Diagnóstico atual...")
    cur.execute(SQL_DIAGNOSTICO)
    total, com_uc, sem_uc = cur.fetchone()
    log(f"    Consolidados (resposta_id=3): {total:,}")
    log(f"    Com cliente_uc_id:            {com_uc:,}")
    log(f"    Sem cliente_uc_id (NULL):     {sem_uc:,}")

    if sem_uc == 0:
        log("\n  Nenhum registro para corrigir. Encerrando.")
        cur.close()
        conn.close()
        return

    # ── Backfill: UC única por distribuidora ────────────────────────────
    log("\n[2/3] Clientes com 1 unica UC por distribuidora (resolucao segura)...")
    cur.execute(SQL_COUNT_SINGLE_UC)
    count_single = cur.fetchone()[0]
    log(f"    Registros elegiveis: {count_single:,}")

    if count_single > 0 and not dry_run:
        cur.execute(SQL_BACKFILL_SINGLE_UC)
        log(f"    Atualizados: {cur.rowcount:,}")
        conn.commit()
    elif count_single > 0:
        log(f"    [DRY-RUN] Seria atualizado: {count_single:,}")

    # ── Resumo final ────────────────────────────────────────────────────
    log(f"\n[3/3] Resumo final...")
    cur.execute(SQL_RESUMO)
    total, com_uc_f, sem_uc_f, pct = cur.fetchone()
    log(f"    Total consolidados (r3):  {total:,}")
    log(f"    Com cliente_uc_id:        {com_uc_f:,}")
    log(f"    Sem cliente_uc_id (NULL): {sem_uc_f:,}")
    log(f"    Percentual vinculado:     {pct}%")

    corrigidos = (com_uc_f or 0) - (com_uc or 0)
    log(f"\n    Registros corrigidos nesta execucao: {corrigidos:,}")
    if sem_uc_f and sem_uc_f > 0:
        log(f"    Restantes sem UC: {sem_uc_f:,} (clientes com multiplas UCs — re-importar dos arquivos originais)")

    cur.close()
    conn.close()

    log(f"\n{SEP}")
    log("MIGRATION 20260417 CONCLUIDA")
    log(SEP)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill cliente_uc_id em consolidados com resposta_id=3"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem gravar nada no banco")
    args = parser.parse_args()
    run(args.dry_run)
