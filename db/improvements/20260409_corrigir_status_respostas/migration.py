"""
migration.py
============
Melhoria 20260409 — Corrigir inconsistências de status entre `respostas` e `tabela_macros`

Inconsistências encontradas:

  1. Tabela `respostas`: ids 0,1,2,7,8 têm status='excluir' (verbo),
     mas `tabela_macros.status` usa 'excluido' (particípio).
     Corrigir: UPDATE respostas SET status='excluido' WHERE status='excluir'

  2. 11.173 registros em `tabela_macros` com status='reprocessar' mas
     resposta_id apontando para respostas pendentes (id=6 'Aguardando processamento'
     e id=11 'ERRO'). Esses registros foram reprocessados manualmente mas o
     resposta_id ficou desatualizado.
     Corrigir: SET resposta_id=NULL para esses casos (sem resposta definitiva ainda)

Rollback:
    UPDATE respostas SET status='excluir' WHERE status='excluido';
    -- (resposta_id NULL não tem rollback automático)

Uso:
    python db/improvements/20260409_corrigir_status_respostas/migration.py
    python db/improvements/20260409_corrigir_status_respostas/migration.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP = "=" * 70

SQL_COUNT_RESPOSTAS = """
    SELECT COUNT(*) FROM respostas WHERE status = 'excluir'
"""

SQL_FIX_RESPOSTAS = """
    UPDATE respostas SET status = 'excluido' WHERE status = 'excluir'
"""

SQL_COUNT_MACRO_INCOERENTE = """
    SELECT r.id, r.mensagem, COUNT(*) as qtd
    FROM tabela_macros tm
    JOIN respostas r ON r.id = tm.resposta_id
    WHERE tm.status = 'reprocessar'
      AND r.status = 'pendente'
    GROUP BY r.id, r.mensagem
"""

SQL_FIX_MACRO_INCOERENTE = """
    UPDATE tabela_macros tm
    JOIN respostas r ON r.id = tm.resposta_id
    SET tm.resposta_id = NULL
    WHERE tm.status = 'reprocessar'
      AND r.status = 'pendente'
"""


def log(msg: str):
    print(msg)


def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260409 -- Corrigir status inconsistentes em respostas e tabela_macros")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    cur  = conn.cursor()

    # --- Passo 1: respostas.status 'excluir' -> 'excluido' ---
    log("\n[1/2] Respostas com status='excluir' a corrigir para 'excluido':")
    cur.execute(SQL_COUNT_RESPOSTAS)
    qtd_respostas = cur.fetchone()[0]
    if qtd_respostas == 0:
        log("      Nenhuma — ja corrigido.")
    else:
        cur.execute("SELECT id, mensagem, status FROM respostas WHERE status = 'excluir'")
        for r in cur.fetchall():
            log(f"      id={r[0]}  '{r[1]}'  status={r[2]} -> 'excluido'")

    # --- Passo 2: tabela_macros reprocessar + resposta pendente ---
    log("\n[2/2] tabela_macros com status='reprocessar' e resposta_id pendente:")
    cur.execute(SQL_COUNT_MACRO_INCOERENTE)
    rows = cur.fetchall()
    total_macro = sum(r[2] for r in rows)
    if total_macro == 0:
        log("      Nenhuma — ja corrigido.")
    else:
        for r in rows:
            log(f"      resposta_id={r[0]}  '{r[1]}'  -> {r[2]:,} registros terão resposta_id=NULL")

    if dry_run:
        log(f"\nDry-run concluido.")
        log(f"  - {qtd_respostas} respostas seriam corrigidas")
        log(f"  - {total_macro:,} registros em tabela_macros teriam resposta_id zerado")
        log("Execute sem --dry-run para aplicar.")
        cur.close(); conn.close()
        log(SEP)
        return

    if qtd_respostas > 0:
        cur.execute(SQL_FIX_RESPOSTAS)
        log(f"\n  OK  {cur.rowcount} respostas corrigidas (excluir -> excluido).")

    if total_macro > 0:
        cur.execute(SQL_FIX_MACRO_INCOERENTE)
        log(f"  OK  {cur.rowcount:,} registros de tabela_macros com resposta_id=NULL.")

    conn.commit()
    cur.close()
    conn.close()
    log(SEP)
    log("Migration concluida com sucesso.")
    log(SEP)


if __name__ == "__main__":
    import traceback
    parser = argparse.ArgumentParser(description="Corrigir status inconsistentes")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar o banco")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
