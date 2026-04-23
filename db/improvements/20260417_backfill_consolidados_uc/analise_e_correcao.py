"""
analise_e_correcao.py
=====================
Melhoria 20260417 — Diagnostico + Backfill seguro + Reset de incertos

Fluxo:
  1. Diagnostico: mostra todos os registros com cliente_uc_id NULL por status.
  2. Fase A (segura): preenche cliente_uc_id onde o cliente tem 1 unica UC
     na distribuidora — sem ambiguidade, 100% correto.
  3. Reset: registros consolidados que continuam sem cliente_uc_id apos
     Fase A sao revertidos para 'pendente' (resposta_id=6) para
     re-entrar no pipeline da macro e serem consultados novamente
     com o par CPF+UC correto.
  4. Resumo final.

Uso:
    python db/improvements/20260417_backfill_consolidados_uc/analise_e_correcao.py --dry-run
    python db/improvements/20260417_backfill_consolidados_uc/analise_e_correcao.py
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP = "=" * 70


def log(msg: str = ""):
    print(msg)


# ───────────────────────────────────────────────────────────────────────────
# SQL
# ───────────────────────────────────────────────────────────────────────────

SQL_DIAG_POR_STATUS = """
    SELECT
        status,
        COUNT(*)                           AS total,
        SUM(cliente_uc_id IS NOT NULL)     AS com_uc,
        SUM(cliente_uc_id IS NULL)         AS sem_uc
    FROM tabela_macros
    GROUP BY status
    ORDER BY FIELD(status, 'pendente','processando','reprocessar','consolidado','excluido')
"""

# Fase A: preencher onde cliente tem 1 unica UC por distribuidora (todos os status)
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
"""

# Reset: consolidados que continuam sem UC -> pendente
SQL_COUNT_CONSOLIDADOS_SEM_UC = """
    SELECT COUNT(*)
    FROM tabela_macros
    WHERE status = 'consolidado'
      AND cliente_uc_id IS NULL
"""

SQL_RESET_CONSOLIDADOS_SEM_UC = """
    UPDATE tabela_macros
    SET status      = 'pendente',
        resposta_id = 6,
        data_update = NOW()
    WHERE status = 'consolidado'
      AND cliente_uc_id IS NULL
"""

# Resumo final
SQL_DIAG_FINAL = SQL_DIAG_POR_STATUS


# ───────────────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────────────

def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260417 — Backfill seguro + Reset de consolidados incertos")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    conn.autocommit(False)
    cur = conn.cursor()

    # ── 1. Diagnostico ──────────────────────────────────────────────────
    log()
    log("[1/4] Diagnostico: registros com cliente_uc_id NULL por status")
    log("-" * 60)
    cur.execute(SQL_DIAG_POR_STATUS)
    rows = cur.fetchall()
    total_geral = 0
    total_sem_uc = 0
    log(f"  {'Status':<15} {'Total':>10} {'Com UC':>10} {'Sem UC':>10}")
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    for status, total, com_uc, sem_uc in rows:
        log(f"  {status:<15} {total:>10,} {com_uc:>10,} {sem_uc:>10,}")
        total_geral += total
        total_sem_uc += sem_uc
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    log(f"  {'TOTAL':<15} {total_geral:>10,} {total_geral - total_sem_uc:>10,} {total_sem_uc:>10,}")

    if total_sem_uc == 0:
        log()
        log("  Nenhum registro com cliente_uc_id NULL. Nada a fazer.")
        cur.close()
        conn.close()
        return

    # ── 2. Fase A: backfill seguro (UC unica) — TODOS os status ────────
    log()
    log("[2/4] Fase A: backfill seguro — clientes com 1 unica UC por distribuidora")
    log("      (aplica a TODOS os status, nao apenas consolidado)")
    log("-" * 60)
    cur.execute(SQL_COUNT_SINGLE_UC)
    count_single = cur.fetchone()[0]
    log(f"  Registros elegiveis: {count_single:,}")

    if count_single > 0 and not dry_run:
        cur.execute(SQL_BACKFILL_SINGLE_UC)
        log(f"  Atualizados: {cur.rowcount:,}")
        conn.commit()
    elif count_single > 0:
        log(f"  [DRY-RUN] Seria atualizado: {count_single:,}")

    # Diagnostico pos-fase A
    log()
    log("  Estado apos Fase A:")
    cur.execute(SQL_DIAG_POR_STATUS)
    rows = cur.fetchall()
    log(f"  {'Status':<15} {'Total':>10} {'Com UC':>10} {'Sem UC':>10}")
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    for status, total, com_uc, sem_uc in rows:
        log(f"  {status:<15} {total:>10,} {com_uc:>10,} {sem_uc:>10,}")

    # ── 3. Reset: consolidados que continuam sem UC → pendente ──────────
    log()
    log("[3/4] Reset: consolidados sem cliente_uc_id -> pendente")
    log("      (vao re-entrar no pipeline e ser consultados com CPF+UC correto)")
    log("-" * 60)
    cur.execute(SQL_COUNT_CONSOLIDADOS_SEM_UC)
    count_reset = cur.fetchone()[0]
    log(f"  Consolidados sem UC restantes: {count_reset:,}")

    if count_reset > 0 and not dry_run:
        cur.execute(SQL_RESET_CONSOLIDADOS_SEM_UC)
        log(f"  Revertidos para 'pendente': {cur.rowcount:,}")
        conn.commit()
    elif count_reset > 0:
        log(f"  [DRY-RUN] Seria revertido: {count_reset:,}")

    # ── 4. Resumo final ─────────────────────────────────────────────────
    log()
    log("[4/4] Resumo final")
    log("-" * 60)
    cur.execute(SQL_DIAG_FINAL)
    rows = cur.fetchall()
    total_g = 0
    total_su = 0
    log(f"  {'Status':<15} {'Total':>10} {'Com UC':>10} {'Sem UC':>10}")
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    for status, total, com_uc, sem_uc in rows:
        log(f"  {status:<15} {total:>10,} {com_uc:>10,} {sem_uc:>10,}")
        total_g += total
        total_su += sem_uc
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    log(f"  {'TOTAL':<15} {total_g:>10,} {total_g - total_su:>10,} {total_su:>10,}")

    log()
    log(f"  Backfill seguro (Fase A):   {count_single:,} registros preenchidos")
    log(f"  Consolidados resetados:     {count_reset:,} registros -> pendente")
    if total_su > 0:
        log(f"  Ainda sem UC (outros status): {total_su:,} (pendente/reprocessar serao")
        log(f"    resolvidos automaticamente pelo 03_buscar_lote quando re-processados)")

    cur.close()
    conn.close()

    log()
    log(SEP)
    log("CONCLUIDO")
    log(SEP)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill seguro + reset consolidados incertos"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem gravar nada no banco")
    args = parser.parse_args()
    run(args.dry_run)
