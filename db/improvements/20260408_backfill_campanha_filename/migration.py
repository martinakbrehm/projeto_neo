"""
migration.py
============
Melhoria 20260408 — Backfill de cliente_origem.campanha com o nome real do arquivo

Problema:
    Registros importados via 02_processar_staging.py tinham campanha='operacional'
    (valor fixo). O nome real do arquivo de origem ficava apenas em staging_imports.filename
    mas não era propagado para cliente_origem.

O que este script faz:
    Para cada cliente em cliente_origem com campanha='operacional',
    busca o staging_import mais recente cujas linhas contenham o CPF do cliente
    e atualiza campanha com o filename real (ex: 'clientes_06-04-2026.xlsx').

    Registros sem correspondência em staging_imports ficam como 'operacional' (não tocados).

Rollback:
    UPDATE cliente_origem SET campanha='operacional'
    WHERE campanha NOT IN ('operacional', 'Dados históricos');

Uso:
    python db/improvements/20260408_backfill_campanha_filename/migration.py
    python db/improvements/20260408_backfill_campanha_filename/migration.py --dry-run
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
# ---------------------------------------------------------------------------
# Contagem pre-execucao
# ---------------------------------------------------------------------------
SQL_COUNT = """
    SELECT COUNT(DISTINCT co.id)
    FROM cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN (
        SELECT normalized_cpf FROM staging_import_rows
        WHERE validation_status = 'valid'
        GROUP BY normalized_cpf
    ) latest ON latest.normalized_cpf = cl.cpf
    WHERE co.campanha = 'operacional'
      AND co.fornecedor = 'fornecedor2'
"""

SQL_SAMPLE = """
    SELECT co.id, co.cliente_id,
           SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -1) AS nome_arquivo
    FROM cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN (
        SELECT sir.normalized_cpf,
               si2.filename,
               ROW_NUMBER() OVER (PARTITION BY sir.normalized_cpf ORDER BY sir.id DESC) AS rn
        FROM staging_import_rows sir
        JOIN staging_imports si2 ON si2.id = sir.staging_id
        WHERE sir.validation_status = 'valid'
    ) si ON si.normalized_cpf = cl.cpf AND si.rn = 1
    WHERE co.campanha = 'operacional'
      AND co.fornecedor = 'fornecedor2'
    LIMIT 10
"""

SQL_CONTAGEM_POR_ARQUIVO = """
    SELECT
        SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -1) AS nome_arquivo,
        COUNT(DISTINCT co.id) AS qtd
    FROM cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN (
        SELECT sir.normalized_cpf, si2.filename,
               ROW_NUMBER() OVER (PARTITION BY sir.normalized_cpf ORDER BY sir.id DESC) AS rn
        FROM staging_import_rows sir
        JOIN staging_imports si2 ON si2.id = sir.staging_id
        WHERE sir.validation_status = 'valid'
    ) si ON si.normalized_cpf = cl.cpf AND si.rn = 1
    WHERE co.campanha = 'operacional'
      AND co.fornecedor = 'fornecedor2'
    GROUP BY nome_arquivo
    ORDER BY qtd DESC
"""

# UPDATE unico no banco -- sem round-trips Python, sem batches, atomico
SQL_UPDATE_DIRETO = """
    UPDATE cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN (
        SELECT sir.normalized_cpf,
               SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -1) AS nome_arquivo
        FROM staging_import_rows sir
        JOIN staging_imports si ON si.id = sir.staging_id
        WHERE sir.validation_status = 'valid'
          AND sir.id IN (
              SELECT MAX(id) FROM staging_import_rows
              WHERE validation_status = 'valid'
              GROUP BY normalized_cpf
          )
    ) latest ON latest.normalized_cpf = cl.cpf
    SET co.campanha = latest.nome_arquivo
    WHERE co.campanha = 'operacional'
      AND co.fornecedor = 'fornecedor2'
"""


def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260408 -- Backfill campanha -> filename real")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30, read_timeout=600, write_timeout=600)
    cur  = conn.cursor()

    log("\n[1/3] Contando registros para atualizar...")
    cur.execute(SQL_COUNT)
    total = cur.fetchone()[0]
    log(f"      Total: {total:,} registros")

    if total == 0:
        log("\nNenhum registro para atualizar. Tudo ja foi migrado.")
        cur.close(); conn.close()
        return

    log("\n[2/3] Amostra (primeiros 10):")
    cur.execute(SQL_SAMPLE)
    for origem_id, cliente_id, nome in cur.fetchall():
        log(f"    id={origem_id}  cliente={cliente_id}  -> '{nome}'")

    log("\n[3/3] Contagem por arquivo:")
    cur.execute(SQL_CONTAGEM_POR_ARQUIVO)
    for nome, qtd in cur.fetchall():
        log(f"    {qtd:>8,}  {nome}")

    if dry_run:
        log(f"\nDry-run concluido. {total:,} registros seriam atualizados.")
        log("Execute sem --dry-run para aplicar.")
        cur.close(); conn.close()
        log(SEP)
        return

    log(f"\n[UPDATE] Executando UPDATE unico no banco ({total:,} registros)...")
    cur.execute(SQL_UPDATE_DIRETO)
    conn.commit()
    affected = cur.rowcount
    log(f"  OK  {affected:,} linhas atualizadas.")

    cur.close()
    conn.close()
    log(SEP)
    log("Migration concluida com sucesso.")
    log(SEP)


if __name__ == "__main__":
    import traceback
    parser = argparse.ArgumentParser(description="Backfill campanha - filename real")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar o banco")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
