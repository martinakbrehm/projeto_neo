"""
migration.py
============
Melhoria 20260409 — Re-Backfill de cliente_origem.campanha com data/arquivo

Problema:
    Registros foram atualizados anteriormente com o nome completo do arquivo,
    mas agora precisamos apenas da parte do caminho com data da pasta e nome do arquivo final.

O que este script faz:
    Para cada cliente em cliente_origem com campanha já atualizada (não 'operacional' nem 'Dados históricos'),
    busca o staging_import mais recente cujas linhas contenham o CPF do cliente
    e atualiza campanha com a porção do caminho contendo a pasta de data e o
    nome do arquivo final (ex: '06-04-2026/clientes_06-04-2026.xlsx').

    Registros sem correspondência em staging_imports ficam inalterados.

Rollback:
    UPDATE cliente_origem SET campanha='operacional'
    WHERE campanha NOT IN ('operacional', 'Dados históricos');

Uso:
    python db/improvements/20260409_backfill_campanha_filename/migration.py
    python db/improvements/20260409_backfill_campanha_filename/migration.py --dry-run
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
    JOIN staging_imports si ON si.id = (
        SELECT si3.id
        FROM staging_import_rows sir3
        JOIN staging_imports si3 ON si3.id = sir3.staging_id
        WHERE sir3.normalized_cpf = cl.cpf AND sir3.validation_status = 'valid'
        ORDER BY sir3.id DESC
        LIMIT 1
    )
    WHERE co.campanha NOT IN ('operacional', 'Dados históricos')
      AND co.fornecedor = 'fornecedor2'
      AND co.campanha != SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -2)
"""

SQL_SAMPLE = """
    SELECT co.id, co.cliente_id, co.campanha AS campanha_atual,
           SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -2) AS novo_nome
    FROM cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN staging_imports si ON si.id = (
        SELECT si3.id
        FROM staging_import_rows sir3
        JOIN staging_imports si3 ON si3.id = sir3.staging_id
        WHERE sir3.normalized_cpf = cl.cpf AND sir3.validation_status = 'valid'
        ORDER BY sir3.id DESC
        LIMIT 1
    )
    WHERE co.campanha NOT IN ('operacional', 'Dados históricos')
      AND co.fornecedor = 'fornecedor2'
      AND co.campanha != SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -2)
    LIMIT 10
"""

SQL_CONTAGEM_POR_ARQUIVO = """
    SELECT
        SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -2) AS novo_nome,
        COUNT(DISTINCT co.id) AS qtd
    FROM cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN staging_imports si ON si.id = (
        SELECT si3.id
        FROM staging_import_rows sir3
        JOIN staging_imports si3 ON si3.id = sir3.staging_id
        WHERE sir3.normalized_cpf = cl.cpf AND sir3.validation_status = 'valid'
        ORDER BY sir3.id DESC
        LIMIT 1
    )
    WHERE co.campanha NOT IN ('operacional', 'Dados históricos')
      AND co.fornecedor = 'fornecedor2'
      AND co.campanha != SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -2)
    GROUP BY novo_nome
    ORDER BY qtd DESC
"""

# UPDATE unico no banco -- sem round-trips Python, sem batches, atomico
SQL_UPDATE_DIRETO = """
    UPDATE cliente_origem co
    JOIN clientes cl ON cl.id = co.cliente_id
    JOIN (
        SELECT sir.normalized_cpf,
               SUBSTRING_INDEX(REPLACE(si.filename, '\\\\', '/'), '/', -2) AS novo_nome
        FROM staging_import_rows sir
        JOIN staging_imports si ON si.id = sir.staging_id
        JOIN (
            SELECT normalized_cpf, MAX(id) AS max_id
            FROM staging_import_rows
            WHERE validation_status = 'valid'
            GROUP BY normalized_cpf
        ) max_sir ON max_sir.normalized_cpf = sir.normalized_cpf AND max_sir.max_id = sir.id
        WHERE sir.validation_status = 'valid'
    ) latest ON latest.normalized_cpf = cl.cpf
    SET co.campanha = latest.novo_nome
    WHERE co.campanha NOT IN ('operacional', 'Dados históricos')
      AND co.fornecedor = 'fornecedor2'
      AND co.campanha != latest.novo_nome
"""


def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260409 -- Re-Backfill campanha -> pasta-data/filename")
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
    for origem_id, cliente_id, atual, novo in cur.fetchall():
        log(f"    id={origem_id}  cliente={cliente_id}  '{atual}' -> '{novo}'")

    log("\n[3/3] Contagem por novo nome:")
    cur.execute(SQL_CONTAGEM_POR_ARQUIVO)
    for novo, qtd in cur.fetchall():
        log(f"    {qtd:>8,}  {novo}")

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
    parser = argparse.ArgumentParser(description="Re-Backfill campanha - pasta-data/filename")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar o banco")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
