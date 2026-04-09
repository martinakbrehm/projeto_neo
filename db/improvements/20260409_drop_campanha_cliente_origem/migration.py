"""
migration.py
============
Melhoria 20260409 — Remover coluna `campanha` de `cliente_origem`

Motivação:
    O campo `campanha` foi criado para identificar o arquivo de origem
    dos clientes, mas essa responsabilidade já é coberta corretamente
    pelas tabelas de staging (staging_imports / staging_import_rows).
    Em `cliente_origem` só faz sentido manter o identificador do fornecedor.

O que este script faz:
    1. Remove a coluna `campanha` de `cliente_origem`
    2. Recria as 12 views que referenciavam `co.campanha`, removendo
       esse campo dos SELECTs

Rollback:
    ALTER TABLE cliente_origem ADD COLUMN campanha VARCHAR(100) DEFAULT NULL;
    -- (e re-executar migration 20260406 para recriar as views)

Uso:
    python db/improvements/20260409_drop_campanha_cliente_origem/migration.py
    python db/improvements/20260409_drop_campanha_cliente_origem/migration.py --dry-run
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
# DDL — remover coluna
# ---------------------------------------------------------------------------

SQL_DROP_COLUMN = "ALTER TABLE cliente_origem DROP COLUMN campanha;"

# ---------------------------------------------------------------------------
# Views — recriar sem campanha
# ---------------------------------------------------------------------------

VIEWS = {
    "view_fornecedor2_macro_automacao": """
        CREATE OR REPLACE VIEW view_fornecedor2_macro_automacao AS
        SELECT vm.*, co.fornecedor
        FROM view_macros_automacao vm
        JOIN cliente_origem co ON co.cliente_id = vm.cliente_id
                               AND co.fornecedor = 'fornecedor2';
    """,
    "view_contatus_macro_automacao": """
        CREATE OR REPLACE VIEW view_contatus_macro_automacao AS
        SELECT vm.*, co.fornecedor
        FROM view_macros_automacao vm
        JOIN cliente_origem co ON co.cliente_id = vm.cliente_id
                               AND co.fornecedor = 'contatus';
    """,
    "view_fornecedor2_macro_consolidados": """
        CREATE OR REPLACE VIEW view_fornecedor2_macro_consolidados AS
        SELECT tm.*, co.fornecedor
        FROM tabela_macros tm
        JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                               AND co.fornecedor = 'fornecedor2'
        WHERE tm.status = 'consolidado';
    """,
    "view_contatus_macro_consolidados": """
        CREATE OR REPLACE VIEW view_contatus_macro_consolidados AS
        SELECT tm.*, co.fornecedor
        FROM tabela_macros tm
        JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                               AND co.fornecedor = 'contatus'
        WHERE tm.status = 'consolidado';
    """,
    "view_fornecedor2_macro": """
        CREATE OR REPLACE VIEW view_fornecedor2_macro AS
        SELECT tm.*, co.fornecedor
        FROM tabela_macros tm
        JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                               AND co.fornecedor = 'fornecedor2';
    """,
    "view_contatus_macro": """
        CREATE OR REPLACE VIEW view_contatus_macro AS
        SELECT tm.*, co.fornecedor
        FROM tabela_macros tm
        JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                               AND co.fornecedor = 'contatus';
    """,
    "view_fornecedor2_api_automacao": """
        CREATE OR REPLACE VIEW view_fornecedor2_api_automacao AS
        SELECT tma.*, co.fornecedor
        FROM view_macro_api_automacao tma
        JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                               AND co.fornecedor = 'fornecedor2';
    """,
    "view_contatus_api_automacao": """
        CREATE OR REPLACE VIEW view_contatus_api_automacao AS
        SELECT tma.*, co.fornecedor
        FROM view_macro_api_automacao tma
        JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                               AND co.fornecedor = 'contatus';
    """,
    "view_fornecedor2_api_consolidados": """
        CREATE OR REPLACE VIEW view_fornecedor2_api_consolidados AS
        SELECT tma.*, co.fornecedor
        FROM tabela_macro_api tma
        JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                               AND co.fornecedor = 'fornecedor2'
        WHERE tma.status = 'consolidado';
    """,
    "view_contatus_api_consolidados": """
        CREATE OR REPLACE VIEW view_contatus_api_consolidados AS
        SELECT tma.*, co.fornecedor
        FROM tabela_macro_api tma
        JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                               AND co.fornecedor = 'contatus'
        WHERE tma.status = 'consolidado';
    """,
    "view_fornecedor2_api": """
        CREATE OR REPLACE VIEW view_fornecedor2_api AS
        SELECT tma.*, co.fornecedor
        FROM tabela_macro_api tma
        JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                               AND co.fornecedor = 'fornecedor2';
    """,
    "view_contatus_api": """
        CREATE OR REPLACE VIEW view_contatus_api AS
        SELECT tma.*, co.fornecedor
        FROM tabela_macro_api tma
        JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                               AND co.fornecedor = 'contatus';
    """,
}


def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260409 -- Drop campanha de cliente_origem + recriar views")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    # Verificar se a coluna ainda existe
    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    cur  = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME   = 'cliente_origem'
          AND COLUMN_NAME  = 'campanha'
    """)
    existe = cur.fetchone()[0]

    if not existe:
        log("\nColuna `campanha` ja nao existe em `cliente_origem`. Nada a fazer.")
        cur.close(); conn.close()
        return

    log(f"\n[1/2] Remover coluna `campanha` de `cliente_origem`")
    log(f"      SQL: {SQL_DROP_COLUMN.strip()}")

    log(f"\n[2/2] Recriar {len(VIEWS)} views sem `campanha`:")
    for v in VIEWS:
        log(f"      - {v}")

    if dry_run:
        log("\nDry-run concluido. Execute sem --dry-run para aplicar.")
        cur.close(); conn.close()
        log(SEP)
        return

    # Executar
    cur.execute(SQL_DROP_COLUMN)
    log("\n  OK  coluna `campanha` removida.")

    for view_name, sql in VIEWS.items():
        cur.execute(sql)
        log(f"  OK  {view_name}")

    conn.commit()
    cur.close()
    conn.close()
    log(SEP)
    log("Migration concluida com sucesso.")
    log(SEP)


if __name__ == "__main__":
    import traceback
    parser = argparse.ArgumentParser(description="Drop campanha de cliente_origem")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar o banco")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
