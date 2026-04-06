"""
setup_database.py
=================
Cria e popula o banco de dados a partir do schema.sql, respeitando
a ordem de dependência entre tabelas, views, procedures e triggers.

Uso:
    python setup_database.py            # aplica schema + seeds
    python setup_database.py --dry-run  # exibe os statements sem executar
    python setup_database.py --drop-all # remove todos os objetos antes de recriar (CUIDADO!)
"""

import re
import sys
import argparse
import pymysql
from pathlib import Path
from pymysql.constants import CLIENT

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Credenciais / conexão
# ---------------------------------------------------------------------------
DB_CONFIG = db_destino(autocommit=True, client_flag=CLIENT.MULTI_STATEMENTS)

SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"

# ---------------------------------------------------------------------------
# Pré-processador de DELIMITER
# ---------------------------------------------------------------------------
# Statements que devem ser ignorados porque o banco já está selecionado
_SKIP_PATTERNS = (
    re.compile(r"CREATE\s+DATABASE\b", re.IGNORECASE),
    re.compile(r"\bUSE\b", re.IGNORECASE),
)


def preprocess_schema(sql: str) -> list[str]:
    """
    Divide o SQL em statements individuais tratando DELIMITER corretamente.
    Ignora CREATE DATABASE e USE <db>.
    """
    statements: list[str] = []
    delimiter = ";"
    buf = ""

    for line in sql.splitlines():
        stripped = line.strip()

        # Detecta mudança de DELIMITER (ex: DELIMITER $$ ou DELIMITER ;)
        m = re.match(r"^DELIMITER\s+(\S+)\s*$", stripped, re.IGNORECASE)
        if m:
            if buf.strip():
                statements.append(buf.strip())
            buf = ""
            delimiter = m.group(1)
            continue

        buf += line + "\n"

        # Verifica se chegamos ao fim de um statement
        if stripped.endswith(delimiter):
            stmt = buf.strip()
            # Se o delimitador não é ';', remove o delimitador customizado do final
            if delimiter != ";":
                stmt = stmt[: -len(delimiter)].rstrip()
            if stmt:
                statements.append(stmt)
            buf = ""

    # Qualquer resto
    if buf.strip():
        statements.append(buf.strip())

    # Filtrar statements indesejados e vazios/comentários
    cleaned: list[str] = []
    for stmt in statements:
        # Pula CREATE DATABASE e USE <db> em qualquer posição (comentários podem preceder)
        if any(p.search(stmt) for p in _SKIP_PATTERNS):
            continue
        # Pula statements que são apenas comentários ou espaços
        body = "\n".join(
            ln for ln in stmt.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        )
        if body.strip():
            cleaned.append(stmt)

    return cleaned


def reorder_statements(statements: list[str]) -> list[str]:
    """
    Reordena statements para reduzir erros de dependência:
      1) CREATE TABLE
      2) ALTER TABLE / CREATE INDEX
      3) INSERT/UPSERT
      4) CREATE VIEW
      5) CREATE PROCEDURE/FUNCTION
      6) CREATE TRIGGER/EVENT/OTHER
    """
    buckets = {
        "create_table": [],
        "alter_index": [],
        "inserts": [],
        "views": [],
        "procs": [],
        "triggers": [],
        "other": [],
    }

    def kind_of(stmt: str) -> str:
        body = "\n".join(ln for ln in stmt.splitlines() if ln.strip() and not ln.strip().startswith("--")).strip().lower()
        if body.startswith("create table"):
            return "create_table"
        if body.startswith("alter table") or body.startswith("create index") or "index" in body and body.startswith("alter"):
            return "alter_index"
        if body.startswith("insert into") or body.startswith("replace into"):
            return "inserts"
        if body.startswith("create view") or body.startswith("create or replace view"):
            return "views"
        if body.startswith("create procedure") or body.startswith("create function"):
            return "procs"
        if body.startswith("create trigger") or body.startswith("create event"):
            return "triggers"
        return "other"

    for stmt in statements:
        buckets[kind_of(stmt)].append(stmt)

    ordered = (
        buckets["create_table"]
        + buckets["alter_index"]
        + buckets["inserts"]
        + buckets["views"]
        + buckets["procs"]
        + buckets["triggers"]
        + buckets["other"]
    )
    return ordered


# ---------------------------------------------------------------------------
# Drop de todos os objetos (opcional, para reset completo)
# ---------------------------------------------------------------------------
DROP_ORDER = [
    # Triggers
    "DROP TRIGGER IF EXISTS before_insert_clientes",
    "DROP TRIGGER IF EXISTS before_update_clientes",
    "DROP TRIGGER IF EXISTS trg_after_insert_tabela_macros",
    # Views
    "DROP VIEW IF EXISTS view_consolidados_unificado",
    "DROP VIEW IF EXISTS view_macros_automacao",
    "DROP VIEW IF EXISTS view_macro_api_automacao",
    "DROP VIEW IF EXISTS view_macros_finalizados",
    # Procedures
    "DROP PROCEDURE IF EXISTS get_macro_api_batch",
    "DROP PROCEDURE IF EXISTS get_macros_automacao_batch",
    "DROP PROCEDURE IF EXISTS proc_macro_api_link_uc",
    "DROP PROCEDURE IF EXISTS extrair_finalizados",
    # Tabelas na ordem inversa de dependência
    "DROP TABLE IF EXISTS staging_import_rows",
    "DROP TABLE IF EXISTS staging_imports",
    "DROP TABLE IF EXISTS tabela_macro_api",
    "DROP TABLE IF EXISTS tabela_macros",
    "DROP TABLE IF EXISTS enderecos",
    "DROP TABLE IF EXISTS telefones",
    "DROP TABLE IF EXISTS cliente_uc",
    "DROP TABLE IF EXISTS clientes",
    "DROP TABLE IF EXISTS respostas",
    "DROP TABLE IF EXISTS distribuidoras",
    "DROP TABLE IF EXISTS tabela_macros_finalizados",
]

# ---------------------------------------------------------------------------
# Seeds independentes das já inclusas no schema.sql
# (distribuidoras e respostas estão no próprio schema via INSERT ... ON DUPLICATE)
# ---------------------------------------------------------------------------
EXTRA_SEEDS: list[tuple[str, tuple]] = [
    # distribuidoras (garante IDs fixos mesmo se o schema falhar no INSERT)
    (
        "INSERT INTO distribuidoras (id, nome) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nome = VALUES(nome)",
        (1, "coelba"),
    ),
    (
        "INSERT INTO distribuidoras (id, nome) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nome = VALUES(nome)",
        (2, "cosern"),
    ),
    (
        "INSERT INTO distribuidoras (id, nome) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nome = VALUES(nome)",
        (3, "celpe"),
    ),
    (
        "INSERT INTO distribuidoras (id, nome) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nome = VALUES(nome)",
        (4, "brasilia"),
    ),
    # respostas base (ids 1-5 não estão no schema.sql)
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (0, "Conta Contrato não existe", "excluir"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (1, "Doc. fiscal não existe", "excluir"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (2, "Titularidade não confirmada", "excluir"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (3, "Titularidade confirmada com contrato ativo", "consolidado"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (4, "Titularidade confirmada com contrato inativo", "reprocessar"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (5, "Titularidade confirmada com inst. suspensa", "reprocessar"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (6, "Aguardando processamento", "pendente"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (7, "Doc. Fiscal nao cadastrado no SAP", "excluir"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (8, "Parceiro informado nao possui conta contrato", "excluir"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (9, "Status instalacao: desligado", "reprocessar"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (10, "Status instalacao: ligado", "consolidado"),
    ),
    (
        "INSERT INTO respostas (id, mensagem, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status)",
        (11, "ERRO", "pendente"),
    ),
]


# ---------------------------------------------------------------------------
# Runner principal
# ---------------------------------------------------------------------------
def get_connection():
    return pymysql.connect(**DB_CONFIG)


def run_drop_all(cursor, dry_run: bool):
    print("\n[DROP] Removendo objetos existentes...")
    for stmt in DROP_ORDER:
        if dry_run:
            print(f"  [DRY] {stmt}")
        else:
            try:
                cursor.execute(stmt)
                print(f"  [OK ] {stmt.split('EXISTS')[-1].strip()}")
            except Exception as e:
                print(f"  [ERR] {stmt[:60]} → {e}")


def run_schema(cursor, statements: list[str], dry_run: bool):
    # Reorder statements to satisfy dependencies (tables -> alters/indexes -> inserts -> views/procs)
    statements = reorder_statements(statements)
    print(f"\n[SCHEMA] Aplicando {len(statements)} statements (reordenados)...")
    errors = []
    for i, stmt in enumerate(statements, 1):
        preview = stmt.replace("\n", " ")[:80]
        if dry_run:
            print(f"  [DRY #{i:03d}] {preview}")
            continue
        try:
            cursor.execute(stmt)
            print(f"  [OK  #{i:03d}] {preview}")
        except pymysql.err.OperationalError as e:
            code = e.args[0]
            # Ignorar "já existe" para objetos idempotentes
            if code in (1050, 1060, 1061, 1062, 1091, 1293, 1304, 1359, 1826):
                print(f"  [SKIP #{i:03d}] (ja existe) {preview}")
            else:
                print(f"  [ERR  #{i:03d}] {e}\n           SQL: {preview}")
                errors.append((i, str(e), stmt))
        except Exception as e:
            print(f"  [ERR  #{i:03d}] {e}\n           SQL: {preview}")
            errors.append((i, str(e), stmt))
    return errors


def run_seeds(cursor, dry_run: bool):
    print(f"\n[SEEDS] Inserindo {len(EXTRA_SEEDS)} registros base...")
    for sql, params in EXTRA_SEEDS:
        if dry_run:
            print(f"  [DRY] {sql % params}")
            continue
        try:
            cursor.execute(sql, params)
            print(f"  [OK ] {sql.split('INTO')[1].split('(')[0].strip()} id={params[0]}")
        except Exception as e:
            print(f"  [ERR] {e}")


def main():
    parser = argparse.ArgumentParser(description="Aplica schema.sql no banco remoto.")
    parser.add_argument("--dry-run", action="store_true", help="Exibe statements sem executar")
    parser.add_argument("--drop-all", action="store_true", help="Remove todos os objetos antes de recriar (CUIDADO!)")
    args = parser.parse_args()

    # Carregar schema
    try:
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            raw_sql = f.read()
    except FileNotFoundError:
        print(f"[ERRO] Arquivo {SCHEMA_FILE} não encontrado. Execute na mesma pasta.")
        sys.exit(1)

    statements = preprocess_schema(raw_sql)
    print(f"[INFO] {len(statements)} statements extraídos de {SCHEMA_FILE}")
    print(f"[INFO] Destino: {DB_CONFIG['host']} -> {DB_CONFIG['database']}")

    if args.dry_run:
        print("[INFO] Modo DRY-RUN — nenhuma alteração será feita.\n")

    # Conectar
    if not args.dry_run:
        try:
            conn = get_connection()
            print(f"[INFO] Conectado com sucesso.\n")
        except Exception as e:
            print(f"[ERRO] Falha ao conectar: {e}")
            sys.exit(1)
    else:
        conn = None

    cursor = conn.cursor() if conn else None

    try:
        # 1. Drop (opcional)
        if args.drop_all:
            confirm = input("[ATENCAO] Confirma remover TODOS os objetos do banco? [s/N] ")
            if confirm.strip().lower() != "s":
                print("Cancelado.")
                sys.exit(0)
            run_drop_all(cursor, args.dry_run)

        # 2. Schema
        errors = run_schema(cursor, statements, args.dry_run)

        # 3. Seeds complementares
        run_seeds(cursor, args.dry_run)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Relatório final
    print("\n" + "=" * 60)
    if args.dry_run:
        print("DRY-RUN concluído — nada foi alterado.")
    elif errors:
        print(f"Concluído com {len(errors)} erro(s):")
        for idx, msg, stmt in errors:
            print(f"  Statement #{idx}: {msg}")
            print(f"    SQL: {stmt[:120]}")
    else:
        print("[OK] Schema aplicado e seeds inseridos com sucesso.")
    print("=" * 60)


if __name__ == "__main__":
    main()
