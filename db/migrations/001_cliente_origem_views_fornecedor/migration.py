"""
migration.py
============
Migração 001 — cliente_origem, correção da unique key de cliente_uc
e views separadas por fornecedor.

O que este script faz (em ordem):
  1. Corrige a UNIQUE KEY de `cliente_uc`: (cliente_id, uc) → (cliente_id, uc, distribuidora_id)
  2. Atualiza `proc_macro_api_link_uc` para incluir distribuidora_id no lookup
  3. Cria a tabela `cliente_origem`
  4. Backfill: registra todos os clientes existentes como 'fornecedor2'
  5. Cria views analíticas por fornecedor (automacao + consolidados)

NÃO executa nada nas tabelas de produção além do backfill de cliente_origem.
NÃO modifica arquivos .py do pipeline — ver README.md para as alterações de código necessárias.

Uso:
    python db/migrations/001_cliente_origem_views_fornecedor/migration.py
    python db/migrations/001_cliente_origem_views_fornecedor/migration.py --dry-run
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
# PASSO 1 — Corrigir UNIQUE KEY de cliente_uc
# ---------------------------------------------------------------------------

SQL_CHECK_UNIQUE_KEY = """
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'cliente_uc'
      AND INDEX_NAME = 'ux_cliente_uc'
      AND COLUMN_NAME = 'distribuidora_id';
"""

SQL_DROP_OLD_KEY = "ALTER TABLE cliente_uc DROP INDEX ux_cliente_uc;"

SQL_ADD_NEW_KEY = """
    ALTER TABLE cliente_uc
      ADD UNIQUE KEY ux_cliente_uc (cliente_id, uc, distribuidora_id);
"""

# ---------------------------------------------------------------------------
# PASSO 2 — Atualizar proc_macro_api_link_uc para filtrar por distribuidora_id
#           (evita ambiguidade após a unique key incluir distribuidora_id)
# ---------------------------------------------------------------------------

SQL_DROP_PROC = "DROP PROCEDURE IF EXISTS proc_macro_api_link_uc;"

SQL_CREATE_PROC = """
CREATE PROCEDURE proc_macro_api_link_uc(
  IN p_macro_api_id INT,
  IN p_uc VARCHAR(50),
  IN p_distribuidora_id TINYINT UNSIGNED
)
BEGIN
  DECLARE v_cliente_id INT;
  DECLARE v_cliente_uc_id INT;
  DECLARE v_uc CHAR(10);

  SELECT cliente_id INTO v_cliente_id
  FROM tabela_macro_api
  WHERE id = p_macro_api_id
  LIMIT 1;

  IF v_cliente_id IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'tabela_macro_api id not found';
  END IF;

  SET v_uc = LPAD(REPLACE(p_uc, ' ', ''), 10, '0');

  -- Inclui distribuidora_id no lookup para evitar ambiguidade após
  -- a unique key (cliente_id, uc, distribuidora_id)
  SELECT id INTO v_cliente_uc_id
  FROM cliente_uc
  WHERE cliente_id = v_cliente_id
    AND uc = v_uc
    AND distribuidora_id = p_distribuidora_id
  LIMIT 1;

  IF v_cliente_uc_id IS NULL THEN
    INSERT INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao)
    VALUES (v_cliente_id, v_uc, p_distribuidora_id, CURRENT_TIMESTAMP);
    SET v_cliente_uc_id = LAST_INSERT_ID();
  END IF;

  UPDATE tabela_macro_api
  SET cliente_uc_id     = v_cliente_uc_id,
      distribuidora_id  = IFNULL(distribuidora_id, p_distribuidora_id),
      resposta_id       = IFNULL(resposta_id,
                            (SELECT id FROM respostas
                             WHERE status = 'pendente'
                             ORDER BY id LIMIT 1))
  WHERE id = p_macro_api_id;

  SELECT v_cliente_uc_id AS cliente_uc_id;
END
"""

# ---------------------------------------------------------------------------
# PASSO 3 — Criar tabela cliente_origem
# ---------------------------------------------------------------------------

SQL_CREATE_CLIENTE_ORIGEM = """
CREATE TABLE IF NOT EXISTS cliente_origem (
  id           INT NOT NULL AUTO_INCREMENT,
  cliente_id   INT NOT NULL,
  -- Identificador do fornecedor: 'fornecedor2', 'contatus', etc.
  fornecedor   VARCHAR(50) NOT NULL,
  -- Campanha ou lote de importação: 'operacional', 'periodo_historico', etc.
  campanha     VARCHAR(100) DEFAULT NULL,
  -- Data de referência do dado na origem (opcional, útil para históricos)
  data_ref     DATE DEFAULT NULL,
  data_import  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  -- Um cliente só pode ter uma entrada por fornecedor
  UNIQUE KEY ux_cliente_fornecedor (cliente_id, fornecedor),
  CONSTRAINT fk_origem_cliente
    FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ---------------------------------------------------------------------------
# PASSO 4 — Backfill: todos os clientes existentes são do fornecedor2
# ---------------------------------------------------------------------------

SQL_BACKFILL_FORNECEDOR2 = """
INSERT IGNORE INTO cliente_origem (cliente_id, fornecedor, campanha, data_import)
SELECT id, 'fornecedor2', 'operacional', data_criacao
FROM clientes;
"""

# ---------------------------------------------------------------------------
# PASSO 5 — Views por fornecedor
# ---------------------------------------------------------------------------

# --- tabela_macros ---

SQL_VIEW_F2_MACRO_AUTOMACAO = """
CREATE OR REPLACE VIEW view_fornecedor2_macro_automacao AS
SELECT vm.*, co.fornecedor, co.campanha
FROM view_macros_automacao vm
JOIN cliente_origem co ON co.cliente_id = vm.cliente_id
                       AND co.fornecedor = 'fornecedor2';
"""

SQL_VIEW_CONTATUS_MACRO_AUTOMACAO = """
CREATE OR REPLACE VIEW view_contatus_macro_automacao AS
SELECT vm.*, co.fornecedor, co.campanha
FROM view_macros_automacao vm
JOIN cliente_origem co ON co.cliente_id = vm.cliente_id
                       AND co.fornecedor = 'contatus';
"""

SQL_VIEW_F2_MACRO_CONSOLIDADOS = """
CREATE OR REPLACE VIEW view_fornecedor2_macro_consolidados AS
SELECT tm.*, co.fornecedor, co.campanha
FROM tabela_macros tm
JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                       AND co.fornecedor = 'fornecedor2'
WHERE tm.status = 'consolidado';
"""

SQL_VIEW_CONTATUS_MACRO_CONSOLIDADOS = """
CREATE OR REPLACE VIEW view_contatus_macro_consolidados AS
SELECT tm.*, co.fornecedor, co.campanha
FROM tabela_macros tm
JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                       AND co.fornecedor = 'contatus'
WHERE tm.status = 'consolidado';
"""

# Visão completa (todos os status) por fornecedor — útil para o dashboard
SQL_VIEW_F2_MACRO_FULL = """
CREATE OR REPLACE VIEW view_fornecedor2_macro AS
SELECT tm.*, co.fornecedor, co.campanha
FROM tabela_macros tm
JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                       AND co.fornecedor = 'fornecedor2';
"""

SQL_VIEW_CONTATUS_MACRO_FULL = """
CREATE OR REPLACE VIEW view_contatus_macro AS
SELECT tm.*, co.fornecedor, co.campanha
FROM tabela_macros tm
JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
                       AND co.fornecedor = 'contatus';
"""

# --- tabela_macro_api ---

SQL_VIEW_F2_API_AUTOMACAO = """
CREATE OR REPLACE VIEW view_fornecedor2_api_automacao AS
SELECT tma.*, co.fornecedor, co.campanha
FROM view_macro_api_automacao tma
JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                       AND co.fornecedor = 'fornecedor2';
"""

SQL_VIEW_CONTATUS_API_AUTOMACAO = """
CREATE OR REPLACE VIEW view_contatus_api_automacao AS
SELECT tma.*, co.fornecedor, co.campanha
FROM view_macro_api_automacao tma
JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                       AND co.fornecedor = 'contatus';
"""

SQL_VIEW_F2_API_CONSOLIDADOS = """
CREATE OR REPLACE VIEW view_fornecedor2_api_consolidados AS
SELECT tma.*, co.fornecedor, co.campanha
FROM tabela_macro_api tma
JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                       AND co.fornecedor = 'fornecedor2'
WHERE tma.status = 'consolidado';
"""

SQL_VIEW_CONTATUS_API_CONSOLIDADOS = """
CREATE OR REPLACE VIEW view_contatus_api_consolidados AS
SELECT tma.*, co.fornecedor, co.campanha
FROM tabela_macro_api tma
JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                       AND co.fornecedor = 'contatus'
WHERE tma.status = 'consolidado';
"""

SQL_VIEW_F2_API_FULL = """
CREATE OR REPLACE VIEW view_fornecedor2_api AS
SELECT tma.*, co.fornecedor, co.campanha
FROM tabela_macro_api tma
JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                       AND co.fornecedor = 'fornecedor2';
"""

SQL_VIEW_CONTATUS_API_FULL = """
CREATE OR REPLACE VIEW view_contatus_api AS
SELECT tma.*, co.fornecedor, co.campanha
FROM tabela_macro_api tma
JOIN cliente_origem co ON co.cliente_id = tma.cliente_id
                       AND co.fornecedor = 'contatus';
"""


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------

def executar(cur, sql: str, dry_run: bool, descricao: str):
    if dry_run:
        log(f"  [DRY-RUN] {descricao}")
        return
    cur.execute(sql)
    log(f"  [OK] {descricao}")


def main():
    parser = argparse.ArgumentParser(
        description="Migração 001 — cliente_origem e views por fornecedor"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra o que seria feito sem aplicar nada")
    args = parser.parse_args()

    log(SEP)
    log("MIGRAÇÃO 001 — cliente_origem + correção unique key + views por fornecedor")
    if args.dry_run:
        log("  [DRY-RUN] nenhuma alteração será aplicada")
    log(SEP)

    db_cfg = db_destino(autocommit=False)
    conn = pymysql.connect(**db_cfg)
    cur = conn.cursor()

    try:
        # ------------------------------------------------------------------
        # PASSO 1 — Verifica e corrige UNIQUE KEY de cliente_uc
        # ------------------------------------------------------------------
        log("\n[PASSO 1] Verificando UNIQUE KEY de cliente_uc...")
        cur.execute(SQL_CHECK_UNIQUE_KEY)
        dist_col_exists = cur.fetchone()[0] > 0

        if dist_col_exists:
            log("  [SKIP] Unique key já inclui distribuidora_id — nenhuma ação necessária.")
        else:
            log("  [INFO] Unique key atual é (cliente_id, uc). Será atualizada para incluir distribuidora_id.")
            executar(cur, SQL_DROP_OLD_KEY, args.dry_run,
                     "DROP INDEX ux_cliente_uc em cliente_uc")
            executar(cur, SQL_ADD_NEW_KEY, args.dry_run,
                     "ADD UNIQUE KEY ux_cliente_uc (cliente_id, uc, distribuidora_id)")

        # ------------------------------------------------------------------
        # PASSO 2 — Atualizar proc_macro_api_link_uc
        # ------------------------------------------------------------------
        log("\n[PASSO 2] Atualizando proc_macro_api_link_uc...")
        executar(cur, SQL_DROP_PROC, args.dry_run,
                 "DROP PROCEDURE IF EXISTS proc_macro_api_link_uc")
        executar(cur, SQL_CREATE_PROC, args.dry_run,
                 "CREATE PROCEDURE proc_macro_api_link_uc (com distribuidora_id no lookup)")

        # ------------------------------------------------------------------
        # PASSO 3 — Criar tabela cliente_origem
        # ------------------------------------------------------------------
        log("\n[PASSO 3] Criando tabela cliente_origem...")
        executar(cur, SQL_CREATE_CLIENTE_ORIGEM, args.dry_run,
                 "CREATE TABLE IF NOT EXISTS cliente_origem")

        # ------------------------------------------------------------------
        # PASSO 4 — Backfill fornecedor2
        # ------------------------------------------------------------------
        log("\n[PASSO 4] Backfill: registrando clientes existentes como 'fornecedor2'...")
        if not args.dry_run:
            cur.execute(SQL_BACKFILL_FORNECEDOR2)
            log(f"  [OK] {cur.rowcount:,} clientes registrados em cliente_origem (INSERT IGNORE)")
        else:
            cur.execute("SELECT COUNT(*) FROM clientes")
            total = cur.fetchone()[0]
            log(f"  [DRY-RUN] {total:,} clientes seriam inseridos em cliente_origem como 'fornecedor2'")

        # ------------------------------------------------------------------
        # PASSO 5 — Criar views por fornecedor
        # ------------------------------------------------------------------
        log("\n[PASSO 5] Criando views por fornecedor...")

        views = [
            (SQL_VIEW_F2_MACRO_AUTOMACAO,       "view_fornecedor2_macro_automacao"),
            (SQL_VIEW_CONTATUS_MACRO_AUTOMACAO,  "view_contatus_macro_automacao"),
            (SQL_VIEW_F2_MACRO_CONSOLIDADOS,     "view_fornecedor2_macro_consolidados"),
            (SQL_VIEW_CONTATUS_MACRO_CONSOLIDADOS,"view_contatus_macro_consolidados"),
            (SQL_VIEW_F2_MACRO_FULL,             "view_fornecedor2_macro"),
            (SQL_VIEW_CONTATUS_MACRO_FULL,       "view_contatus_macro"),
            (SQL_VIEW_F2_API_AUTOMACAO,          "view_fornecedor2_api_automacao"),
            (SQL_VIEW_CONTATUS_API_AUTOMACAO,    "view_contatus_api_automacao"),
            (SQL_VIEW_F2_API_CONSOLIDADOS,       "view_fornecedor2_api_consolidados"),
            (SQL_VIEW_CONTATUS_API_CONSOLIDADOS, "view_contatus_api_consolidados"),
            (SQL_VIEW_F2_API_FULL,               "view_fornecedor2_api"),
            (SQL_VIEW_CONTATUS_API_FULL,         "view_contatus_api"),
        ]

        for sql, nome in views:
            executar(cur, sql, args.dry_run, f"CREATE OR REPLACE VIEW {nome}")

        # ------------------------------------------------------------------
        # Commit
        # ------------------------------------------------------------------
        if not args.dry_run:
            conn.commit()
            log(f"\n{SEP}")
            log("MIGRAÇÃO APLICADA COM SUCESSO")
            log(SEP)
            log("\nPRÓXIMOS PASSOS (alterações de código — ver README.md):")
            log("  1. Atualizar uc_map em etl/load/macro/02_processar_staging.py")
            log("  2. Adicionar INSERT em cliente_origem em 02_processar_staging.py")
            log("  3. Adicionar queries por fornecedor em dashboard_macros/data/loader.py")
        else:
            log(f"\n{SEP}")
            log("DRY-RUN CONCLUÍDO — nenhuma alteração foi aplicada")
            log(SEP)

    except Exception as e:
        conn.rollback()
        log(f"\n[ERRO] {e}")
        log("Rollback executado. Nenhuma alteração foi gravada.")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
