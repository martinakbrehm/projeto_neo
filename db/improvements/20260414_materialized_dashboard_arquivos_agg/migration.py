"""
Migration: tabela materializada dashboard_arquivos_agg
Substitui a query _SQL_STATS_ARQUIVO (ROW_NUMBER + staging_import_rows JOIN) por
uma tabela física pré-calculada, eliminando o travamento no carregamento do dashboard.

Procedure: sp_refresh_dashboard_arquivos_agg
  - TRUNCATE + INSERT ... SELECT  (a query pesada roda só no refresh, não no dashboard)
  - Chamada ao final do ETL de staging/macro

Uso:
    python migration.py           # executa migration
    python migration.py --dry-run # apenas exibe SQL sem executar
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from config import db_destino
import pymysql

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS dashboard_arquivos_agg (
    id                INT UNSIGNED NOT NULL AUTO_INCREMENT,
    arquivo           VARCHAR(255) NOT NULL,
    data_carga        DATE         NOT NULL,
    cpfs_no_arquivo   INT UNSIGNED NOT NULL DEFAULT 0,
    cpfs_processados  INT UNSIGNED NOT NULL DEFAULT 0,
    ativos            INT UNSIGNED NOT NULL DEFAULT 0,
    inativos          INT UNSIGNED NOT NULL DEFAULT 0,
    atualizado_em     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_daa_data_carga (data_carga),
    INDEX idx_daa_arquivo    (arquivo(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# Procedure que re-popula a tabela via tabela temporária indexada.
# Evita o O(n²) da CTE sem índice (ROW_NUMBER + JOIN sem index).
# Estratégia:
#   1. tmp_latest_ids : MAX(id) por UC/cli → uma passagem em tabela_macros
#   2. tmp_latest_macros : JOIN por PRIMARY KEY + índices na temp table
#   3. INSERT final usa a temp indexada → lookup O(n)
PROCEDURE_SQL = """
CREATE PROCEDURE sp_refresh_dashboard_arquivos_agg()
BEGIN
    -- 1. IDs dos últimos registros por combinação UC (deduplicação)
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_ids;
    CREATE TEMPORARY TABLE tmp_latest_ids (
        max_id INT UNSIGNED NOT NULL,
        INDEX (max_id)
    )
    SELECT MAX(id) AS max_id
    FROM tabela_macros
    WHERE status != 'pendente'
      AND resposta_id IS NOT NULL
    GROUP BY
        CASE
            WHEN cliente_uc_id IS NOT NULL THEN CONCAT('u', cliente_uc_id)
            ELSE CONCAT('c', cliente_id, '_', distribuidora_id)
        END;

    -- 2. Dados completos dos últimos registros (join por PK — rápido)
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_macros;
    CREATE TEMPORARY TABLE tmp_latest_macros (
        id               INT UNSIGNED NOT NULL,
        cliente_uc_id    INT UNSIGNED NULL,
        cliente_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        status           VARCHAR(30)  NOT NULL,
        INDEX (cliente_uc_id),
        INDEX (cliente_id, distribuidora_id)
    )
    SELECT tm.id, tm.cliente_uc_id, tm.cliente_id, tm.distribuidora_id, tm.status
    FROM tabela_macros tm
    INNER JOIN tmp_latest_ids t ON t.max_id = tm.id;

    -- 3. Popula tabela materializada usando temp indexada
    TRUNCATE TABLE dashboard_arquivos_agg;

    INSERT INTO dashboard_arquivos_agg
        (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos)
    SELECT
        si.filename                                                                          AS arquivo,
        DATE(si.created_at)                                                                  AS data_carga,
        COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, '')))    AS cpfs_no_arquivo,
        COUNT(DISTINCT CASE
            WHEN m.id IS NOT NULL
            THEN CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))
        END)                                                                                 AS cpfs_processados,
        COUNT(DISTINCT CASE
            WHEN m.status = 'consolidado'
            THEN CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))
        END)                                                                                 AS ativos,
        COUNT(DISTINCT CASE
            WHEN m.status IN ('excluido', 'reprocessar')
            THEN CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))
        END)                                                                                 AS inativos
    FROM staging_imports si
    JOIN staging_import_rows sir
        ON  sir.staging_id        = si.id
        AND sir.validation_status = 'valid'
    LEFT JOIN clientes cl
        ON cl.cpf = sir.normalized_cpf
    LEFT JOIN cliente_uc cu
        ON  cu.cliente_id       = cl.id
        AND cu.uc               = sir.normalized_uc
        AND cu.distribuidora_id = CAST(si.distribuidora_nome AS UNSIGNED)
    LEFT JOIN tmp_latest_macros m
        ON (
              (cu.id IS NOT NULL AND m.cliente_uc_id    = cu.id)
           OR (cu.id IS NULL     AND m.cliente_id       = cl.id
                                 AND m.distribuidora_id = CAST(si.distribuidora_nome AS UNSIGNED))
           )
    GROUP BY si.id, si.filename, DATE(si.created_at)
    ORDER BY si.id DESC;

    DROP TEMPORARY TABLE IF EXISTS tmp_latest_ids;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_macros;
END
"""


def run(dry_run: bool = False):
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    steps = [
        ("Criando tabela dashboard_arquivos_agg...", DDL_TABLE),
        ("Dropando procedure antiga (se existir)...",
         "DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg"),
        ("Criando stored procedure sp_refresh_dashboard_arquivos_agg...", PROCEDURE_SQL),
    ]

    for msg, sql in steps:
        print(msg)
        if not dry_run:
            cur.execute(sql)
            conn.commit()

    print("Tabela e procedure criadas com sucesso.")

    if not dry_run:
        print("\nPopulando dados iniciais (pode demorar alguns minutos)...")
        cur.execute("CALL sp_refresh_dashboard_arquivos_agg()")
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM dashboard_arquivos_agg")
        count = cur.fetchone()[0]
        print(f"Linhas na tabela materializada: {count}")

        if count > 0:
            cur.execute("""
                SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos
                FROM dashboard_arquivos_agg
                ORDER BY data_carga DESC
                LIMIT 5
            """)
            print("\nÚltimos 5 arquivos:")
            for r in cur.fetchall():
                print(f"  {r[0]:<50} {r[1]}  cpfs={r[2]}  proc={r[3]}  ativos={r[4]}  inativos={r[5]}")
    else:
        print("\n[DRY-RUN] Nenhuma alteração aplicada.")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
