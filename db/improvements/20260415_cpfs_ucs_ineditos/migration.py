"""
Migration: 20260415_cpfs_ucs_ineditos
Adiciona colunas de CPFs/UCs inéditos em dashboard_arquivos_agg.

Colunas novas:
  - cpfs_ineditos:          CPFs que apareceram pela PRIMEIRA vez neste arquivo
  - ucs_ineditas:           combinações CPF+UC inéditas neste arquivo
  - ineditos_processados:   inéditos que já foram processados pela macro
  - ineditos_ativos:        inéditos com status consolidado
  - ineditos_inativos:      inéditos com status excluido/reprocessar

Atualiza a stored procedure sp_refresh_dashboard_arquivos_agg para calcular
esses valores usando MIN(staging_id) por CPF/UC — eficiente em single-pass.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import db_destino
import pymysql


SQL_ALTER = """
ALTER TABLE dashboard_arquivos_agg
    ADD COLUMN cpfs_ineditos          INT UNSIGNED NOT NULL DEFAULT 0 AFTER inativos,
    ADD COLUMN ucs_ineditas           INT UNSIGNED NOT NULL DEFAULT 0 AFTER cpfs_ineditos,
    ADD COLUMN ineditos_processados   INT UNSIGNED NOT NULL DEFAULT 0 AFTER ucs_ineditas,
    ADD COLUMN ineditos_ativos        INT UNSIGNED NOT NULL DEFAULT 0 AFTER ineditos_processados,
    ADD COLUMN ineditos_inativos      INT UNSIGNED NOT NULL DEFAULT 0 AFTER ineditos_ativos
"""

SQL_DROP_PROCEDURE = "DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg"

SQL_CREATE_PROCEDURE = """
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

    -- 3. Primeiro staging_id por CPF (para calcular CPFs inéditos)
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first;
    CREATE TEMPORARY TABLE tmp_cpf_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        INDEX (first_staging_id),
        INDEX (normalized_cpf)
    )
    SELECT normalized_cpf, MIN(staging_id) AS first_staging_id
    FROM staging_import_rows
    WHERE validation_status = 'valid'
    GROUP BY normalized_cpf;

    -- 4. Primeiro staging_id por CPF+UC (para calcular UCs inéditas)
    DROP TEMPORARY TABLE IF EXISTS tmp_uc_first;
    CREATE TEMPORARY TABLE tmp_uc_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        normalized_uc    CHAR(10)     NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        INDEX (first_staging_id)
    )
    SELECT normalized_cpf, normalized_uc, MIN(staging_id) AS first_staging_id
    FROM staging_import_rows
    WHERE validation_status = 'valid'
      AND normalized_uc IS NOT NULL
      AND normalized_uc != ''
    GROUP BY normalized_cpf, normalized_uc;

    -- 5. Popula tabela materializada
    TRUNCATE TABLE dashboard_arquivos_agg;

    INSERT INTO dashboard_arquivos_agg
        (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
         cpfs_ineditos, ucs_ineditas, ineditos_processados, ineditos_ativos, ineditos_inativos)
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
        END)                                                                                 AS inativos,
        COUNT(DISTINCT CASE
            WHEN cf.first_staging_id = si.id
            THEN sir.normalized_cpf
        END)                                                                                 AS cpfs_ineditos,
        COUNT(DISTINCT CASE
            WHEN uf.first_staging_id = si.id
            THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc)
        END)                                                                                 AS ucs_ineditas,
        COUNT(DISTINCT CASE
            WHEN cf.first_staging_id = si.id AND m.id IS NOT NULL
            THEN sir.normalized_cpf
        END)                                                                                 AS ineditos_processados,
        COUNT(DISTINCT CASE
            WHEN cf.first_staging_id = si.id AND m.status = 'consolidado'
            THEN sir.normalized_cpf
        END)                                                                                 AS ineditos_ativos,
        COUNT(DISTINCT CASE
            WHEN cf.first_staging_id = si.id AND m.status IN ('excluido', 'reprocessar')
            THEN sir.normalized_cpf
        END)                                                                                 AS ineditos_inativos
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
    LEFT JOIN tmp_cpf_first cf
        ON cf.normalized_cpf = sir.normalized_cpf
    LEFT JOIN tmp_uc_first uf
        ON  uf.normalized_cpf = sir.normalized_cpf
        AND uf.normalized_uc  = sir.normalized_uc
    GROUP BY si.id, si.filename, DATE(si.created_at)
    ORDER BY si.id DESC;

    DROP TEMPORARY TABLE IF EXISTS tmp_latest_ids;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_macros;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first;
    DROP TEMPORARY TABLE IF EXISTS tmp_uc_first;
END
"""


def run():
    conn = pymysql.connect(**db_destino())
    try:
        with conn.cursor() as cur:
            # 1. Adicionar colunas
            print("ALTER TABLE dashboard_arquivos_agg: +cpfs_ineditos, +ucs_ineditas...")
            try:
                cur.execute(SQL_ALTER)
                conn.commit()
            except Exception as e:
                if "Duplicate column" in str(e):
                    print("  Colunas já existem, pulando ALTER.")
                else:
                    raise

            # 2. Recriar stored procedure
            print("Recriando sp_refresh_dashboard_arquivos_agg...")
            cur.execute(SQL_DROP_PROCEDURE)
            cur.execute(SQL_CREATE_PROCEDURE)
            conn.commit()

            # 3. Popular dados
            print("Executando refresh (CALL sp_refresh_dashboard_arquivos_agg)...")
            cur.execute("CALL sp_refresh_dashboard_arquivos_agg()")
            conn.commit()

            # 4. Verificar
            cur.execute("""
                SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_processados,
                       ativos, inativos, cpfs_ineditos, ucs_ineditas,
                       ineditos_processados, ineditos_ativos, ineditos_inativos
                FROM dashboard_arquivos_agg
                ORDER BY data_carga DESC
            """)
            print("\nResultado:")
            for r in cur.fetchall():
                print(f"  {r[0]:<50} cpfs={r[2]:>6,}  proc={r[3]:>6,}  "
                      f"ativos={r[4]:>5,}  inativos={r[5]:>6,}  "
                      f"cpfs_ined={r[6]:>6,}  ucs_ined={r[7]:>6,}  "
                      f"ined_proc={r[8]:>6,}  ined_ativ={r[9]:>5,}  ined_inat={r[10]:>5,}")

    finally:
        conn.close()


if __name__ == "__main__":
    run()
