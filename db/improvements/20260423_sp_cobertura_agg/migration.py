"""
Migration: Criar SP sp_refresh_dashboard_cobertura_agg
Data: 2026-04-23

Cria stored procedure para atualizar dashboard_cobertura_agg de hora em hora,
alinhando com as SPs existentes para macros_agg e arquivos_agg.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

SP_DDL = """
CREATE PROCEDURE sp_refresh_dashboard_cobertura_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
    CREATE TEMPORARY TABLE tmp_cob_combo_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        normalized_uc    CHAR(10)     NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        INDEX (normalized_cpf, normalized_uc)
    )
    SELECT normalized_cpf, normalized_uc, MIN(staging_id) AS first_staging_id
    FROM staging_import_rows
    WHERE validation_status = 'valid'
      AND normalized_uc IS NOT NULL
      AND normalized_uc != ''
    GROUP BY normalized_cpf, normalized_uc;

    DELETE FROM dashboard_cobertura_agg;

    INSERT INTO dashboard_cobertura_agg
        (arquivo, data_carga, total_combos, combos_novas, combos_existentes)
    SELECT
        si.filename AS arquivo,
        DATE(MIN(si.created_at)) AS data_carga,
        COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', sir.normalized_uc)) AS total_combos,
        COUNT(DISTINCT CASE WHEN cf.first_staging_id = si.id
            THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc) END) AS combos_novas,
        COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', sir.normalized_uc))
          - COUNT(DISTINCT CASE WHEN cf.first_staging_id = si.id
              THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc) END) AS combos_existentes
    FROM staging_imports si
    JOIN staging_import_rows sir
        ON sir.staging_id = si.id
        AND sir.validation_status = 'valid'
        AND sir.normalized_uc IS NOT NULL
        AND sir.normalized_uc != ''
    LEFT JOIN tmp_cob_combo_first cf
        ON cf.normalized_cpf = sir.normalized_cpf
        AND cf.normalized_uc = sir.normalized_uc
    GROUP BY si.filename
    ORDER BY MIN(si.id) DESC;

    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
END
"""


def main():
    conn = pymysql.connect(**config.db_destino())
    cur = conn.cursor()

    print("=== Migration: sp_refresh_dashboard_cobertura_agg ===\n")

    # 1. Criar SP
    print("[1/2] Criando SP sp_refresh_dashboard_cobertura_agg...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_cobertura_agg")
    conn.commit()
    cur.execute(SP_DDL)
    conn.commit()
    print("  OK — SP criada.")

    # 2. Executar para popular a tabela
    print("[2/2] Executando SP para popular dashboard_cobertura_agg...")
    cur.execute("CALL sp_refresh_dashboard_cobertura_agg()")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM dashboard_cobertura_agg")
    cnt = cur.fetchone()[0]
    print(f"  OK — {cnt} registros inseridos.")

    cur.close()
    conn.close()
    print("\n=== Migration concluída ===")


if __name__ == "__main__":
    main()
