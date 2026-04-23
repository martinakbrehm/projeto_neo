"""
Migration: Inéditos por distribuidora — combo = (CPF + UC + distribuidora)
Data: 2026-04-23

Definição de negócio:
  Inédito = primeira vez que a combinação (CPF, UC, distribuidora) aparece
  no staging. Se o mesmo CPF vier com UC diferente, conta como novo.

Colunas resultantes:
  cpfs_ineditos = CPFs distintos que trazem pelo menos 1 combo novo no arquivo
  ucs_ineditas  = quantidade de combos (CPF+UC+dist) novos no arquivo
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

SP_ARQUIVOS = """
CREATE PROCEDURE sp_refresh_dashboard_arquivos_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- =========================================================
    -- 0. Ordem cronológica dos arquivos
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_stg_order;
    CREATE TEMPORARY TABLE tmp_stg_order (
        staging_id INT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT id AS staging_id, created_at FROM staging_imports;

    -- =========================================================
    -- 0b. Expandir distribuidoras por arquivo
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_staging_dist;
    CREATE TEMPORARY TABLE tmp_staging_dist (
        staging_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (staging_id, distribuidora_id)
    ) ENGINE=InnoDB;

    -- Arquivos com dist numérica ("1,2,3" ou "3")
    INSERT INTO tmp_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome REGEXP '^[0-9]'
      AND FIND_IN_SET(d.id, REPLACE(si.distribuidora_nome, ' ', '')) > 0;

    -- Arquivos "multi" ou texto → todas as distribuidoras
    INSERT IGNORE INTO tmp_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome NOT REGEXP '^[0-9]';

    -- =========================================================
    -- 1. Total de combos (CPF+UC) por arquivo (contagem bruta)
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_cpfs;
    CREATE TEMPORARY TABLE tmp_arq_cpfs (
        staging_id       INT UNSIGNED NOT NULL,
        cpfs_no_arquivo  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT staging_id,
           COUNT(DISTINCT CONCAT(normalized_cpf, '|', COALESCE(normalized_uc, ''))) AS cpfs_no_arquivo
    FROM staging_import_rows
    WHERE validation_status = 'valid'
    GROUP BY staging_id;

    -- =========================================================
    -- 2. PRIMEIRO STAGING para cada combo (CPF, UC, dist) — CRONOLÓGICO
    --    UC coalescido para '' quando NULL/vazio
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_combo_first;
    CREATE TEMPORARY TABLE tmp_combo_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (normalized_cpf, uc_key, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_combo_first (normalized_cpf, uc_key, distribuidora_id, first_staging_id)
    SELECT sub.normalized_cpf, sub.uc_key, sub.distribuidora_id, sub.staging_id
    FROM (
        SELECT sir.normalized_cpf,
               COALESCE(NULLIF(sir.normalized_uc, ''), '') AS uc_key,
               sd.distribuidora_id,
               sir.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY sir.normalized_cpf,
                                COALESCE(NULLIF(sir.normalized_uc, ''), ''),
                                sd.distribuidora_id
                   ORDER BY so.created_at, sir.staging_id
               ) AS rn
        FROM staging_import_rows sir
        INNER JOIN tmp_staging_dist sd ON sd.staging_id = sir.staging_id
        INNER JOIN tmp_stg_order so   ON so.staging_id = sir.staging_id
        WHERE sir.validation_status = 'valid'
    ) sub
    WHERE sub.rn = 1;

    -- =========================================================
    -- 3. Contar inéditos por arquivo
    --    cpfs_ineditos = CPFs distintos com pelo menos 1 combo novo
    --    ucs_ineditas  = total de combos novos
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_ineditos;
    CREATE TEMPORARY TABLE tmp_ineditos (
        staging_id    INT UNSIGNED NOT NULL,
        cpfs_ineditos INT UNSIGNED NOT NULL DEFAULT 0,
        ucs_ineditas  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT first_staging_id AS staging_id,
           COUNT(DISTINCT normalized_cpf) AS cpfs_ineditos,
           COUNT(*)                       AS ucs_ineditas
    FROM tmp_combo_first
    GROUP BY first_staging_id;

    -- =========================================================
    -- 4. Lookup CPF → status (último resultado por CPF+dist)
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_status;
    CREATE TEMPORARY TABLE tmp_cpf_status (
        cpf              CHAR(11) NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        status           VARCHAR(30) NOT NULL,
        PRIMARY KEY (cpf, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cpf_status (cpf, distribuidora_id, status)
    SELECT cl.cpf, g.distribuidora_id, tm.status
    FROM (
        SELECT cliente_id, distribuidora_id, MAX(id) AS max_id
        FROM tabela_macros
        WHERE status != 'pendente' AND resposta_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id
    ) g
    INNER JOIN tabela_macros tm ON tm.id = g.max_id
    INNER JOIN clientes cl ON cl.id = g.cliente_id
    ON DUPLICATE KEY UPDATE status = VALUES(status);

    -- =========================================================
    -- 5. Status e flag inédito por CPF×arquivo
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_file_status;
    CREATE TEMPORARY TABLE tmp_cpf_file_status (
        staging_id    INT UNSIGNED NOT NULL,
        cpf           CHAR(11) NOT NULL,
        is_processado TINYINT(1) NOT NULL DEFAULT 1,
        is_ativo      TINYINT(1) NOT NULL DEFAULT 0,
        is_inativo    TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id, cpf)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cpf_file_status (staging_id, cpf, is_processado, is_ativo, is_inativo)
    SELECT
        sir.staging_id,
        sir.normalized_cpf,
        1,
        MAX(CASE WHEN cs.status = 'consolidado' THEN 1 ELSE 0 END),
        CASE WHEN MAX(CASE WHEN cs.status = 'consolidado' THEN 1 ELSE 0 END) = 0
             THEN 1 ELSE 0 END
    FROM staging_import_rows sir
    INNER JOIN tmp_staging_dist sd ON sd.staging_id = sir.staging_id
    INNER JOIN tmp_cpf_status cs
        ON cs.cpf = sir.normalized_cpf
        AND cs.distribuidora_id = sd.distribuidora_id
    WHERE sir.validation_status = 'valid'
    GROUP BY sir.staging_id, sir.normalized_cpf;

    -- Flag: CPF é inédito no arquivo se QUALQUER combo (CPF, UC, dist) é novo
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_is_ined;
    CREATE TEMPORARY TABLE tmp_cpf_is_ined (
        staging_id     INT UNSIGNED NOT NULL,
        normalized_cpf CHAR(11) NOT NULL,
        is_inedito     TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id, normalized_cpf)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cpf_is_ined (staging_id, normalized_cpf, is_inedito)
    SELECT sir.staging_id, sir.normalized_cpf,
           MAX(CASE WHEN cf.first_staging_id = sir.staging_id THEN 1 ELSE 0 END) AS is_inedito
    FROM staging_import_rows sir
    INNER JOIN tmp_staging_dist sd ON sd.staging_id = sir.staging_id
    LEFT JOIN tmp_combo_first cf
        ON cf.normalized_cpf = sir.normalized_cpf
        AND cf.uc_key = COALESCE(NULLIF(sir.normalized_uc, ''), '')
        AND cf.distribuidora_id = sd.distribuidora_id
        AND cf.first_staging_id = sir.staging_id
    WHERE sir.validation_status = 'valid'
    GROUP BY sir.staging_id, sir.normalized_cpf;

    -- =========================================================
    -- 6. Agregar por arquivo
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_status;
    CREATE TEMPORARY TABLE tmp_arq_status (
        staging_id         INT UNSIGNED NOT NULL,
        cpfs_processados   INT UNSIGNED NOT NULL DEFAULT 0,
        ativos             INT UNSIGNED NOT NULL DEFAULT 0,
        inativos           INT UNSIGNED NOT NULL DEFAULT 0,
        ineditos_proc      INT UNSIGNED NOT NULL DEFAULT 0,
        ineditos_ativos    INT UNSIGNED NOT NULL DEFAULT 0,
        ineditos_inativos  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_arq_status
        (staging_id, cpfs_processados, ativos, inativos,
         ineditos_proc, ineditos_ativos, ineditos_inativos)
    SELECT
        sir.staging_id,
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1      THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1     THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 AND ci.is_inedito = 1
              THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1 AND ci.is_inedito = 1
              THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1 AND ci.is_inedito = 1
              THEN sir.normalized_cpf END)
    FROM staging_import_rows sir
    LEFT JOIN tmp_cpf_file_status cfs
        ON cfs.staging_id = sir.staging_id AND cfs.cpf = sir.normalized_cpf
    LEFT JOIN tmp_cpf_is_ined ci
        ON ci.staging_id = sir.staging_id AND ci.normalized_cpf = sir.normalized_cpf
    WHERE sir.validation_status = 'valid'
    GROUP BY sir.staging_id;

    -- =========================================================
    -- 7. INSERT final
    -- =========================================================
    TRUNCATE TABLE dashboard_arquivos_agg;

    INSERT INTO dashboard_arquivos_agg
        (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
         cpfs_ineditos, ucs_ineditas, ineditos_processados, ineditos_ativos, ineditos_inativos)
    SELECT
        si.filename,
        DATE(si.created_at),
        COALESCE(ac.cpfs_no_arquivo, 0),
        COALESCE(ast.cpfs_processados, 0),
        COALESCE(ast.ativos, 0),
        COALESCE(ast.inativos, 0),
        COALESCE(ined.cpfs_ineditos, 0),
        COALESCE(ined.ucs_ineditas, 0),
        COALESCE(ast.ineditos_proc, 0),
        COALESCE(ast.ineditos_ativos, 0),
        COALESCE(ast.ineditos_inativos, 0)
    FROM staging_imports si
    LEFT JOIN tmp_arq_cpfs ac ON ac.staging_id = si.id
    LEFT JOIN tmp_arq_status ast ON ast.staging_id = si.id
    LEFT JOIN tmp_ineditos ined ON ined.staging_id = si.id
    ORDER BY si.created_at, si.id;

    -- Cleanup
    DROP TEMPORARY TABLE IF EXISTS tmp_stg_order;
    DROP TEMPORARY TABLE IF EXISTS tmp_staging_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_cpfs;
    DROP TEMPORARY TABLE IF EXISTS tmp_combo_first;
    DROP TEMPORARY TABLE IF EXISTS tmp_ineditos;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_file_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_is_ined;
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_status;
END
"""

SP_COBERTURA = """
CREATE PROCEDURE sp_refresh_dashboard_cobertura_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- Ordem cronológica
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_stg_order;
    CREATE TEMPORARY TABLE tmp_cob_stg_order (
        staging_id INT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT id AS staging_id, created_at FROM staging_imports;

    -- Distribuidoras por arquivo
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_staging_dist;
    CREATE TEMPORARY TABLE tmp_cob_staging_dist (
        staging_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (staging_id, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome REGEXP '^[0-9]'
      AND FIND_IN_SET(d.id, REPLACE(si.distribuidora_nome, ' ', '')) > 0;

    INSERT IGNORE INTO tmp_cob_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome NOT REGEXP '^[0-9]';

    -- Primeiro staging por (CPF, UC, distribuidora), CRONOLOGICAMENTE
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
    CREATE TEMPORARY TABLE tmp_cob_combo_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        normalized_uc    CHAR(10)     NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        INDEX (normalized_cpf, normalized_uc, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_combo_first (normalized_cpf, normalized_uc, distribuidora_id, first_staging_id)
    SELECT sub.normalized_cpf, sub.normalized_uc, sub.distribuidora_id, sub.staging_id
    FROM (
        SELECT sir.normalized_cpf, sir.normalized_uc, sd.distribuidora_id, sir.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY sir.normalized_cpf, sir.normalized_uc, sd.distribuidora_id
                   ORDER BY so.created_at, sir.staging_id
               ) AS rn
        FROM staging_import_rows sir
        INNER JOIN tmp_cob_staging_dist sd ON sd.staging_id = sir.staging_id
        INNER JOIN tmp_cob_stg_order so ON so.staging_id = sir.staging_id
        WHERE sir.validation_status = 'valid'
          AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
    ) sub
    WHERE sub.rn = 1;

    DELETE FROM dashboard_cobertura_agg;

    INSERT INTO dashboard_cobertura_agg
        (arquivo, data_carga, total_combos, combos_novas, combos_existentes)
    SELECT
        si.filename AS arquivo,
        DATE(MIN(si.created_at)) AS data_carga,
        COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', sir.normalized_uc, '|', sd.distribuidora_id)) AS total_combos,
        COUNT(DISTINCT CASE WHEN cf.first_staging_id = si.id
            THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc, '|', sd.distribuidora_id) END) AS combos_novas,
        COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', sir.normalized_uc, '|', sd.distribuidora_id))
          - COUNT(DISTINCT CASE WHEN cf.first_staging_id = si.id
              THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc, '|', sd.distribuidora_id) END) AS combos_existentes
    FROM staging_imports si
    JOIN staging_import_rows sir
        ON sir.staging_id = si.id
        AND sir.validation_status = 'valid'
        AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
    JOIN tmp_cob_staging_dist sd ON sd.staging_id = si.id
    LEFT JOIN tmp_cob_combo_first cf
        ON cf.normalized_cpf = sir.normalized_cpf
        AND cf.normalized_uc = sir.normalized_uc
        AND cf.distribuidora_id = sd.distribuidora_id
    GROUP BY si.filename
    ORDER BY MIN(si.created_at), MIN(si.id);

    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_staging_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_stg_order;
END
"""


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print("=" * 60)
    print("Fix: inéditos por distribuidora (CPF+dist, CPF+UC+dist)")
    print("=" * 60)

    for name, ddl in [("sp_refresh_dashboard_arquivos_agg", SP_ARQUIVOS),
                       ("sp_refresh_dashboard_cobertura_agg", SP_COBERTURA)]:
        print(f"\nRecriando {name}...")
        cur.execute(f"DROP PROCEDURE IF EXISTS {name}")
        conn.commit()
        cur.execute(ddl)
        conn.commit()
        print("  OK")

    for sp in ["sp_refresh_dashboard_arquivos_agg", "sp_refresh_dashboard_cobertura_agg"]:
        print(f"\n[CALL] {sp}...")
        t0 = time.time()
        try:
            cur.execute(f"CALL {sp}()")
            conn.commit()
            print(f"  OK ({time.time()-t0:.1f}s)")
        except Exception as e:
            print(f"  ERRO ({time.time()-t0:.1f}s): {e}")
            conn.close()
            return

    print("\n" + "=" * 60)
    print("RESULTADO: dashboard_arquivos_agg")
    print("=" * 60)
    cur.execute("""
        SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_ineditos, ucs_ineditas,
               cpfs_processados, ativos, inativos
        FROM dashboard_arquivos_agg ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        pct = round(r[3]/r[2]*100,1) if r[2]>0 else 0
        print(f"  {r[0]:55s} | carga={r[1]} | total={r[2]:>6} | ined={r[3]:>6} ({pct:>5.1f}%) | uc_ined={r[4]:>6} | proc={r[5]:>6}")

    print("\n" + "=" * 60)
    print("RESULTADO: dashboard_cobertura_agg")
    print("=" * 60)
    cur.execute("""
        SELECT arquivo, data_carga, total_combos, combos_novas, combos_existentes
        FROM dashboard_cobertura_agg ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        pct = round(r[3]/r[2]*100,1) if r[2]>0 else 0
        print(f"  {r[0]:55s} | carga={r[1]} | combos={r[2]:>6} | novas={r[3]:>6} ({pct:>5.1f}%) | exist={r[4]:>6}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
