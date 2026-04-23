"""
Migration: Fix ordenação cronológica de inéditos nas SPs arquivos e cobertura
Data: 2026-04-23

Problema: As SPs usavam MIN(staging_id) para determinar o primeiro aparecimento
de cada CPF/UC. Mas a ordem dos staging_ids NÃO corresponde à ordem cronológica:
  - historico (id=12, created=2026-03-23) — dados mais antigos, id mais alto
  - 300k.csv  (id=8,  created=2026-03-23) — idem
  - 35K_CELP  (id=1,  created=2026-04-06) — dados mais recentes, id mais baixo

Resultado errado:
  - historico: 0 inéditos (deviam ser ~115k, é o arquivo mais antigo)
  - 300k: 254k inéditos (perde 46k atribuídos a arquivos com id menor)
  - CELP 16-04: 4k inéditos (deviam ser ~30k+, pois dados novos)

Solução: Substituir MIN(staging_id) por subquery que seleciona o staging_id
com o menor (created_at, staging_id) — i.e., ORDER BY si.created_at, sir.staging_id.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

# ───────────────────────────────────────────────────────────────────────────
# SP ARQUIVOS — fix nos steps 2 (tmp_cpf_first) e 3 (tmp_uc_first)
# ───────────────────────────────────────────────────────────────────────────
SP_ARQUIVOS = """
CREATE PROCEDURE sp_refresh_dashboard_arquivos_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- 0. Lookup de ordem cronológica: staging_id → rank
    --    Rank = posição cronológica (created_at, id) do arquivo
    DROP TEMPORARY TABLE IF EXISTS tmp_stg_order;
    CREATE TEMPORARY TABLE tmp_stg_order (
        staging_id INT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT id AS staging_id, created_at
    FROM staging_imports;

    -- 1. CPFs+UCs por arquivo
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

    -- 2. Primeiro staging por CPF, ORDENADO CRONOLOGICAMENTE
    --    Usa (si.created_at, sir.staging_id) em vez de MIN(staging_id)
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first;
    CREATE TEMPORARY TABLE tmp_cpf_first (
        normalized_cpf   CHAR(11) NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (normalized_cpf)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cpf_first (normalized_cpf, first_staging_id)
    SELECT sub.normalized_cpf, sub.staging_id
    FROM (
        SELECT sir.normalized_cpf, sir.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY sir.normalized_cpf
                   ORDER BY so.created_at, sir.staging_id
               ) AS rn
        FROM staging_import_rows sir
        INNER JOIN tmp_stg_order so ON so.staging_id = sir.staging_id
        WHERE sir.validation_status = 'valid'
    ) sub
    WHERE sub.rn = 1;

    -- 3. Primeiro staging por CPF+UC, ORDENADO CRONOLOGICAMENTE
    DROP TEMPORARY TABLE IF EXISTS tmp_uc_first;
    CREATE TEMPORARY TABLE tmp_uc_first (
        normalized_cpf   CHAR(11) NOT NULL,
        normalized_uc    CHAR(10) NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (normalized_cpf, normalized_uc)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_uc_first (normalized_cpf, normalized_uc, first_staging_id)
    SELECT sub.normalized_cpf, sub.normalized_uc, sub.staging_id
    FROM (
        SELECT sir.normalized_cpf, sir.normalized_uc, sir.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY sir.normalized_cpf, sir.normalized_uc
                   ORDER BY so.created_at, sir.staging_id
               ) AS rn
        FROM staging_import_rows sir
        INNER JOIN tmp_stg_order so ON so.staging_id = sir.staging_id
        WHERE sir.validation_status = 'valid'
          AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
    ) sub
    WHERE sub.rn = 1;

    -- 4. Contagens de ineditos por staging_id
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_ined_counts;
    CREATE TEMPORARY TABLE tmp_cpf_ined_counts (
        staging_id    INT UNSIGNED NOT NULL,
        cpfs_ineditos INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT first_staging_id AS staging_id, COUNT(DISTINCT normalized_cpf) AS cpfs_ineditos
    FROM tmp_cpf_first
    GROUP BY first_staging_id;

    DROP TEMPORARY TABLE IF EXISTS tmp_uc_ined_counts;
    CREATE TEMPORARY TABLE tmp_uc_ined_counts (
        staging_id   INT UNSIGNED NOT NULL,
        ucs_ineditas INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT first_staging_id AS staging_id, COUNT(*) AS ucs_ineditas
    FROM tmp_uc_first
    GROUP BY first_staging_id;

    DROP TEMPORARY TABLE IF EXISTS tmp_ineditos;
    CREATE TEMPORARY TABLE tmp_ineditos (
        staging_id    INT UNSIGNED NOT NULL,
        cpfs_ineditos INT UNSIGNED NOT NULL DEFAULT 0,
        ucs_ineditas  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_ineditos (staging_id, cpfs_ineditos, ucs_ineditas)
    SELECT staging_id, cpfs_ineditos, 0 FROM tmp_cpf_ined_counts;

    UPDATE tmp_ineditos i
    INNER JOIN tmp_uc_ined_counts u ON u.staging_id = i.staging_id
    SET i.ucs_ineditas = u.ucs_ineditas;

    INSERT INTO tmp_ineditos (staging_id, cpfs_ineditos, ucs_ineditas)
    SELECT u.staging_id, 0, u.ucs_ineditas
    FROM tmp_uc_ined_counts u
    LEFT JOIN tmp_cpf_ined_counts c ON c.staging_id = u.staging_id
    WHERE c.staging_id IS NULL;

    -- 5. Lookup CPF -> status
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

    -- 5b. Expandir distribuidoras multi-valor
    DROP TEMPORARY TABLE IF EXISTS tmp_staging_dist;
    CREATE TEMPORARY TABLE tmp_staging_dist (
        staging_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (staging_id, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome REGEXP '^[0-9]'
      AND FIND_IN_SET(d.id, REPLACE(si.distribuidora_nome, ' ', '')) > 0;

    INSERT IGNORE INTO tmp_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome NOT REGEXP '^[0-9]';

    -- 5c. Melhor status por CPF por arquivo
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

    -- 6. Agregar por arquivo
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
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 AND cf.first_staging_id = sir.staging_id
              THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1 AND cf.first_staging_id = sir.staging_id
              THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1 AND cf.first_staging_id = sir.staging_id
              THEN sir.normalized_cpf END)
    FROM staging_import_rows sir
    LEFT JOIN tmp_cpf_file_status cfs
        ON cfs.staging_id = sir.staging_id AND cfs.cpf = sir.normalized_cpf
    LEFT JOIN tmp_cpf_first cf ON cf.normalized_cpf = sir.normalized_cpf
    WHERE sir.validation_status = 'valid'
    GROUP BY sir.staging_id;

    -- 7. INSERT final
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
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_cpfs;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first;
    DROP TEMPORARY TABLE IF EXISTS tmp_uc_first;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_ined_counts;
    DROP TEMPORARY TABLE IF EXISTS tmp_uc_ined_counts;
    DROP TEMPORARY TABLE IF EXISTS tmp_ineditos;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_staging_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_file_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_status;
END
"""

# ───────────────────────────────────────────────────────────────────────────
# SP COBERTURA — fix no tmp_cob_combo_first
# ───────────────────────────────────────────────────────────────────────────
SP_COBERTURA = """
CREATE PROCEDURE sp_refresh_dashboard_cobertura_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- Ordem cronológica dos staging_imports
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_stg_order;
    CREATE TEMPORARY TABLE tmp_cob_stg_order (
        staging_id INT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT id AS staging_id, created_at
    FROM staging_imports;

    -- Primeiro staging por CPF+UC, CRONOLOGICAMENTE
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
    CREATE TEMPORARY TABLE tmp_cob_combo_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        normalized_uc    CHAR(10)     NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        INDEX (normalized_cpf, normalized_uc)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_combo_first (normalized_cpf, normalized_uc, first_staging_id)
    SELECT sub.normalized_cpf, sub.normalized_uc, sub.staging_id
    FROM (
        SELECT sir.normalized_cpf, sir.normalized_uc, sir.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY sir.normalized_cpf, sir.normalized_uc
                   ORDER BY so.created_at, sir.staging_id
               ) AS rn
        FROM staging_import_rows sir
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
        AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
    LEFT JOIN tmp_cob_combo_first cf
        ON cf.normalized_cpf = sir.normalized_cpf
        AND cf.normalized_uc = sir.normalized_uc
    GROUP BY si.filename
    ORDER BY MIN(si.created_at), MIN(si.id);

    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_stg_order;
END
"""


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print("=" * 60)
    print("Fix: ordenação cronológica de inéditos (created_at, id)")
    print("=" * 60)

    # 1. Recriar SP arquivos
    print("\n[1/4] Recriando sp_refresh_dashboard_arquivos_agg...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg")
    conn.commit()
    cur.execute(SP_ARQUIVOS)
    conn.commit()
    print("  OK")

    # 2. Recriar SP cobertura
    print("\n[2/4] Recriando sp_refresh_dashboard_cobertura_agg...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_cobertura_agg")
    conn.commit()
    cur.execute(SP_COBERTURA)
    conn.commit()
    print("  OK")

    # 3. Executar ambas
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

    # 4. Mostrar resultados
    print("\n" + "=" * 60)
    print("RESULTADO: dashboard_arquivos_agg")
    print("=" * 60)
    cur.execute("""
        SELECT arquivo, cpfs_no_arquivo, cpfs_ineditos, ucs_ineditas,
               cpfs_processados, ativos, inativos
        FROM dashboard_arquivos_agg
        ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        pct = round(r[2]/r[1]*100,1) if r[1]>0 else 0
        print(f"  {r[0]:55s} | total={r[1]:>6} | ined={r[2]:>6} ({pct:>5.1f}%) | uc_ined={r[3]:>6} | proc={r[4]:>6} | at={r[5]:>6} | inat={r[6]:>6}")

    print("\n" + "=" * 60)
    print("RESULTADO: dashboard_cobertura_agg")
    print("=" * 60)
    cur.execute("""
        SELECT arquivo, total_combos, combos_novas, combos_existentes
        FROM dashboard_cobertura_agg
        ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        pct = round(r[2]/r[1]*100,1) if r[1]>0 else 0
        print(f"  {r[0]:55s} | combos={r[1]:>6} | novas={r[2]:>6} ({pct:>5.1f}%) | exist={r[3]:>6}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
