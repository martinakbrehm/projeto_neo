"""
Migration: Fix sp_refresh_dashboard_arquivos_agg para distribuidoras multi-valor
Data: 2026-04-23

Problema: O SP usava CAST(si.distribuidora_nome AS UNSIGNED) para filtrar
por distribuidora. Para arquivos com distribuidora_nome='1,2,3' ou 'multi',
o CAST retornava apenas 1 ou 0, perdendo matches de outras distribuidoras.

Resultado:
  - historico (dist='multi'): 0 processados (deviam ser 115,986)
  - 300k.csv (dist='1,2,3'): 97,214 processados (deviam ser 293,465)

Solução: Adicionar tmp_staging_dist que expande distribuidora_nome em linhas
individuais (staging_id, distribuidora_id). Um novo tmp_cpf_file_status
pré-computa o status "melhor" de cada CPF por arquivo (consolidado > demais),
evitando double-counting de ativos/inativos.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

NEW_SP = """
CREATE PROCEDURE sp_refresh_dashboard_arquivos_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- 1. CPFs+UCs por arquivo (usando covering index idx_sir_valid_cpf_uc_stg)
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

    -- 2. Primeiro staging por CPF (para cpfs_ineditos)
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first;
    CREATE TEMPORARY TABLE tmp_cpf_first (
        normalized_cpf   CHAR(11) NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (normalized_cpf)
    ) ENGINE=InnoDB
    SELECT normalized_cpf, MIN(staging_id) AS first_staging_id
    FROM staging_import_rows
    WHERE validation_status = 'valid'
    GROUP BY normalized_cpf;

    -- 3. Primeiro staging por CPF+UC (para ucs_ineditas)
    DROP TEMPORARY TABLE IF EXISTS tmp_uc_first;
    CREATE TEMPORARY TABLE tmp_uc_first (
        normalized_cpf   CHAR(11) NOT NULL,
        normalized_uc    CHAR(10) NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (normalized_cpf, normalized_uc)
    ) ENGINE=InnoDB
    SELECT normalized_cpf, normalized_uc, MIN(staging_id) AS first_staging_id
    FROM staging_import_rows
    WHERE validation_status = 'valid'
      AND normalized_uc IS NOT NULL AND normalized_uc != ''
    GROUP BY normalized_cpf, normalized_uc;

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

    -- 5. Lookup CPF -> status (ultimo resultado por CPF+distribuidora)
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

    -- 5b. NOVO: Expandir distribuidora_nome em linhas individuais
    --     '3'     -> (staging_id, 3)
    --     '1,2,3' -> (staging_id, 1), (staging_id, 2), (staging_id, 3)
    --     'multi' -> (staging_id, 1), (staging_id, 2), (staging_id, 3), ...
    DROP TEMPORARY TABLE IF EXISTS tmp_staging_dist;
    CREATE TEMPORARY TABLE tmp_staging_dist (
        staging_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (staging_id, distribuidora_id)
    ) ENGINE=InnoDB;

    -- Arquivos cujo distribuidora_nome começa com dígito (single ou comma-separated)
    INSERT INTO tmp_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome REGEXP '^[0-9]'
      AND FIND_IN_SET(d.id, REPLACE(si.distribuidora_nome, ' ', '')) > 0;

    -- Arquivos com distribuidora_nome não-numérica ('multi', etc.) -> todas
    INSERT IGNORE INTO tmp_staging_dist (staging_id, distribuidora_id)
    SELECT si.id, d.id
    FROM staging_imports si
    CROSS JOIN distribuidoras d
    WHERE si.distribuidora_nome NOT REGEXP '^[0-9]';

    -- 5c. NOVO: Melhor status por CPF por arquivo (evita double-counting)
    --     Se consolidado em qualquer dist do arquivo -> ativo
    --     Senão se processado em qualquer dist -> inativo
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

    -- 6. Agregar resultados por arquivo
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
    ORDER BY si.id DESC;

    -- Cleanup
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


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print("=" * 60)
    print("Fix sp_refresh_dashboard_arquivos_agg: multi-distribuidora")
    print("=" * 60)

    # 1. Recriar SP
    print("\n[1/3] Recriando SP...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg")
    conn.commit()
    cur.execute(NEW_SP)
    conn.commit()
    print("  OK")

    # 2. Executar SP e medir tempo
    print("\n[2/3] Executando SP...")
    t0 = time.time()
    try:
        cur.execute("CALL sp_refresh_dashboard_arquivos_agg()")
        conn.commit()
        elapsed = time.time() - t0
        print(f"  OK ({elapsed:.1f}s)")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  ERRO ({elapsed:.1f}s): {e}")
        conn.close()
        return

    # 3. Verificar resultados
    print("\n[3/3] Resultados:")
    cur.execute("""
        SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_processados,
               ativos, inativos, cpfs_ineditos, ucs_ineditas
        FROM dashboard_arquivos_agg ORDER BY data_carga
    """)
    for r in cur.fetchall():
        print(f"  {r[0]:55s} | carga={r[1]} | cpfs={r[2]:>6} | proc={r[3]:>6} | ativos={r[4]:>6} | inat={r[5]:>6}")

    # Totais
    cur.execute("SELECT SUM(cpfs_processados), SUM(ativos), SUM(inativos) FROM dashboard_arquivos_agg")
    t = cur.fetchone()
    print(f"\n  TOTAIS: processados={t[0]:,}  ativos={t[1]:,}  inativos={t[2]:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
