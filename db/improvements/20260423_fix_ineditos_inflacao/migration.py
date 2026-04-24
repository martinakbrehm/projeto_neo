"""
Migration: Corrigir inflação de inéditos — usar distribuidora real do macros
Data: 2026-04-23

Problema:
  A SP anterior fazia CROSS JOIN de staging rows com TODAS as distribuidoras
  do arquivo. Para arquivos multi-dist (historico=4 dists, 300k=3 dists),
  isso multiplicava ucs_ineditas por 3-4x (115K linhas → 464K "inéditos").
  Mas o pipeline cria apenas 1 entrada em tabela_macros por staging row.

Correção:
  Cada staging row recebe EXATAMENTE 1 distribuidora:
  1. Se (cpf, uc) existe em tabela_macros → usa distribuidora real do macros
  2. Se arquivo é single-dist → usa distribuidora do arquivo
  3. Senão → distribuidora 0 (desconhecida)

  Assim ucs_ineditas ≤ cpfs_no_arquivo SEMPRE.
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
    -- 1. Distribuidora REAL por (cpf, uc) — vinda de tabela_macros
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_actual_dist;
    CREATE TEMPORARY TABLE tmp_actual_dist (
        cpf              CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (cpf, uc_key)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_actual_dist (cpf, uc_key, distribuidora_id)
    SELECT cl.cpf, cu.uc, MIN(tm.distribuidora_id)
    FROM tabela_macros tm
    JOIN clientes cl   ON cl.id = tm.cliente_id
    JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
    WHERE tm.cliente_uc_id IS NOT NULL
    GROUP BY cl.cpf, cu.uc;

    -- =========================================================
    -- 1b. Distribuidora de arquivos single-dist (distribuidora_nome = número único)
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_single_dist;
    CREATE TEMPORARY TABLE tmp_single_dist (
        staging_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_single_dist (staging_id, distribuidora_id)
    SELECT si.id, CAST(si.distribuidora_nome AS UNSIGNED)
    FROM staging_imports si
    WHERE si.distribuidora_nome REGEXP '^[0-9]+$';

    -- =========================================================
    -- 2. Atribuir EXATAMENTE 1 distribuidora por staging row
    --    Prioridade: macros > single-dist file > 0 (desconhecida)
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_row_dist;
    CREATE TEMPORARY TABLE tmp_row_dist (
        staging_id       INT UNSIGNED NOT NULL,
        normalized_cpf   CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        INDEX idx_sid (staging_id),
        INDEX idx_cpf_uc_dist (normalized_cpf, uc_key, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_row_dist (staging_id, normalized_cpf, uc_key, distribuidora_id)
    SELECT sir.staging_id,
           sir.normalized_cpf,
           COALESCE(NULLIF(sir.normalized_uc, ''), '') AS uc_key,
           COALESCE(ad.distribuidora_id, sd.distribuidora_id, 0) AS distribuidora_id
    FROM staging_import_rows sir
    LEFT JOIN tmp_actual_dist ad
        ON ad.cpf = sir.normalized_cpf
        AND ad.uc_key = COALESCE(NULLIF(sir.normalized_uc, ''), '')
    LEFT JOIN tmp_single_dist sd ON sd.staging_id = sir.staging_id
    WHERE sir.validation_status = 'valid'
    GROUP BY sir.staging_id, sir.normalized_cpf, uc_key;

    -- =========================================================
    -- 3. Total de combos (CPF+UC) por arquivo (contagem bruta)
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_cpfs;
    CREATE TEMPORARY TABLE tmp_arq_cpfs (
        staging_id       INT UNSIGNED NOT NULL,
        cpfs_no_arquivo  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT staging_id,
           COUNT(*) AS cpfs_no_arquivo
    FROM tmp_row_dist
    GROUP BY staging_id;

    -- =========================================================
    -- 4. Primeiro staging para cada combo (CPF, UC, dist) — CRONOLÓGICO
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
        SELECT rd.normalized_cpf,
               rd.uc_key,
               rd.distribuidora_id,
               rd.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY rd.normalized_cpf, rd.uc_key, rd.distribuidora_id
                   ORDER BY so.created_at, rd.staging_id
               ) AS rn
        FROM tmp_row_dist rd
        INNER JOIN tmp_stg_order so ON so.staging_id = rd.staging_id
    ) sub
    WHERE sub.rn = 1;

    -- =========================================================
    -- 5. Contar inéditos por arquivo
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
    -- 6. Status do CPF por distribuidora (último resultado)
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
    -- 7. Status por CPF×arquivo (usando dist real do row)
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_file_status;
    CREATE TEMPORARY TABLE tmp_cpf_file_status (
        staging_id    INT UNSIGNED NOT NULL,
        cpf           CHAR(11) NOT NULL,
        is_processado TINYINT(1) NOT NULL DEFAULT 0,
        is_ativo      TINYINT(1) NOT NULL DEFAULT 0,
        is_inativo    TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id, cpf)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cpf_file_status (staging_id, cpf, is_processado, is_ativo, is_inativo)
    SELECT
        rd.staging_id,
        rd.normalized_cpf,
        1,
        MAX(CASE WHEN cs.status = 'consolidado' THEN 1 ELSE 0 END),
        CASE WHEN MAX(CASE WHEN cs.status = 'consolidado' THEN 1 ELSE 0 END) = 0
             THEN 1 ELSE 0 END
    FROM tmp_row_dist rd
    INNER JOIN tmp_cpf_status cs
        ON cs.cpf = rd.normalized_cpf
        AND cs.distribuidora_id = rd.distribuidora_id
    WHERE rd.distribuidora_id > 0
    GROUP BY rd.staging_id, rd.normalized_cpf;

    -- =========================================================
    -- 8. Flag inédito por CPF×arquivo
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_is_ined;
    CREATE TEMPORARY TABLE tmp_cpf_is_ined (
        staging_id     INT UNSIGNED NOT NULL,
        normalized_cpf CHAR(11) NOT NULL,
        is_inedito     TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id, normalized_cpf)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cpf_is_ined (staging_id, normalized_cpf, is_inedito)
    SELECT rd.staging_id, rd.normalized_cpf,
           MAX(CASE WHEN cf.first_staging_id = rd.staging_id THEN 1 ELSE 0 END) AS is_inedito
    FROM tmp_row_dist rd
    LEFT JOIN tmp_combo_first cf
        ON cf.normalized_cpf = rd.normalized_cpf
        AND cf.uc_key = rd.uc_key
        AND cf.distribuidora_id = rd.distribuidora_id
        AND cf.first_staging_id = rd.staging_id
    GROUP BY rd.staging_id, rd.normalized_cpf;

    -- =========================================================
    -- 9. Agregar por arquivo
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
        rd.staging_id,
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1      THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1     THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 AND ci.is_inedito = 1
              THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1 AND ci.is_inedito = 1
              THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1 AND ci.is_inedito = 1
              THEN rd.normalized_cpf END)
    FROM tmp_row_dist rd
    LEFT JOIN tmp_cpf_file_status cfs
        ON cfs.staging_id = rd.staging_id AND cfs.cpf = rd.normalized_cpf
    LEFT JOIN tmp_cpf_is_ined ci
        ON ci.staging_id = rd.staging_id AND ci.normalized_cpf = rd.normalized_cpf
    GROUP BY rd.staging_id;

    -- =========================================================
    -- 10. INSERT final
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
    DROP TEMPORARY TABLE IF EXISTS tmp_actual_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_single_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_row_dist;
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

    -- Distribuidora real por (cpf, uc) de macros
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_actual_dist;
    CREATE TEMPORARY TABLE tmp_cob_actual_dist (
        cpf              CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (cpf, uc_key)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_actual_dist (cpf, uc_key, distribuidora_id)
    SELECT cl.cpf, cu.uc, MIN(tm.distribuidora_id)
    FROM tabela_macros tm
    JOIN clientes cl   ON cl.id = tm.cliente_id
    JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
    WHERE tm.cliente_uc_id IS NOT NULL
    GROUP BY cl.cpf, cu.uc;

    -- Single-dist files
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_single_dist;
    CREATE TEMPORARY TABLE tmp_cob_single_dist (
        staging_id       INT UNSIGNED NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_single_dist (staging_id, distribuidora_id)
    SELECT si.id, CAST(si.distribuidora_nome AS UNSIGNED)
    FROM staging_imports si
    WHERE si.distribuidora_nome REGEXP '^[0-9]+$';

    -- 1 distribuidora por staging row
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_row_dist;
    CREATE TEMPORARY TABLE tmp_cob_row_dist (
        staging_id       INT UNSIGNED NOT NULL,
        normalized_cpf   CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        INDEX (staging_id),
        INDEX (normalized_cpf, uc_key, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_row_dist (staging_id, normalized_cpf, uc_key, distribuidora_id)
    SELECT sir.staging_id,
           sir.normalized_cpf,
           sir.normalized_uc,
           COALESCE(ad.distribuidora_id, sd.distribuidora_id, 0)
    FROM staging_import_rows sir
    LEFT JOIN tmp_cob_actual_dist ad
        ON ad.cpf = sir.normalized_cpf AND ad.uc_key = sir.normalized_uc
    LEFT JOIN tmp_cob_single_dist sd ON sd.staging_id = sir.staging_id
    WHERE sir.validation_status = 'valid'
      AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
    GROUP BY sir.staging_id, sir.normalized_cpf, sir.normalized_uc;

    -- Primeiro staging por (cpf, uc, dist)
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
    CREATE TEMPORARY TABLE tmp_cob_combo_first (
        normalized_cpf   CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        first_staging_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (normalized_cpf, uc_key, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_cob_combo_first (normalized_cpf, uc_key, distribuidora_id, first_staging_id)
    SELECT sub.normalized_cpf, sub.uc_key, sub.distribuidora_id, sub.staging_id
    FROM (
        SELECT rd.normalized_cpf, rd.uc_key, rd.distribuidora_id, rd.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY rd.normalized_cpf, rd.uc_key, rd.distribuidora_id
                   ORDER BY so.created_at, rd.staging_id
               ) AS rn
        FROM tmp_cob_row_dist rd
        INNER JOIN tmp_cob_stg_order so ON so.staging_id = rd.staging_id
    ) sub
    WHERE sub.rn = 1;

    -- Resultado
    DELETE FROM dashboard_cobertura_agg;

    INSERT INTO dashboard_cobertura_agg
        (arquivo, data_carga, total_combos, combos_novas, combos_existentes)
    SELECT
        si.filename AS arquivo,
        DATE(si.created_at) AS data_carga,
        COALESCE(tot.cnt, 0) AS total_combos,
        COALESCE(nov.cnt, 0) AS combos_novas,
        COALESCE(tot.cnt, 0) - COALESCE(nov.cnt, 0) AS combos_existentes
    FROM staging_imports si
    LEFT JOIN (
        SELECT staging_id, COUNT(*) AS cnt FROM tmp_cob_row_dist GROUP BY staging_id
    ) tot ON tot.staging_id = si.id
    LEFT JOIN (
        SELECT first_staging_id, COUNT(*) AS cnt FROM tmp_cob_combo_first GROUP BY first_staging_id
    ) nov ON nov.first_staging_id = si.id
    ORDER BY si.created_at, si.id;

    -- Cleanup
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_stg_order;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_actual_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_single_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_row_dist;
    DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first;
END
"""


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print("=" * 70)
    print("Fix: inéditos sem inflação — 1 distribuidora real por staging row")
    print("=" * 70)

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

    print("\n" + "=" * 70)
    print("RESULTADO: dashboard_arquivos_agg")
    print("=" * 70)
    cur.execute("""
        SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_ineditos, ucs_ineditas,
               cpfs_processados, ativos, inativos,
               ineditos_processados, ineditos_ativos, ineditos_inativos
        FROM dashboard_arquivos_agg ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        arq = r[0]
        total, cpf_ined, uc_ined = r[2], r[3], r[4]
        proc, ativos, inat = r[5], r[6], r[7]
        ined_proc, ined_at, ined_in = r[8], r[9], r[10]
        pend = total - proc
        flag = "OK" if uc_ined <= total else "INFLADO!"
        print(f"  {arq:55s}")
        print(f"    total={total:>6} | proc={proc:>6} | pend={pend:>6} | proc+pend={proc+pend:>6}")
        print(f"    cpf_ined={cpf_ined:>6} | uc_ined={uc_ined:>6} | ined_proc={ined_proc:>6} | {flag}")
        print()

    print("=" * 70)
    print("RESULTADO: dashboard_cobertura_agg")
    print("=" * 70)
    cur.execute("""
        SELECT arquivo, data_carga, total_combos, combos_novas, combos_existentes
        FROM dashboard_cobertura_agg ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        pct = round(r[3]/r[2]*100,1) if r[2]>0 else 0
        print(f"  {r[0]:55s} | combos={r[2]:>6} | novas={r[3]:>6} ({pct:>5.1f}%) | exist={r[4]:>6}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
