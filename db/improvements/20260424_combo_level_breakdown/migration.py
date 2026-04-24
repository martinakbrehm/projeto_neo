"""
Migration: Breakdown de combos inéditas por arquivo (nível combo CPF+UC+dist)
Data: 2026-04-24

Objetivo:
  A tabela "Visão Geral" do dashboard deve mostrar como TOTAL apenas as
  combinações CPF+UC inéditas (mesmas combos_novas da tabela cobertura).
  A partir dessas, mostrar quantas já rodaram (processadas), pendentes,
  ativas e inativas — tudo a nível de COMBO (CPF+UC+dist), não de CPF.

Alterações:
  1. Adiciona 3 colunas à dashboard_arquivos_agg:
     combos_processadas, combos_ativas, combos_inativas
  2. Reescreve sp_refresh_dashboard_arquivos_agg com novo passo 6b/9b
     que calcula status por combo via tabela_macros
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

ALTER_SQL = [
    "ALTER TABLE dashboard_arquivos_agg ADD COLUMN combos_processadas INT UNSIGNED NOT NULL DEFAULT 0 AFTER ucs_ineditas",
    "ALTER TABLE dashboard_arquivos_agg ADD COLUMN combos_ativas INT UNSIGNED NOT NULL DEFAULT 0 AFTER combos_processadas",
    "ALTER TABLE dashboard_arquivos_agg ADD COLUMN combos_inativas INT UNSIGNED NOT NULL DEFAULT 0 AFTER combos_ativas",
]

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
    -- 1b. Distribuidora de arquivos single-dist
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
    -- 2. EXATAMENTE 1 distribuidora por staging row
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
    -- 3. Total de combos (CPF+UC) por arquivo
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
    -- 6. Status do CPF por distribuidora (último resultado) — p/ contagem geral
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
    -- 6b. Status por COMBO (CPF+UC+dist) — último resultado de tabela_macros
    --     Usado para breakdown das combos inéditas
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_combo_macro_status;
    CREATE TEMPORARY TABLE tmp_combo_macro_status (
        cpf              CHAR(11)     NOT NULL,
        uc_key           VARCHAR(20)  NOT NULL,
        distribuidora_id INT UNSIGNED NOT NULL,
        status           VARCHAR(30)  NOT NULL,
        PRIMARY KEY (cpf, uc_key, distribuidora_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_combo_macro_status (cpf, uc_key, distribuidora_id, status)
    SELECT cl.cpf, cu.uc, g.distribuidora_id, tm.status
    FROM (
        SELECT cliente_id, cliente_uc_id, distribuidora_id, MAX(id) AS max_id
        FROM tabela_macros
        WHERE status != 'pendente' AND resposta_id IS NOT NULL
          AND cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, cliente_uc_id, distribuidora_id
    ) g
    INNER JOIN tabela_macros tm ON tm.id = g.max_id
    INNER JOIN clientes cl ON cl.id = g.cliente_id
    INNER JOIN cliente_uc cu ON cu.id = g.cliente_uc_id
    ON DUPLICATE KEY UPDATE status = VALUES(status);

    -- =========================================================
    -- 7. Status por CPF×arquivo (usando dist real do row) — p/ contagem geral
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
    -- 9. Agregar por arquivo (CPF-level — mantido para compatibilidade)
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
    -- 9b. Agregar COMBOS INÉDITAS por arquivo (nível combo CPF+UC+dist)
    --     Para cada combo inédita: verificar se foi processada em tabela_macros
    -- =========================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_combo_status;
    CREATE TEMPORARY TABLE tmp_arq_combo_status (
        staging_id         INT UNSIGNED NOT NULL,
        combos_processadas INT UNSIGNED NOT NULL DEFAULT 0,
        combos_ativas      INT UNSIGNED NOT NULL DEFAULT 0,
        combos_inativas    INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_arq_combo_status (staging_id, combos_processadas, combos_ativas, combos_inativas)
    SELECT
        cf.first_staging_id AS staging_id,
        COUNT(cms.cpf)                                                         AS combos_processadas,
        SUM(CASE WHEN cms.status = 'consolidado' THEN 1 ELSE 0 END)           AS combos_ativas,
        SUM(CASE WHEN cms.cpf IS NOT NULL AND cms.status != 'consolidado'
                 THEN 1 ELSE 0 END)                                            AS combos_inativas
    FROM tmp_combo_first cf
    LEFT JOIN tmp_combo_macro_status cms
        ON cms.cpf = cf.normalized_cpf
        AND cms.uc_key = cf.uc_key
        AND cms.distribuidora_id = cf.distribuidora_id
    GROUP BY cf.first_staging_id;

    -- =========================================================
    -- 10. INSERT final
    -- =========================================================
    TRUNCATE TABLE dashboard_arquivos_agg;

    INSERT INTO dashboard_arquivos_agg
        (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
         cpfs_ineditos, ucs_ineditas, combos_processadas, combos_ativas, combos_inativas,
         ineditos_processados, ineditos_ativos, ineditos_inativos)
    SELECT
        si.filename,
        DATE(si.created_at),
        COALESCE(ac.cpfs_no_arquivo, 0),
        COALESCE(ast.cpfs_processados, 0),
        COALESCE(ast.ativos, 0),
        COALESCE(ast.inativos, 0),
        COALESCE(ined.cpfs_ineditos, 0),
        COALESCE(ined.ucs_ineditas, 0),
        COALESCE(acs.combos_processadas, 0),
        COALESCE(acs.combos_ativas, 0),
        COALESCE(acs.combos_inativas, 0),
        COALESCE(ast.ineditos_proc, 0),
        COALESCE(ast.ineditos_ativos, 0),
        COALESCE(ast.ineditos_inativos, 0)
    FROM staging_imports si
    LEFT JOIN tmp_arq_cpfs ac ON ac.staging_id = si.id
    LEFT JOIN tmp_arq_status ast ON ast.staging_id = si.id
    LEFT JOIN tmp_ineditos ined ON ined.staging_id = si.id
    LEFT JOIN tmp_arq_combo_status acs ON acs.staging_id = si.id
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
    DROP TEMPORARY TABLE IF EXISTS tmp_combo_macro_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_file_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_is_ined;
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_combo_status;
END
"""


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print("=" * 70)
    print("Migration: combo-level breakdown das combos inéditas")
    print("=" * 70)

    # 1. ALTER TABLE — adicionar novas colunas
    for sql in ALTER_SQL:
        col_name = sql.split("ADD COLUMN ")[1].split(" ")[0]
        cur.execute(f"SHOW COLUMNS FROM dashboard_arquivos_agg LIKE '{col_name}'")
        if cur.fetchone():
            print(f"  Coluna {col_name} já existe — pulando")
        else:
            print(f"  Adicionando coluna {col_name}...")
            cur.execute(sql)
            conn.commit()
            print("    OK")

    # 2. Recriar SP
    print("\nRecriando sp_refresh_dashboard_arquivos_agg...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg")
    conn.commit()
    cur.execute(SP_ARQUIVOS)
    conn.commit()
    print("  OK")

    # 3. Executar SP
    print("\n[CALL] sp_refresh_dashboard_arquivos_agg...")
    t0 = time.time()
    try:
        cur.execute("CALL sp_refresh_dashboard_arquivos_agg()")
        conn.commit()
        print(f"  OK ({time.time()-t0:.1f}s)")
    except Exception as e:
        print(f"  ERRO ({time.time()-t0:.1f}s): {e}")
        conn.close()
        return

    # 4. Validar resultado
    print("\n" + "=" * 70)
    print("RESULTADO: dashboard_arquivos_agg (combo-level)")
    print("=" * 70)
    cur.execute("""
        SELECT arquivo, data_carga, cpfs_no_arquivo,
               ucs_ineditas, combos_processadas, combos_ativas, combos_inativas
        FROM dashboard_arquivos_agg ORDER BY data_carga, arquivo
    """)
    for r in cur.fetchall():
        arq, dt, total, ined, proc, at, ina = r
        pend = ined - proc
        pct_at = round(at / proc * 100, 1) if proc > 0 else 0
        pct_in = round(ina / proc * 100, 1) if proc > 0 else 0
        # Validação: proc = at + ina, pend >= 0
        ok_sum = "OK" if proc == at + ina else f"ERRO proc({proc})!=at({at})+ina({ina})"
        ok_pend = "OK" if pend >= 0 else f"ERRO pend({pend})<0"
        print(f"  {arq:55s}")
        print(f"    inéditas={ined:>6} | proc={proc:>6} | pend={pend:>6} | at={at:>6} ({pct_at:.1f}%) | ina={ina:>6} ({pct_in:.1f}%)")
        print(f"    check: soma={ok_sum}, pend={ok_pend}")
        print()

    # 5. Cross-check com cobertura
    print("=" * 70)
    print("CROSS-CHECK: ucs_ineditas vs combos_novas (cobertura)")
    print("=" * 70)
    cur.execute("""
        SELECT a.arquivo, a.ucs_ineditas, c.combos_novas,
               CASE WHEN a.ucs_ineditas = c.combos_novas THEN 'OK' ELSE 'DIFERENTE' END AS chk
        FROM dashboard_arquivos_agg a
        JOIN dashboard_cobertura_agg c ON c.arquivo = a.arquivo
        ORDER BY a.data_carga, a.arquivo
    """)
    all_ok = True
    for r in cur.fetchall():
        arq, ined, novas, chk = r
        if chk != "OK":
            all_ok = False
        print(f"  {arq:55s} ined={ined:>6} novas={novas:>6} [{chk}]")

    if all_ok:
        print("\n  TODOS OK: ucs_ineditas == combos_novas em todos os arquivos")
    else:
        print("\n  ATENÇÃO: Há divergências!")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
