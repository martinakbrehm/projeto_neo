"""
Migration: Separar 'inativas' em 'excluidas' e 'reprocessar' no dashboard_arquivos_agg.

Conceitos:
  - Processadas = tudo que rodou na macro (consolidado + excluido + reprocessar)
  - Pendentes   = combos que NUNCA rodaram (verdadeiramente pendentes)
  - Ativas      = consolidado
  - Excluídas   = excluido
  - Reprocessar = reprocessar (já rodou, mas vai rodar de novo)

Alterações:
  1. ADD COLUMN combos_excluidas INT UNSIGNED DEFAULT 0
  2. ADD COLUMN combos_reprocessar INT UNSIGNED DEFAULT 0
  3. DROP COLUMN combos_inativas (substituída pelas duas acima)
  4. Recria sp_refresh_dashboard_arquivos_agg com o step 9b atualizado
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import config, pymysql

def run():
    conn = pymysql.connect(**config.db_destino())
    cur = conn.cursor()

    # --- 1. Alterar tabela dashboard_arquivos_agg ---
    print("[1/3] Alterando dashboard_arquivos_agg ...")

    # Adicionar colunas novas (se não existem)
    cur.execute("SHOW COLUMNS FROM dashboard_arquivos_agg LIKE 'combos_excluidas'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE dashboard_arquivos_agg ADD COLUMN combos_excluidas INT UNSIGNED NOT NULL DEFAULT 0 AFTER combos_ativas")
        print("  + combos_excluidas adicionada")
    else:
        print("  = combos_excluidas já existe")

    cur.execute("SHOW COLUMNS FROM dashboard_arquivos_agg LIKE 'combos_reprocessar'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE dashboard_arquivos_agg ADD COLUMN combos_reprocessar INT UNSIGNED NOT NULL DEFAULT 0 AFTER combos_excluidas")
        print("  + combos_reprocessar adicionada")
    else:
        print("  = combos_reprocessar já existe")

    # Remover combos_inativas (não mais usada)
    cur.execute("SHOW COLUMNS FROM dashboard_arquivos_agg LIKE 'combos_inativas'")
    if cur.fetchone():
        cur.execute("ALTER TABLE dashboard_arquivos_agg DROP COLUMN combos_inativas")
        print("  - combos_inativas removida")
    else:
        print("  = combos_inativas já ausente")

    conn.commit()

    # --- 2. Recriar stored procedure ---
    print("[2/3] Recriando sp_refresh_dashboard_arquivos_agg ...")

    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg")
    conn.commit()

    sp_body = """
CREATE PROCEDURE sp_refresh_dashboard_arquivos_agg()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- 0. Ordem cronológica dos arquivos
    DROP TEMPORARY TABLE IF EXISTS tmp_stg_order;
    CREATE TEMPORARY TABLE tmp_stg_order (
        staging_id INT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT id AS staging_id, created_at FROM staging_imports;

    -- 1. Distribuidora REAL por (cpf, uc)
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

    -- 1b. Distribuidora de arquivos single-dist
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

    -- 2. EXATAMENTE 1 distribuidora por staging row
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

    -- 3. Total de combos (CPF+UC) por arquivo
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_cpfs;
    CREATE TEMPORARY TABLE tmp_arq_cpfs (
        staging_id       INT UNSIGNED NOT NULL,
        cpfs_no_arquivo  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB
    SELECT staging_id, COUNT(*) AS cpfs_no_arquivo
    FROM tmp_row_dist
    GROUP BY staging_id;

    -- 4. Primeiro staging para cada combo (CPF, UC, dist)
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
        SELECT rd.normalized_cpf, rd.uc_key, rd.distribuidora_id, rd.staging_id,
               ROW_NUMBER() OVER (
                   PARTITION BY rd.normalized_cpf, rd.uc_key, rd.distribuidora_id
                   ORDER BY so.created_at, rd.staging_id
               ) AS rn
        FROM tmp_row_dist rd
        INNER JOIN tmp_stg_order so ON so.staging_id = rd.staging_id
    ) sub
    WHERE sub.rn = 1;

    -- 5. Contar inéditos por arquivo
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

    -- 6. Status do CPF por distribuidora (último resultado)
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

    -- 6b. Status por COMBO (CPF+UC+dist) — separa consolidado/excluido/reprocessar
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

    -- 7. Status por CPF x arquivo
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
    SELECT rd.staging_id, rd.normalized_cpf, 1,
        MAX(CASE WHEN cs.status = 'consolidado' THEN 1 ELSE 0 END),
        CASE WHEN MAX(CASE WHEN cs.status = 'consolidado' THEN 1 ELSE 0 END) = 0
             THEN 1 ELSE 0 END
    FROM tmp_row_dist rd
    INNER JOIN tmp_cpf_status cs
        ON cs.cpf = rd.normalized_cpf AND cs.distribuidora_id = rd.distribuidora_id
    WHERE rd.distribuidora_id > 0
    GROUP BY rd.staging_id, rd.normalized_cpf;

    -- 8. Flag inédito por CPF x arquivo
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

    -- 9. Agregar por arquivo (CPF-level)
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
    SELECT rd.staging_id,
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1      THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1     THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_processado = 1 AND ci.is_inedito = 1 THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_ativo = 1 AND ci.is_inedito = 1 THEN rd.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cfs.is_inativo = 1 AND ci.is_inedito = 1 THEN rd.normalized_cpf END)
    FROM tmp_row_dist rd
    LEFT JOIN tmp_cpf_file_status cfs ON cfs.staging_id = rd.staging_id AND cfs.cpf = rd.normalized_cpf
    LEFT JOIN tmp_cpf_is_ined ci ON ci.staging_id = rd.staging_id AND ci.normalized_cpf = rd.normalized_cpf
    GROUP BY rd.staging_id;

    -- 9b. Combos inéditas por arquivo: processadas / ativas / excluídas / reprocessar
    --     processadas = tudo que rodou (consolidado + excluido + reprocessar)
    --     pendentes   = combos inéditas - processadas (calculado no app)
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_combo_status;
    CREATE TEMPORARY TABLE tmp_arq_combo_status (
        staging_id          INT UNSIGNED NOT NULL,
        combos_processadas  INT UNSIGNED NOT NULL DEFAULT 0,
        combos_ativas       INT UNSIGNED NOT NULL DEFAULT 0,
        combos_excluidas    INT UNSIGNED NOT NULL DEFAULT 0,
        combos_reprocessar  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_arq_combo_status (staging_id, combos_processadas, combos_ativas, combos_excluidas, combos_reprocessar)
    SELECT
        cf.first_staging_id AS staging_id,
        COUNT(cms.cpf)                                                    AS combos_processadas,
        SUM(CASE WHEN cms.status = 'consolidado' THEN 1 ELSE 0 END)      AS combos_ativas,
        SUM(CASE WHEN cms.status = 'excluido'    THEN 1 ELSE 0 END)      AS combos_excluidas,
        SUM(CASE WHEN cms.status = 'reprocessar' THEN 1 ELSE 0 END)      AS combos_reprocessar
    FROM tmp_combo_first cf
    LEFT JOIN tmp_combo_macro_status cms
        ON cms.cpf = cf.normalized_cpf
        AND cms.uc_key = cf.uc_key
        AND cms.distribuidora_id = cf.distribuidora_id
    GROUP BY cf.first_staging_id;

    -- 10. INSERT final
    TRUNCATE TABLE dashboard_arquivos_agg;

    INSERT INTO dashboard_arquivos_agg
        (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
         cpfs_ineditos, ucs_ineditas,
         combos_processadas, combos_ativas, combos_excluidas, combos_reprocessar,
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
        COALESCE(acs.combos_excluidas, 0),
        COALESCE(acs.combos_reprocessar, 0),
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
    cur.execute(sp_body)
    conn.commit()
    print("  SP recriada com sucesso")

    # --- 3. Executar SP para popular dados ---
    print("[3/3] Executando sp_refresh_dashboard_arquivos_agg ...")
    cur.execute("CALL sp_refresh_dashboard_arquivos_agg()")
    conn.commit()

    # Verificar resultado
    cur.execute("SELECT arquivo, combos_processadas, combos_ativas, combos_excluidas, combos_reprocessar FROM dashboard_arquivos_agg")
    rows = cur.fetchall()
    print(f"\n{'Arquivo':<40} {'Proc':>8} {'Ativas':>8} {'Excl':>8} {'Reproc':>8}")
    print("-" * 76)
    for r in rows:
        print(f"{r[0]:<40} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,} {r[4]:>8,}")

    cur.close()
    conn.close()
    print("\nMigration completa!")


if __name__ == '__main__':
    run()
