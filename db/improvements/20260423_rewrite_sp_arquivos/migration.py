"""
Migration: Reescrever sp_refresh_dashboard_arquivos_agg para performance
Data: 2026-04-23

Problema: A SP antiga fazia JOINs cruzados entre staging_import_rows (885k),
tabela_macros (911k), clientes (446k), cliente_uc (681k) numa única query.
Isso resultava em execuções de 50+ minutos que davam timeout.

Solução: Decompor em 5 temp tables pequenas com JOINs parciais,
cada uma usando os indexes existentes. O INSERT final junta apenas
os resultados pré-computados (12 linhas × 12 linhas).
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
    --    Usa duas temp tables separadas para evitar "Can't reopen table"
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

    -- Tabela final de ineditos: merge das duas acima
    DROP TEMPORARY TABLE IF EXISTS tmp_ineditos;
    CREATE TEMPORARY TABLE tmp_ineditos (
        staging_id    INT UNSIGNED NOT NULL,
        cpfs_ineditos INT UNSIGNED NOT NULL DEFAULT 0,
        ucs_ineditas  INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (staging_id)
    ) ENGINE=InnoDB;

    -- Insere CPFs ineditos
    INSERT INTO tmp_ineditos (staging_id, cpfs_ineditos, ucs_ineditas)
    SELECT staging_id, cpfs_ineditos, 0 FROM tmp_cpf_ined_counts;

    -- Atualiza UCs para staging_ids que ja tem CPFs
    UPDATE tmp_ineditos i
    INNER JOIN tmp_uc_ined_counts u ON u.staging_id = i.staging_id
    SET i.ucs_ineditas = u.ucs_ineditas;

    -- Insere staging_ids que so tem UCs (sem CPFs ineditos)
    INSERT INTO tmp_ineditos (staging_id, cpfs_ineditos, ucs_ineditas)
    SELECT u.staging_id, 0, u.ucs_ineditas
    FROM tmp_uc_ined_counts u
    LEFT JOIN tmp_cpf_ined_counts c ON c.staging_id = u.staging_id
    WHERE c.staging_id IS NULL;

    -- 5. Lookup CPF → status (ultimo resultado por CPF+distribuidora)
    --    Usa idx_tm_sp_arquivos (status, resposta_id, cliente_uc_id, cliente_id, distribuidora_id, id)
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

    -- 6. Agregar resultados por arquivo: processados/ativos/inativos
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
        COUNT(DISTINCT CASE WHEN cs.status IS NOT NULL THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cs.status = 'consolidado' THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cs.status IN ('excluido','reprocessar') THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cs.status IS NOT NULL AND cf.first_staging_id = sir.staging_id
              THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cs.status = 'consolidado' AND cf.first_staging_id = sir.staging_id
              THEN sir.normalized_cpf END),
        COUNT(DISTINCT CASE WHEN cs.status IN ('excluido','reprocessar') AND cf.first_staging_id = sir.staging_id
              THEN sir.normalized_cpf END)
    FROM staging_import_rows sir
    INNER JOIN staging_imports si ON si.id = sir.staging_id
    LEFT JOIN tmp_cpf_status cs
        ON cs.cpf = sir.normalized_cpf
        AND cs.distribuidora_id = CAST(si.distribuidora_nome AS UNSIGNED)
    LEFT JOIN tmp_cpf_first cf ON cf.normalized_cpf = sir.normalized_cpf
    WHERE sir.validation_status = 'valid'
    GROUP BY sir.staging_id;

    -- 7. INSERT final: junta tudo (12 linhas × 12 linhas)
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
    DROP TEMPORARY TABLE IF EXISTS tmp_arq_status;
END
"""


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print("=" * 60)
    print("Reescrevendo sp_refresh_dashboard_arquivos_agg")
    print("=" * 60)

    # 1. Recriar SP
    print("\n[1/4] Recriando SP...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_arquivos_agg")
    conn.commit()
    cur.execute(NEW_SP)
    conn.commit()
    print("  OK")

    # 2. Executar todas as 3 SPs e medir tempo
    for sp in [
        "sp_refresh_dashboard_macros_agg",
        "sp_refresh_dashboard_arquivos_agg",
        "sp_refresh_dashboard_cobertura_agg",
    ]:
        print(f"\n[CALL] {sp}...")
        t0 = time.time()
        try:
            cur.execute(f"CALL {sp}()")
            conn.commit()
            elapsed = time.time() - t0
            print(f"  OK ({elapsed:.1f}s)")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  ERRO ({elapsed:.1f}s): {e}")

    # 3. Verificar contagens
    print("\n" + "=" * 60)
    print("RESULTADO FINAL")
    print("=" * 60)
    for tbl in ["dashboard_macros_agg", "dashboard_arquivos_agg", "dashboard_cobertura_agg"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl}: {cur.fetchone()[0]} rows")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
