"""
Migration: Corrigir dia em dashboard_macros_agg para nunca ser anterior à
data de importação do arquivo (staging created_at).

PROBLEMA:
  Registros backfillados (reimportar_retroativo.py) têm data_update em março/
  datas antigas. Com MIN(staging_id) por (cpf, uc), esses registros são
  atribuídos ao primeiro arquivo que os importou, mas com dia = data original
  do processamento (e.g., 2026-03-03).

  Resultado: ao filtrar por um arquivo no dashboard, o resumo diário mostra
  datas anteriores à importação daquele arquivo — o que é confuso, pois a
  execução ocorreu antes do arquivo existir no sistema.

CORREÇÃO:
  Inclui DATE(si.created_at) na tabela temporária tmp_combo_arquivo.
  O campo `dia` passa a ser:
    GREATEST(DATE(COALESCE(m.data_extracao, m.data_update)), ta.staging_date)
  Para registros sem UC (Dados historicos), mantém a data original.

  Assim, processamentos anteriores à importação do arquivo aparecem no dia
  da importação — sem perda de dados, apenas reposicionados no tempo correto.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql
import time

NEW_SP = """
CREATE PROCEDURE `sp_refresh_dashboard_macros_agg`()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- -----------------------------------------------------------------------
    -- Mapeia cada par (cpf, uc) ao PRIMEIRO arquivo em que apareceu (MIN staging_id).
    -- Inclui a data de criação do staging para capear o dia mínimo da macro.
    -- -----------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_combo_arquivo;
    CREATE TEMPORARY TABLE tmp_combo_arquivo (
        cpf          CHAR(11)     NOT NULL,
        uc           CHAR(10)     NOT NULL,
        filename     VARCHAR(255) NOT NULL,
        staging_date DATE         NOT NULL,
        PRIMARY KEY (cpf, uc)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_combo_arquivo (cpf, uc, filename, staging_date)
    SELECT sub.normalized_cpf, sub.normalized_uc, si.filename, DATE(si.created_at)
    FROM (
        SELECT normalized_cpf, normalized_uc,
               MIN(staging_id) AS first_staging_id
        FROM staging_import_rows
        WHERE validation_status = 'valid'
          AND normalized_uc IS NOT NULL
          AND normalized_uc != ''
        GROUP BY normalized_cpf, normalized_uc
    ) sub
    JOIN staging_imports si ON si.id = sub.first_staging_id;

    TRUNCATE TABLE dashboard_macros_agg;

    INSERT INTO dashboard_macros_agg
        (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
    SELECT
        -- Para combos com arquivo: dia nunca anterior à data de importação do staging.
        -- Para Dados historicos (sem UC): mantém a data original do processamento.
        CASE
            WHEN ta.staging_date IS NOT NULL
            THEN GREATEST(DATE(COALESCE(m.data_extracao, m.data_update)), ta.staging_date)
            ELSE DATE(COALESCE(m.data_extracao, m.data_update))
        END                                             AS dia,
        m.status,
        r.mensagem,
        r.status                                        AS resposta_status,
        d.nome                                          AS empresa,
        COALESCE(co.fornecedor, 'fornecedor2')          AS fornecedor,
        COALESCE(ta.filename, 'Dados historicos')       AS arquivo_origem,
        COUNT(*)                                        AS qtd
    FROM tabela_macros m
    LEFT JOIN respostas      r  ON r.id  = m.resposta_id
    LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
    LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
    LEFT JOIN clientes       cl ON cl.id = m.cliente_id
    LEFT JOIN cliente_uc     cu ON cu.id = m.cliente_uc_id
    LEFT JOIN tmp_combo_arquivo ta ON ta.cpf = cl.cpf
                                  AND ta.uc  = cu.uc
    WHERE m.status != 'pendente'
      AND m.resposta_id IS NOT NULL
    GROUP BY
        CASE
            WHEN ta.staging_date IS NOT NULL
            THEN GREATEST(DATE(COALESCE(m.data_extracao, m.data_update)), ta.staging_date)
            ELSE DATE(COALESCE(m.data_extracao, m.data_update))
        END,
        m.status, r.mensagem, r.status, d.nome,
        COALESCE(co.fornecedor, 'fornecedor2'),
        COALESCE(ta.filename, 'Dados historicos');

    DROP TEMPORARY TABLE IF EXISTS tmp_combo_arquivo;
END
"""


def run():
    conn = pymysql.connect(**config.db_destino(), read_timeout=300, write_timeout=300)
    conn.autocommit(True)
    cur = conn.cursor()

    print("Dropping old procedure...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_macros_agg")

    print("Creating new procedure...")
    cur.execute(NEW_SP)
    print("Procedure created OK.")

    print("\nExecuting refresh to validate...")
    t0 = time.time()
    cur.execute("CALL sp_refresh_dashboard_macros_agg()")
    while cur.nextset():
        pass
    print(f"Refresh concluído em {time.time() - t0:.1f}s")

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT arquivo_origem) FROM dashboard_macros_agg")
    rows, arqs = cur.fetchone()
    print(f"dashboard_macros_agg: {rows} linhas, {arqs} arquivos distintos")

    # Mostrar range de datas por arquivo após fix
    cur.execute("""
        SELECT arquivo_origem, MIN(dia) min_dia, MAX(dia) max_dia, SUM(qtd) total
        FROM dashboard_macros_agg
        GROUP BY arquivo_origem
        ORDER BY min_dia, arquivo_origem
    """)
    print(f"\n{'arquivo':<55} {'min_dia':>12} {'max_dia':>12} {'total':>8}")
    for r in cur.fetchall():
        print(f"{str(r[0]):<55} {str(r[1]):>12} {str(r[2]):>12} {r[3]:>8}")

    conn.close()
    print("\nMigration concluída com sucesso.")


if __name__ == "__main__":
    run()
