"""
Migration: 20260415_arquivo_origem_real
Atualiza dashboard_macros_agg para exibir o nome REAL do arquivo de staging
em vez de rótulos genéricos ('Dados históricos' / 'Operacional').

Alterações:
  1. ALTER arquivo_origem de VARCHAR(20) → VARCHAR(255) para caber nomes reais
  2. Recria stored procedure sp_refresh_dashboard_macros_agg com JOIN em
     staging_import_rows/staging_imports para resolver nome do arquivo.
     - Usa temp table com GROUP BY (MAX staging_id por CPF) → performático
     - CPFs sem staging mantêm 'Dados históricos'
  3. Executa refresh para popular com dados atualizados

Performance: a query pesada roda apenas no refresh (procedure), nunca no
dashboard load. O dashboard continua fazendo SELECT simples na tabela física.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import db_destino
import pymysql


SQL_ALTER_COLUMN = """
ALTER TABLE dashboard_macros_agg
    MODIFY COLUMN arquivo_origem VARCHAR(255) NOT NULL
"""

SQL_DROP_PROCEDURE = "DROP PROCEDURE IF EXISTS sp_refresh_dashboard_macros_agg"

SQL_CREATE_PROCEDURE = """
CREATE PROCEDURE sp_refresh_dashboard_macros_agg()
BEGIN
    -- ---------------------------------------------------------------
    -- 1. Temp table: mapeia CPF → nome do arquivo (último staging)
    --    MAX(staging_id) = arquivo mais recente por CPF
    --    Performance: single-pass GROUP BY, sem ROW_NUMBER
    -- ---------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_arquivo;
    CREATE TEMPORARY TABLE tmp_cpf_arquivo (
        cpf      CHAR(11)     NOT NULL,
        filename VARCHAR(255) NOT NULL,
        INDEX (cpf)
    );

    INSERT INTO tmp_cpf_arquivo (cpf, filename)
    SELECT sub.normalized_cpf, si.filename
    FROM (
        SELECT normalized_cpf, MAX(staging_id) AS latest_staging_id
        FROM staging_import_rows
        WHERE validation_status = 'valid'
        GROUP BY normalized_cpf
    ) sub
    JOIN staging_imports si ON si.id = sub.latest_staging_id;

    -- ---------------------------------------------------------------
    -- 2. TRUNCATE + INSERT com nome real do arquivo
    --    CPFs sem match em staging → 'Dados históricos'
    -- ---------------------------------------------------------------
    TRUNCATE TABLE dashboard_macros_agg;

    INSERT INTO dashboard_macros_agg
        (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
    SELECT
        DATE(COALESCE(m.data_extracao, m.data_update))          AS dia,
        m.status,
        r.mensagem,
        r.status                                                AS resposta_status,
        d.nome                                                  AS empresa,
        COALESCE(co.fornecedor, 'fornecedor2')                  AS fornecedor,
        COALESCE(ta.filename, 'Dados históricos')               AS arquivo_origem,
        COUNT(*)                                                AS qtd
    FROM tabela_macros m
    LEFT JOIN respostas        r   ON r.id  = m.resposta_id
    LEFT JOIN distribuidoras   d   ON d.id  = m.distribuidora_id
    LEFT JOIN cliente_origem   co  ON co.cliente_id = m.cliente_id
    LEFT JOIN clientes         cl  ON cl.id = m.cliente_id
    LEFT JOIN tmp_cpf_arquivo  ta  ON ta.cpf = cl.cpf
    WHERE m.status != 'pendente'
      AND m.resposta_id IS NOT NULL
    GROUP BY
        DATE(COALESCE(m.data_extracao, m.data_update)),
        m.status, r.mensagem, r.status, d.nome,
        COALESCE(co.fornecedor, 'fornecedor2'),
        COALESCE(ta.filename, 'Dados históricos');

    DROP TEMPORARY TABLE IF EXISTS tmp_cpf_arquivo;
END
"""


def run():
    conn = pymysql.connect(**db_destino())
    try:
        with conn.cursor() as cur:
            # 1. Ampliar coluna
            print("ALTER arquivo_origem VARCHAR(20) → VARCHAR(255)...")
            cur.execute(SQL_ALTER_COLUMN)
            conn.commit()

            # 2. Recriar stored procedure
            print("Recriando sp_refresh_dashboard_macros_agg com nomes reais...")
            cur.execute(SQL_DROP_PROCEDURE)
            cur.execute(SQL_CREATE_PROCEDURE)
            conn.commit()

            # 3. Popular dados
            print("Executando refresh (CALL sp_refresh_dashboard_macros_agg)...")
            cur.execute("CALL sp_refresh_dashboard_macros_agg()")
            conn.commit()

            # 4. Verificar resultado
            cur.execute("SELECT COUNT(*) FROM dashboard_macros_agg")
            count = cur.fetchone()[0]
            print(f"Linhas na tabela materializada: {count}")

            cur.execute("""
                SELECT arquivo_origem, SUM(qtd) AS total
                FROM dashboard_macros_agg
                GROUP BY arquivo_origem
                ORDER BY total DESC
            """)
            print("\nRegistros por arquivo_origem:")
            for r in cur.fetchall():
                print(f"  {r[0]:<50} {r[1]:>8,}")

            cur.execute("SELECT MIN(dia), MAX(dia) FROM dashboard_macros_agg")
            r = cur.fetchone()
            print(f"\nPeríodo: {r[0]} → {r[1]}")

    finally:
        conn.close()


if __name__ == "__main__":
    run()
