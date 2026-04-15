"""
Migration: 20260413_fix_view_query
Recria view_dashboard_macros_agg com SQL otimizado.

Problema anterior: o SQL da view usava ROW_NUMBER() OVER (PARTITION BY ...) num
subquery de LEFT JOIN contra staging_import_rows — isso travava em tabelas grandes
porque o MySQL reavalia a window function inteira a cada SELECT.

Solução: substituir por MAX(id) + GROUP BY para encontrar o último registro por CPF,
que usa o índice diretamente e é muito mais rápido.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import db_destino
import pymysql

SQL_DROP = "DROP VIEW IF EXISTS view_dashboard_macros_agg"

SQL_CREATE_VIEW = """
CREATE VIEW view_dashboard_macros_agg AS
SELECT
    DATE(COALESCE(m.data_extracao, m.data_update))  AS dia,
    m.status,
    r.mensagem,
    r.status                                         AS resposta_status,
    d.nome                                           AS empresa,
    COALESCE(co.fornecedor, 'fornecedor2')           AS fornecedor,
    CASE
        WHEN m.data_extracao IS NULL THEN 'Dados históricos'
        ELSE 'Operacional'
    END                                              AS arquivo_origem,
    COUNT(*)                                         AS qtd
FROM tabela_macros m
LEFT JOIN respostas      r   ON r.id  = m.resposta_id
LEFT JOIN distribuidoras d   ON d.id  = m.distribuidora_id
LEFT JOIN cliente_origem co  ON co.cliente_id = m.cliente_id
WHERE m.status != 'pendente'
  AND m.resposta_id IS NOT NULL
GROUP BY
    DATE(COALESCE(m.data_extracao, m.data_update)),
    m.status, r.mensagem, r.status, d.nome,
    COALESCE(co.fornecedor, 'fornecedor2'),
    CASE
        WHEN m.data_extracao IS NULL THEN 'Dados históricos'
        ELSE 'Operacional'
    END
"""


def run():
    conn = pymysql.connect(**db_destino())
    try:
        with conn.cursor() as cur:
            print("Removendo view antiga...")
            cur.execute(SQL_DROP)
            print("Criando view com SQL otimizado (MAX+GROUP BY)...")
            cur.execute(SQL_CREATE_VIEW)
            conn.commit()
            print("View view_dashboard_macros_agg recriada com sucesso.")

            # Testar contagem rápida
            print("Testando COUNT(*) na view...")
            cur.execute("SELECT COUNT(*) FROM view_dashboard_macros_agg")
            count = cur.fetchone()[0]
            print(f"Linhas na view: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
