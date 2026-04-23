"""
Migration: Otimizar índices para view_dashboard_macros_agg

Data: 2026-04-13
Autor: GitHub Copilot

Descrição:
- Adiciona índices específicos para otimizar a view view_dashboard_macros_agg.
- Foca em reduzir 'Using temporary' e 'Using filesort' no EXPLAIN.
- Índices para GROUP BY, JOINs e subqueries.

Mudanças:
- Adiciona índice composto em tabela_macros para cobrir filtros e GROUP BY.
- Adiciona índice em staging_import_rows para subquery ROW_NUMBER.
- Adiciona índices em tabelas de lookup (respostas, distribuidoras, etc.).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino  # noqa: E402

import pymysql


def up():
    """Executa a migration: adiciona índices de otimização."""
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    # Índice composto para tabela_macros: cobre WHERE e GROUP BY
    cur.execute("""
        CREATE INDEX idx_tabela_macros_agg_cover
        ON tabela_macros (status, resposta_id, data_extracao, data_update, distribuidora_id, cliente_id)
    """)

    # Índice para subquery ROW_NUMBER em staging_import_rows
    cur.execute("""
        CREATE INDEX idx_staging_rows_normcpf_id_desc
        ON staging_import_rows (normalized_cpf, id DESC)
    """)

    # Índices em tabelas de lookup para joins rápidos
    cur.execute("CREATE INDEX idx_respostas_lookup ON respostas (id, mensagem, status)")
    cur.execute("CREATE INDEX idx_distribuidoras_lookup ON distribuidoras (id, nome)")
    cur.execute("CREATE INDEX idx_cliente_origem_lookup ON cliente_origem (cliente_id, fornecedor)")
    cur.execute("CREATE INDEX idx_clientes_lookup ON clientes (id, cpf)")

    conn.commit()
    print("Índices adicionais criados para otimização da view.")

    cur.close()
    conn.close()


def down():
    """Reverte a migration: remove os índices."""
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    indexes_to_drop = [
        "DROP INDEX idx_tabela_macros_agg_cover ON tabela_macros",
        "DROP INDEX idx_staging_rows_normcpf_id_desc ON staging_import_rows",
        "DROP INDEX idx_respostas_lookup ON respostas",
        "DROP INDEX idx_distribuidoras_lookup ON distribuidoras",
        "DROP INDEX idx_cliente_origem_lookup ON cliente_origem",
        "DROP INDEX idx_clientes_lookup ON clientes",
    ]

    for sql in indexes_to_drop:
        try:
            cur.execute(sql)
        except pymysql.Error as e:
            print(f"Erro ao remover índice: {e}")

    conn.commit()
    print("Índices removidos.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    up()