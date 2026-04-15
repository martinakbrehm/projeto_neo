"""
Migration: Criar view agregada view_dashboard_macros_agg para o dashboard.

Agrupa tabela_macros por (dia, status, mensagem, empresa, fornecedor, arquivo_origem)
com COUNT(*) AS qtd — reduz de centenas de milhares de linhas para poucos milhares.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino  # noqa: E402
import pymysql


_VIEW_SQL = """
CREATE OR REPLACE VIEW view_dashboard_macros_agg AS
SELECT
    DATE(COALESCE(m.data_extracao, m.data_update))          AS dia,
    m.status,
    r.mensagem,
    r.status                                                 AS resposta_status,
    d.nome                                                   AS empresa,
    COALESCE(co.fornecedor, 'fornecedor2')                   AS fornecedor,
    CASE
        WHEN m.data_extracao IS NULL THEN 'Dados historicos'
        ELSE COALESCE(la.filename, 'Dados historicos')
    END                                                      AS arquivo_origem,
    COUNT(*)                                                 AS qtd
FROM tabela_macros m
LEFT JOIN respostas      r   ON r.id  = m.resposta_id
LEFT JOIN distribuidoras d   ON d.id  = m.distribuidora_id
LEFT JOIN cliente_origem co  ON co.cliente_id = m.cliente_id
LEFT JOIN clientes       cl  ON cl.id = m.cliente_id
LEFT JOIN (
    SELECT sir.normalized_cpf, si.filename,
           ROW_NUMBER() OVER (PARTITION BY sir.normalized_cpf ORDER BY sir.id DESC) AS rn
    FROM staging_import_rows sir
    JOIN staging_imports si ON si.id = sir.staging_id
    WHERE sir.validation_status = 'valid'
) la ON la.normalized_cpf = cl.cpf AND la.rn = 1
WHERE m.status != 'pendente'
  AND m.resposta_id IS NOT NULL
GROUP BY
    DATE(COALESCE(m.data_extracao, m.data_update)),
    m.status,
    r.mensagem,
    r.status,
    d.nome,
    COALESCE(co.fornecedor, 'fornecedor2'),
    CASE
        WHEN m.data_extracao IS NULL THEN 'Dados historicos'
        ELSE COALESCE(la.filename, 'Dados historicos')
    END
"""

_INDEXES = [
    ("idx_tm_status_resposta",    "tabela_macros",        "(status, resposta_id)"),
    ("idx_tm_datas",              "tabela_macros",        "(data_extracao, data_update)"),
    ("idx_tm_distribuidora",      "tabela_macros",        "(distribuidora_id)"),
    ("idx_tm_cliente",            "tabela_macros",        "(cliente_id)"),
    ("idx_clientes_cpf",          "clientes",             "(cpf)"),
    ("idx_co_cliente",            "cliente_origem",       "(cliente_id)"),
    ("idx_sir_cpf_id",            "staging_import_rows",  "(normalized_cpf, id DESC)"),
    ("idx_sir_staging_valid",     "staging_import_rows",  "(staging_id, validation_status)"),
]


def up():
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    try:
        cur.execute(_VIEW_SQL)
        conn.commit()
        print("View 'view_dashboard_macros_agg' criada/atualizada com sucesso.")
    except Exception as e:
        print(f"ERRO ao criar view: {e}")
        conn.close()
        raise

    for name, table, cols in _INDEXES:
        try:
            cur.execute(f"CREATE INDEX {name} ON {table} {cols}")
            print(f"  Indice criado: {name}")
        except pymysql.err.OperationalError as e:
            if e.args[0] in (1061, 1831):  # duplicate key / duplicate index
                print(f"  Indice ja existe: {name}")
            else:
                print(f"  Aviso ao criar indice {name}: {e}")

    conn.commit()
    conn.close()
    print("Migration concluida.")


def down():
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()
    cur.execute("DROP VIEW IF EXISTS view_dashboard_macros_agg")
    for name, table, _ in _INDEXES:
        try:
            cur.execute(f"DROP INDEX {name} ON {table}")
        except Exception:
            pass
    conn.commit()
    conn.close()
    print("View e indices removidos.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--down", action="store_true")
    args = p.parse_args()
    if args.down:
        down()
    else:
        up()
