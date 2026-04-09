import sys
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino()

_CACHE: dict = {}  # cache por tipo {'macro': df, 'api': df}


# CTE que pega o filename do staging mais recente por CPF
_CTE_ARQUIVO = """
    WITH latest_arquivo AS (
        SELECT
            sir.normalized_cpf,
            si.filename,
            ROW_NUMBER() OVER (
                PARTITION BY sir.normalized_cpf
                ORDER BY sir.id DESC
            ) AS rn
        FROM staging_import_rows sir
        JOIN staging_imports si ON si.id = sir.staging_id
        WHERE sir.validation_status = 'valid'
    )
"""

# Expressão de arquivo_origem usada nos SELECTs
_COL_ARQUIVO = """
            COALESCE(la.filename, 'Dados hist\u00f3ricos') AS arquivo_origem
"""

# Joins adicionais para resolver o filename
_JOIN_ARQUIVO = """
        LEFT JOIN clientes          cl ON cl.id  = m.cliente_id
        LEFT JOIN latest_arquivo    la ON la.normalized_cpf = cl.cpf AND la.rn = 1
"""

SQLs = {
    # Dashboard analítico: todos os registros de tabela_macros com labels de respostas,
    # distribuidoras e origem do cliente (fornecedor2 vs contatus, arquivo de staging).
    "macro": _CTE_ARQUIVO + """
        SELECT
            m.id,
            DATE(COALESCE(m.data_extracao, m.data_update)) AS dia,
            m.data_update,
            m.data_extracao,
            m.status,
            m.resposta_id,
            r.mensagem,
            r.status                                     AS resposta_status,
            d.nome                                       AS empresa,
            COALESCE(co.fornecedor, 'fornecedor2')       AS fornecedor,
""" + _COL_ARQUIVO + """
        FROM tabela_macros m
        LEFT JOIN respostas      r  ON r.id  = m.resposta_id
        LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
        LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
""" + _JOIN_ARQUIVO + """
        WHERE m.status != 'pendente'
          AND m.resposta_id IS NOT NULL
    """,

    # Pipeline ativo apenas (deduplicado por CPF+UC via view)
    "macro_pipeline": _CTE_ARQUIVO + """
        SELECT
            m.id,
            DATE(COALESCE(m.data_extracao, m.data_update)) AS dia,
            m.data_update,
            m.data_extracao,
            m.status,
            m.resposta_id,
            r.mensagem,
            r.status                                     AS resposta_status,
            d.nome                                       AS empresa,
            COALESCE(co.fornecedor, 'fornecedor2')       AS fornecedor,
""" + _COL_ARQUIVO + """
        FROM view_macros_automacao m
        LEFT JOIN respostas      r  ON r.id  = m.resposta_id
        LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
        LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
""" + _JOIN_ARQUIVO + """
        WHERE m.status != 'pendente'
          AND m.resposta_id IS NOT NULL
    """,

    "api": """
        SELECT
            m.id,
            DATE(m.data_update)                          AS dia,
            m.data_update,
            m.data_extracao,
            m.status,
            m.resposta_id,
            r.mensagem,
            r.status                                     AS resposta_status,
            d.nome                                       AS empresa,
            COALESCE(co.fornecedor, 'fornecedor2')       AS fornecedor,
            NULL                                         AS arquivo_origem
        FROM tabela_macro_api m
        LEFT JOIN respostas      r  ON r.id  = m.resposta_id
        LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
        LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
    """,
}



def carregar_dados(tipo: str = "macro") -> pd.DataFrame:
    """Carrega dados do banco de dados.

    tipo: 'macro' | 'macro_pipeline' | 'api'
    Resultado é cacheado em memória por tipo.
    """
    tipo = tipo if tipo in SQLs else "macro"
    if tipo in _CACHE:
        return _CACHE[tipo].copy()

    query = SQLs[tipo]
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        _CACHE[tipo] = df
        return df.copy()
    except Exception as e:
        print(f"[ERRO] Falha ao carregar dados do banco ({tipo}): {e}")
        return pd.DataFrame()


def invalidar_cache(tipo: str = None):
    """Remove o cache para forçar recarga na próxima chamada."""
    if tipo:
        _CACHE.pop(tipo, None)
    else:
        _CACHE.clear()
