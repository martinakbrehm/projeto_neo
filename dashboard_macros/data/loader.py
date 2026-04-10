import sys
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino()

_CACHE: dict = {}  # cache por tipo {'macro': df, 'api': df}


# CTE que pega o filename do staging mais recente por CPF
_CTE_ARQUIVO = """"""

# Expressão de arquivo_origem usada nos SELECTs
_COL_ARQUIVO = """
            CASE
                WHEN m.data_extracao IS NULL THEN 'Dados históricos'
                ELSE COALESCE(
                    (SELECT si.filename
                     FROM staging_import_rows sir
                     JOIN staging_imports si ON si.id = sir.staging_id
                     WHERE sir.normalized_cpf = cl.cpf
                       AND sir.validation_status = 'valid'
                     ORDER BY sir.id DESC
                     LIMIT 1),
                    'Dados históricos'
                )
            END AS arquivo_origem
"""

# Joins adicionais para resolver o filename
_JOIN_ARQUIVO = """
        LEFT JOIN clientes          cl ON cl.id  = m.cliente_id
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


# SQL para estatísticas por arquivo de staging (não usa cache — sempre atualizado)
# Conta combinações CPF+UC distintas do arquivo e quantas já têm registro
# em tabela_macros via cliente_uc_id (vínculo exato) ou fallback por CPF+distribuidora.
# IMPORTANTE: com o modelo de INSERT por ciclo, uma mesma UC pode ter vários registros
# não-pendentes. Usamos ROW_NUMBER() para considerar apenas o ÚLTIMO registro
# rodado por UC (maior id), evitando dupla contagem de ativos/inativos.
_SQL_STATS_ARQUIVO = """
    WITH latest_macros AS (
        SELECT id, cliente_uc_id, cliente_id, distribuidora_id, status
        FROM (
            SELECT
                id, cliente_uc_id, cliente_id, distribuidora_id, status,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CASE
                            WHEN cliente_uc_id IS NOT NULL
                                THEN CONCAT('uc_', cliente_uc_id)
                            ELSE CONCAT('cli_', cliente_id, '_', distribuidora_id)
                        END
                    ORDER BY id DESC
                ) AS rn
            FROM tabela_macros
            WHERE status != 'pendente'
              AND resposta_id IS NOT NULL
        ) ranked
        WHERE rn = 1
    )
    SELECT
        si.filename                                                          AS arquivo,
        DATE(si.created_at)                                                  AS data_carga,
        COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))) AS cpfs_no_arquivo,
        COUNT(DISTINCT CASE
            WHEN m.id IS NOT NULL
            THEN CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))
        END)                                                                 AS cpfs_processados,
        COUNT(DISTINCT CASE
            WHEN m.status = 'consolidado'
            THEN CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))
        END)                                                                 AS ativos,
        COUNT(DISTINCT CASE
            WHEN m.status IN ('excluido','reprocessar')
            THEN CONCAT(sir.normalized_cpf, '|', COALESCE(sir.normalized_uc, ''))
        END)                                                                 AS inativos
    FROM staging_imports si
    JOIN staging_import_rows sir
        ON sir.staging_id = si.id
       AND sir.validation_status = 'valid'
    LEFT JOIN clientes cl
        ON cl.cpf = sir.normalized_cpf
    -- Caso 1: normalized_uc preenchida → resolve via cliente_uc (vínculo exato)
    LEFT JOIN cliente_uc cu
        ON cu.cliente_id      = cl.id
       AND cu.uc              = sir.normalized_uc
       AND cu.distribuidora_id = CAST(si.distribuidora_nome AS UNSIGNED)
    -- Junta apenas o último registro rodado por UC (via CTE latest_macros)
    LEFT JOIN latest_macros m
        ON (
              (cu.id IS NOT NULL AND m.cliente_uc_id = cu.id)
           OR (cu.id IS NULL     AND m.cliente_id = cl.id
                                  AND m.distribuidora_id = CAST(si.distribuidora_nome AS UNSIGNED))
           )
    GROUP BY si.id, si.filename, DATE(si.created_at)
    ORDER BY si.id DESC
    LIMIT 15
"""


def carregar_stats_por_arquivo() -> pd.DataFrame:
    """Retorna estatísticas dos últimos 15 arquivos de staging por CPF+distribuidora."""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(_SQL_STATS_ARQUIVO)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"[ERRO] carregar_stats_por_arquivo: {e}")
        return pd.DataFrame()


