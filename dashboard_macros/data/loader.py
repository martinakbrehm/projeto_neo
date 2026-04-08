import sys
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino()

_CACHE: dict = {}  # cache por tipo {'macro': df, 'api': df}


SQLs = {
    # Dashboard analítico: todos os registros de tabela_macros com labels de respostas,
    # distribuidoras e origem do cliente (fornecedor2 vs contatus, campanha/arquivo).
    "macro": """
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
            COALESCE(co.campanha,   'operacional')        AS campanha
        FROM tabela_macros m
        LEFT JOIN respostas      r  ON r.id  = m.resposta_id
        LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
        LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
        WHERE m.data_extracao IS NOT NULL
    """,

    # Pipeline ativo apenas (deduplicado por CPF+UC via view)
    "macro_pipeline": """
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
            COALESCE(co.campanha,   'operacional')        AS campanha
        FROM view_macros_automacao m
        LEFT JOIN respostas      r  ON r.id  = m.resposta_id
        LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
        LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
        WHERE m.data_extracao IS NOT NULL
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
            COALESCE(co.campanha,   'operacional')        AS campanha
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
        if not df.empty:
            df["dia"] = pd.to_datetime(df["dia"], errors="coerce").dt.date
            # Calcula coluna "arquivo_origem":
            # - sem data_extracao => dado veio de migracao historica
            # - com data_extracao => usa campanha como identificador do arquivo/lote
            sem_extracao = df["data_extracao"].isna() if "data_extracao" in df.columns else pd.Series(False, index=df.index)
            df["arquivo_origem"] = df["campanha"].where(~sem_extracao, other="Migracao historica")
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
