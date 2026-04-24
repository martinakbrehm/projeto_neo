import sys
import time
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino()

_CACHE: dict = {}        # cache por tipo {'macro': df, 'api': df} — sem TTL, vive durante o processo
_CACHE_STATS: dict = {}  # cache para stats_por_arquivo / cobertura
_CACHE_STATS_TTL = 300   # segundos (5 min)

# ---------------------------------------------------------------------------
# Tabela materializada — populada pela stored procedure
# sp_refresh_dashboard_macros_agg() chamada ao final do ETL.
# SELECT simples em tabela indexada: latência <1ms.
# ---------------------------------------------------------------------------
SQLs = {
    "macro": "SELECT dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd FROM dashboard_macros_agg ORDER BY dia DESC",

    "api": """
        SELECT
            DATE(m.data_update)                          AS dia,
            m.status,
            r.mensagem,
            r.status                                     AS resposta_status,
            d.nome                                       AS empresa,
            COALESCE(co.fornecedor, 'fornecedor2')       AS fornecedor,
            NULL                                         AS arquivo_origem,
            COUNT(*)                                     AS qtd
        FROM tabela_macro_api m
        LEFT JOIN respostas      r  ON r.id  = m.resposta_id
        LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
        LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
        WHERE m.status != 'pendente'
          AND m.resposta_id IS NOT NULL
        GROUP BY
            DATE(m.data_update),
            m.status,
            r.mensagem,
            r.status,
            d.nome,
            COALESCE(co.fornecedor, 'fornecedor2')
    """,
}



def carregar_dados(tipo: str = "macro") -> pd.DataFrame:
    """Carrega dados do banco de dados.

    tipo: 'macro' | 'macro_pipeline' | 'api'
    Resultado é cacheado em memória por tipo (sem TTL — vive durante o processo).
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
        # Não cachear DataFrames vazios — podem ser resultado de race condition
        # com o refresh (TRUNCATE + INSERT) da stored procedure
        if not df.empty:
            _CACHE[tipo] = df
        return df.copy()
    except Exception as e:
        print(f"[ERRO] Falha ao carregar dados ({tipo}): {e}")
        return pd.DataFrame()


def invalidar_cache(tipo: str = None):
    """Remove o cache para forçar recarga na próxima chamada.
    Se 'stats' for passado, invalida apenas o cache de stats_por_arquivo."""
    if tipo == "stats":
        _CACHE_STATS.clear()
    elif tipo:
        _CACHE.pop(tipo, None)
    else:
        _CACHE.clear()
        _CACHE_STATS.clear()


def refresh_dashboard_macros_agg() -> bool:
    """Executa a stored procedure que re-popula a tabela materializada.

    Deve ser chamada ao final de cada ciclo do ETL (passo 04).
    Invalida o cache de 'macro' automaticamente após o refresh.
    Retorna True se bem-sucedido, False caso contrário.
    """
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("CALL sp_refresh_dashboard_macros_agg()")
            conn.commit()
        conn.close()
        invalidar_cache("macro")
        print("[INFO] dashboard_macros_agg atualizada com sucesso.")
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao executar sp_refresh_dashboard_macros_agg: {e}")
        return False


# ---------------------------------------------------------------------------
# Tabela materializada de arquivos — populada por sp_refresh_dashboard_arquivos_agg
# SELECT simples em tabela física indexada: latência <1ms.
# A query complexa (ROW_NUMBER + staging_import_rows) roda apenas na stored
# procedure, nunca diretamente no dashboard.
# ---------------------------------------------------------------------------
_SQL_STATS_ARQUIVO_MAT = """
    SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
           cpfs_ineditos, ucs_ineditas, ineditos_processados, ineditos_ativos, ineditos_inativos
    FROM dashboard_arquivos_agg
    ORDER BY data_carga DESC
    LIMIT 15
"""


def carregar_stats_por_arquivo() -> pd.DataFrame:
    """Retorna estatísticas dos últimos 15 arquivos de staging.
    Lê da tabela materializada dashboard_arquivos_agg (SELECT simples).
    Cacheado em memória por _CACHE_STATS_TTL segundos.
    """
    cached = _CACHE_STATS.get("stats")
    if cached is not None:
        df_cached, ts = cached
        if time.time() - ts < _CACHE_STATS_TTL:
            return df_cached.copy()

    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(_SQL_STATS_ARQUIVO_MAT)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        _CACHE_STATS["stats"] = (df, time.time())
        return df.copy()
    except Exception as e:
        print(f"[ERRO] carregar_stats_por_arquivo: {e}")
        return pd.DataFrame()


_SQL_COBERTURA = """
    SELECT arquivo, data_carga,
           total_combos, combos_novas, combos_existentes
    FROM dashboard_cobertura_agg
    ORDER BY data_carga DESC, arquivo
"""


def carregar_cobertura() -> pd.DataFrame:
    """Retorna tabela de novos vs existentes por arquivo de staging."""
    cached = _CACHE_STATS.get("cobertura")
    if cached is not None:
        df_cached, ts = cached
        if time.time() - ts < _CACHE_STATS_TTL:
            return df_cached.copy()
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(_SQL_COBERTURA)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        _CACHE_STATS["cobertura"] = (df, time.time())
        return df.copy()
    except Exception as e:
        print(f"[ERRO] carregar_cobertura: {e}")
        return pd.DataFrame()


def refresh_dashboard_arquivos_agg() -> bool:
    """Recalcula dashboard_arquivos_agg diretamente (sem stored procedure).

    Usa a lógica centralizada do refresh_scheduler para segurança.
    """
    try:
        from dashboard_macros.refresh_scheduler import (
            limpar_queries_orfas,
            refresh_arquivos,
        )
        limpar_queries_orfas()
        ok = refresh_arquivos()
        if ok:
            invalidar_cache("stats")
        return ok
    except Exception as e:
        print(f"[ERRO] Falha ao atualizar dashboard_arquivos_agg: {e}")
        return False


