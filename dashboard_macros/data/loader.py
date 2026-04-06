import sys
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino()

_CACHE: dict = {}  # cache por tipo {'macro': df, 'api': df}


SQLs = {
    # Dashboard analítico: todos os registros de tabela_macros com labels de respostas e distribuidoras.
    # Usamos view_macros_automacao para o subconjunto do pipeline ativo (pendente/reprocessar) e
    # fazemos UNION com view_macros_finalizados (consolidado) para cobrir todo o histórico importado.
    # JOINs extras trazem empresa (distribuidoras.nome) e mensagem (respostas.mensagem).
    "macro": """
        SELECT
            m.id,
            DATE(m.data_update)  AS dia,
            m.data_update,
            m.status,
            m.resposta_id,
            r.mensagem,
            r.status           AS resposta_status,
            d.nome             AS empresa
        FROM tabela_macros m
        LEFT JOIN respostas     r ON r.id = m.resposta_id
        LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
    """,
    # TODO [improvements 20260406]: após aplicar db/improvements/20260406_cliente_origem_views_fornecedor/migration.py
    # adicionar as seguintes chaves para filtro por fornecedor no dashboard:
    #
    # "fornecedor2_macro":  SELECT ... FROM view_fornecedor2_macro  + JOINs respostas/distribuidoras
    # "fornecedor2_api":    SELECT ... FROM view_fornecedor2_api    + JOINs
    # "contatus_macro":     SELECT ... FROM view_contatus_macro     + JOINs
    # "contatus_api":       SELECT ... FROM view_contatus_api       + JOINs
    #
    # Ver README.md da migração para o SQL completo de cada query.

    # Pipeline ativo apenas (deduplicado por CPF+UC via view)
    "macro_pipeline": """
        SELECT
            m.id,
            DATE(m.data_update)  AS dia,
            m.data_update,
            m.status,
            m.resposta_id,
            r.mensagem,
            r.status           AS resposta_status,
            d.nome             AS empresa
        FROM view_macros_automacao m
        LEFT JOIN respostas     r ON r.id = m.resposta_id
        LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
    """,
    "api": """
        SELECT
            m.id,
            DATE(m.data_update)  AS dia,
            m.data_update,
            m.status,
            m.resposta_id,
            r.mensagem,
            r.status           AS resposta_status,
            d.nome             AS empresa
        FROM tabela_macro_api m
        LEFT JOIN respostas    r ON r.id = m.resposta_id
        LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
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
