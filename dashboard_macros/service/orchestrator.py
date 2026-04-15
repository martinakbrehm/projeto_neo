import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
try:
    from ..data import loader
except ImportError:
    from data import loader

# ID da resposta que representa falha/erro na macro
RESPOSTA_ID_ERRO = 11

# Statuses que indicam cliente ativo (contrato consolidado)
STATUS_ATIVO = {"consolidado"}
# Statuses que indicam cliente inativo/excluído/aguardando
STATUS_INATIVO = {"excluido", "reprocessar"}


def build_dashboard_data(resumo_sel, filtro_empresa,
                         tipo_macro: str = "macro",
                         filtro_fornecedor: str = None,
                         filtro_arquivo=None):
    """Carrega dados do banco, aplica filtros e retorna (data_resumo, data_mensagens, data_origens).

    - resumo_sel       : list de strings de data (YYYY-MM-DD) ou vazio
    - filtro_empresa   : list ou valor único ou vazio
    - tipo_macro       : 'macro' (tabela_macros) ou 'api' (tabela_macro_api)
    - filtro_fornecedor: 'fornecedor2' | 'contatus' | None (todos)
    - filtro_arquivo   : list de strings de arquivo_origem ou vazio
    """
    df = loader.carregar_dados(tipo_macro)

    if df is None or df.empty:
        return [], []

    dff = df.copy()

    # --- filtro de fornecedor ---
    if filtro_fornecedor and "fornecedor" in dff.columns:
        dff = dff[dff["fornecedor"] == filtro_fornecedor]

    # --- filtro de arquivo ---
    if filtro_arquivo and "arquivo_origem" in dff.columns:
        if isinstance(filtro_arquivo, list):
            dff = dff[dff["arquivo_origem"].isin(filtro_arquivo)]
        else:
            dff = dff[dff["arquivo_origem"] == filtro_arquivo]

    # --- filtro de dias ---
    if resumo_sel:
        try:
            dff = dff[dff["dia"].astype(str).isin(resumo_sel)]
        except Exception:
            pass

    # --- filtro de empresa ---
    if filtro_empresa:
        try:
            if isinstance(filtro_empresa, list):
                dff = dff[dff["empresa"].astype(str).isin([str(x) for x in filtro_empresa])]
            else:
                dff = dff[dff["empresa"].astype(str) == str(filtro_empresa)]
        except Exception:
            pass

    if dff.empty:
        return [], []

    # ---------------------------------------------------------------
    # Distribuição de mensagens
    # Usa coluna 'qtd' (pré-agregada no SQL) para somar contagens.
    # ---------------------------------------------------------------
    data_mensagens = []
    if "mensagem" in dff.columns:
        mask_msg = (
            dff["resposta_status"].notna() &
            ~dff["resposta_status"].isin(["pendente"]) &
            dff["mensagem"].notna()
        )
        cnt = (
            dff.loc[mask_msg, ["mensagem", "qtd"]]
            .assign(mensagem=lambda df: df["mensagem"].astype(str).str.strip())
            .groupby("mensagem")["qtd"]
            .sum()
            .reset_index()
            .rename(columns={"qtd": "quantidade"})
            .sort_values("quantidade", ascending=False)
        )
        data_mensagens = cnt.to_dict("records")

    # ---------------------------------------------------------------
    # Masks de status
    # ---------------------------------------------------------------
    mask_ativo   = dff["status"].isin(STATUS_ATIVO)
    mask_inativo = dff["status"].isin(STATUS_INATIVO)

    # ---------------------------------------------------------------
    # Tabela Resumo diário
    # Usa 'qtd' para somar — cada linha do df é um grupo pré-agregado.
    # ---------------------------------------------------------------
    data_resumo = []
    if "dia" in dff.columns:
        dia_str = dff["dia"].astype(str)
        total_s   = dff.groupby(dia_str)["qtd"].sum()
        ativo_s   = dff[mask_ativo].groupby(dff[mask_ativo]["dia"].astype(str))["qtd"].sum()
        inativo_s = dff[mask_inativo].groupby(dff[mask_inativo]["dia"].astype(str))["qtd"].sum()

        resumo = pd.DataFrame({
            "dia":      total_s.index,
            "total":    total_s.values,
            "ativos":   ativo_s.reindex(total_s.index, fill_value=0).values,
            "inativos": inativo_s.reindex(total_s.index, fill_value=0).values,
        }).sort_values("dia")

        resumo["pct_ativos"]   = (resumo["ativos"]   / resumo["total"] * 100).round(1).astype(str) + "%"
        resumo["pct_inativos"] = (resumo["inativos"] / resumo["total"] * 100).round(1).astype(str) + "%"

        if len(resumo) > 1:
            total_sum   = int(resumo["total"].sum())
            ativos_sum  = int(resumo["ativos"].sum())
            inativos_sum = int(resumo["inativos"].sum())
            soma = {
                "dia":          "Total",
                "total":        total_sum,
                "ativos":       ativos_sum,
                "pct_ativos":   f"{round(ativos_sum / total_sum * 100, 1)}%" if total_sum else "0%",
                "inativos":     inativos_sum,
                "pct_inativos": f"{round(inativos_sum / total_sum * 100, 1)}%" if total_sum else "0%",
            }
            resumo = pd.concat([resumo, pd.DataFrame([soma])], ignore_index=True)

        # converte Int64/numpy types para int nativo (JSON-serializable)
        for col in ["total", "ativos", "inativos"]:
            resumo[col] = resumo[col].astype(int)

        data_resumo = resumo.to_dict("records")

    return data_resumo, data_mensagens, build_tabela_arquivos()


def build_tabela_arquivos() -> list:
    """Retorna lista de dicts com estatísticas dos últimos 15 arquivos de staging.

    Retorna todos os campos (gerais + inéditos). O dashboard alterna entre os
    dois conjuntos de colunas via seletor.
    """
    df = loader.carregar_stats_por_arquivo()
    if df is None or df.empty:
        return []

    int_cols = [
        "cpfs_no_arquivo", "cpfs_processados", "ativos", "inativos",
        "cpfs_ineditos", "ucs_ineditas",
        "ineditos_processados", "ineditos_ativos", "ineditos_inativos",
    ]
    for col in int_cols:
        df[col] = df[col].fillna(0).astype(int)

    # Percentuais gerais
    df["total_proc"] = df["ativos"] + df["inativos"]
    df["pct_ativos"] = df.apply(
        lambda r: f"{round(r['ativos'] / r['total_proc'] * 100, 1)}%" if r["total_proc"] > 0 else "-",
        axis=1,
    )
    df["pct_inativos"] = df.apply(
        lambda r: f"{round(r['inativos'] / r['total_proc'] * 100, 1)}%" if r["total_proc"] > 0 else "-",
        axis=1,
    )
    df["cpfs_pendentes"] = df["cpfs_no_arquivo"] - df["cpfs_processados"]

    # Percentuais inéditos
    df["ined_total_proc"] = df["ineditos_ativos"] + df["ineditos_inativos"]
    df["pct_ineditos_ativos"] = df.apply(
        lambda r: f"{round(r['ineditos_ativos'] / r['ined_total_proc'] * 100, 1)}%" if r["ined_total_proc"] > 0 else "-",
        axis=1,
    )
    df["pct_ineditos_inativos"] = df.apply(
        lambda r: f"{round(r['ineditos_inativos'] / r['ined_total_proc'] * 100, 1)}%" if r["ined_total_proc"] > 0 else "-",
        axis=1,
    )
    df["ineditos_pendentes"] = df["cpfs_ineditos"] - df["ineditos_processados"]

    df["data_carga"] = df["data_carga"].astype(str)

    return df[[
        "arquivo", "data_carga",
        # Geral
        "cpfs_no_arquivo", "cpfs_processados", "cpfs_pendentes",
        "ativos", "pct_ativos", "inativos", "pct_inativos",
        # Inéditos
        "cpfs_ineditos", "ucs_ineditas",
        "ineditos_processados", "ineditos_pendentes",
        "ineditos_ativos", "pct_ineditos_ativos",
        "ineditos_inativos", "pct_ineditos_inativos",
    ]].to_dict("records")
