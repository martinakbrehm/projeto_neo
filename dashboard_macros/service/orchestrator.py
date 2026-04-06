import pandas as pd
from ..data import loader

# ID da resposta que representa falha/erro na macro
RESPOSTA_ID_ERRO = 11

# Statuses que indicam cliente ativo (contrato consolidado)
STATUS_ATIVO = {"consolidado"}
# Statuses que indicam cliente inativo/excluído/aguardando
STATUS_INATIVO = {"excluir", "reprocessar"}


def build_dashboard_data(resumo_sel, filtro_empresa,
                         tipo_macro: str = "macro"):
    """Carrega dados do banco, aplica filtros e retorna (data_resumo, data_mensagens).

    - resumo_sel    : list de strings de data (YYYY-MM-DD) ou vazio
    - filtro_empresa: list ou valor único ou vazio
    - tipo_macro    : 'macro' (tabela_macros) ou 'api' (tabela_macro_api)
    """
    df = loader.carregar_dados(tipo_macro)

    if df is None or df.empty:
        return [], []

    dff = df.copy()

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
    # ---------------------------------------------------------------
    data_mensagens = []
    if "mensagem" in dff.columns:
        cnt = (
            dff.loc[~dff["resposta_id"].eq(RESPOSTA_ID_ERRO), "mensagem"]
            .fillna("(sem resposta)")
            .astype(str)
            .str.strip()
            .value_counts()
            .reset_index()
        )
        cnt.columns = ["mensagem", "quantidade"]
        data_mensagens = cnt.to_dict("records")

    # ---------------------------------------------------------------
    # Masks de status
    # ---------------------------------------------------------------
    mask_ativo   = dff["status"].isin(STATUS_ATIVO)
    mask_inativo = dff["status"].isin(STATUS_INATIVO)

    # ---------------------------------------------------------------
    # Tabela Resumo diário
    # ---------------------------------------------------------------
    data_resumo = []
    if "dia" in dff.columns:
        g = dff.groupby(dff["dia"].astype(str))
        total_s   = g.size()
        ativo_s   = mask_ativo.groupby(dff["dia"].astype(str)).sum()
        inativo_s = mask_inativo.groupby(dff["dia"].astype(str)).sum()

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

    return data_resumo, data_mensagens
