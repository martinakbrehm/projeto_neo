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
                         filtro_arquivo=None,
                         granularidade: str = "combo"):
    """Carrega dados do banco, aplica filtros e retorna (data_resumo, data_mensagens, data_origens).

    - resumo_sel       : list de strings de data (YYYY-MM-DD) ou vazio
    - filtro_empresa   : list ou valor único ou vazio
    - tipo_macro       : 'macro' (tabela_macros) ou 'api' (tabela_macro_api)
    - filtro_fornecedor: 'fornecedor2' | 'contatus' | None (todos)
    - filtro_arquivo   : list de strings de arquivo_origem ou vazio
    """
    df = loader.carregar_dados(tipo_macro)

    if df is None or df.empty:
        return [], [], build_tabela_arquivos(granularidade)

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
            # Expandir seleções de mês (ex: "mes:2026-04") para dias individuais
            dias_expandidos = []
            for sel in (resumo_sel if isinstance(resumo_sel, list) else [resumo_sel]):
                s = str(sel)
                if s.startswith("mes:"):
                    prefixo = s[4:]  # "2026-04"
                    dias_do_mes = [str(d) for d in dff["dia"].dropna().unique() if str(d).startswith(prefixo)]
                    dias_expandidos.extend(dias_do_mes)
                else:
                    dias_expandidos.append(s)
            if dias_expandidos:
                dff = dff[dff["dia"].astype(str).isin(dias_expandidos)]
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
        return [], [], build_tabela_arquivos(granularidade)

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

    return data_resumo, data_mensagens, build_tabela_arquivos(granularidade)


def build_tabela_arquivos(granularidade: str = "combo") -> list:
    """Retorna lista de dicts com estatísticas por arquivo (CPF+UC combo)."""
    df = loader.carregar_stats_por_arquivo()
    if df is None or df.empty:
        return []

    int_cols = [
        "cpfs_no_arquivo", "cpfs_processados", "ativos", "inativos",
        "cpfs_ineditos", "ucs_ineditas",
        "combos_processadas", "combos_ativas", "combos_excluidas", "combos_reprocessar",
        "ineditos_processados", "ineditos_ativos", "ineditos_inativos",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
        else:
            df[col] = 0

    # --- Combo-level (para visão geral) ---
    df["combos_pendentes"] = (df["ucs_ineditas"] - df["combos_processadas"]).clip(lower=0)
    df["pct_combos_ativas"] = df.apply(
        lambda r: f"{round(r['combos_ativas'] / r['combos_processadas'] * 100, 1)}%"
        if r["combos_processadas"] > 0 else "-", axis=1,
    )
    df["pct_combos_excluidas"] = df.apply(
        lambda r: f"{round(r['combos_excluidas'] / r['combos_processadas'] * 100, 1)}%"
        if r["combos_processadas"] > 0 else "-", axis=1,
    )
    df["pct_combos_reprocessar"] = df.apply(
        lambda r: f"{round(r['combos_reprocessar'] / r['combos_processadas'] * 100, 1)}%"
        if r["combos_processadas"] > 0 else "-", axis=1,
    )

    df["data_carga"] = df["data_carga"].astype(str)

    return df[[
        "arquivo", "data_carga",
        "cpfs_no_arquivo", "ucs_ineditas",
        "combos_processadas", "combos_pendentes",
        "combos_ativas", "pct_combos_ativas",
        "combos_excluidas", "pct_combos_excluidas",
        "combos_reprocessar", "pct_combos_reprocessar",
    ]].to_dict("records")


def build_tabela_cobertura() -> list:
    """Retorna cobertura por arquivo contando combinações únicas de CPF+UC.

    Regra: a unidade de contagem é o par (CPF, UC). Um mesmo CPF com UCs
    diferentes gera combinações distintas. Linhas sem UC são excluídas.
    Combinação 'nova' = aparece pela 1ª vez considerando todos os arquivos;
    combinação 'existente' = já estava presente em arquivo anterior.
    """
    df = loader.carregar_cobertura()
    if df is None or df.empty:
        return []

    for col in ["total_combos", "combos_novas", "combos_existentes"]:
        df[col] = df[col].fillna(0).astype(int)

    df["pct_novas"] = df.apply(
        lambda r: f"{round(r['combos_novas'] / r['total_combos'] * 100, 1)}%"
        if r["total_combos"] > 0 else "-", axis=1)
    df["pct_existentes"] = df.apply(
        lambda r: f"{round(r['combos_existentes'] / r['total_combos'] * 100, 1)}%"
        if r["total_combos"] > 0 else "-", axis=1)

    df["data_carga"] = df["data_carga"].astype(str)

    return df[[
        "arquivo", "data_carga",
        "total_combos", "combos_novas", "pct_novas",
        "combos_existentes", "pct_existentes",
    ]].to_dict("records")
