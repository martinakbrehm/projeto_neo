import sys
import os
import threading
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))

import dash
from dash import dcc, html, dash_table
from flask import jsonify, request
import dash_auth

try:
    from .data import loader
    from .service import orchestrator
    from .refresh_scheduler import executar_refresh
except ImportError:
    from data import loader
    from service import orchestrator
    from refresh_scheduler import executar_refresh

REFRESH_INTERVAL_MS = 20 * 60 * 1000  # 20 minutos em milissegundos

COLUMN_LABELS = {
    "dia":        "Data",
    "total":      "Total",
    "ativos":     "Ativos",
    "pct_ativos": "% Ativos",
    "inativos":   "Inativos",
    "pct_inativos": "% Inativos",
}

external_stylesheets = [
    "https://fonts.googleapis.com/css?family=Roboto:400,700&display=swap",
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css",
]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Dashboard de aproveitamento das macros"

# Autenticação básica para todo o app
auth = dash_auth.BasicAuth(
    app,
    {'neo': 'dashboard2026'}
)

TITLE_STYLE         = {"fontFamily": "Roboto", "color": "#1a237e", "fontWeight": "700", "fontSize": "22px"}
SECTION_TITLE_STYLE = {"fontFamily": "Roboto", "color": "#3949ab", "fontWeight": "700", "fontSize": "18px"}
SUBTITLE_STYLE      = {"fontFamily": "Roboto", "color": "#283593", "fontWeight": "700", "fontSize": "16px"}

_df_inicial             = pd.DataFrame()
_opcoes_dia_inicial     = []
_opcoes_empresa_inicial = []
_opcoes_arquivo_inicial = []

# Refresh inicial das tabelas materializadas (em background para não bloquear startup)
def _refresh_bg():
    try:
        # Pré-carregar dados existentes no cache ANTES do refresh
        # para que o dashboard mostre dados mesmo durante o TRUNCATE+INSERT das SPs
        loader.carregar_dados("macro")
        loader.carregar_stats_por_arquivo()
        loader.carregar_cobertura()
        executar_refresh()
    except Exception as e:
        print(f"[WARN] Refresh inicial falhou: {e}")
    finally:
        # Invalida TODO o cache para que o próximo callback busque dados frescos
        loader.invalidar_cache()
        print("[INFO] Cache invalidado após refresh")

threading.Thread(target=_refresh_bg, daemon=True).start()


@app.server.before_request
def _log_incoming_requests():
    try:
        _ = request.path
    except Exception:
        pass


app.layout = html.Div([

    # Cabecalho
    html.Div([
        html.Img(src="https://img.icons8.com/color/48/000000/combo-chart--v2.png",
                 style={"height": "48px", "marginRight": "16px"}),
        html.H1("Dashboard de aproveitamento das macros",
                style={**TITLE_STYLE, "display": "inline-block", "verticalAlign": "middle", "margin": 0}),
    ], style={"display": "flex", "alignItems": "center", "marginBottom": "16px", "marginTop": "16px"}),

    # Tipo fixo = macro (hidden store para compatibilidade)
    dcc.Store(id="selector-tipo-macro", data="macro"),

    # Seletor Fornecedor
    html.Div([
        html.Label("Fornecedor:", style={"fontWeight": "700", "fontSize": "14px",
                                         "marginRight": "12px", "color": "#1a237e"}),
        dcc.RadioItems(
            id="selector-fornecedor",
            options=[
                {"label": "  Todos",       "value": "todos"},
                {"label": "  Fornecedor2", "value": "fornecedor2"},
                {"label": "  Contatus",    "value": "contatus"},
            ],
            value="todos",
            inline=True,
            inputStyle={"marginRight": "6px", "cursor": "pointer"},
            labelStyle={"marginRight": "24px", "fontFamily": "Roboto", "fontSize": "15px",
                        "fontWeight": "600", "cursor": "pointer"},
        ),
    ], style={"background": "#ede7f6", "padding": "10px 16px", "borderRadius": "8px",
              "marginBottom": "12px", "display": "flex", "alignItems": "center",
              "boxShadow": "0 1px 4px rgba(74,20,140,0.10)"}),

    # Info bar
    html.Div(id="info-registros", style={"marginBottom": "12px", "fontSize": "14px", "fontWeight": "600"}),

    # Filtros
    html.Div([
        html.Div([
            html.Label("Filtrar m\u00eas", style={"fontWeight": "700", "fontSize": "13px",
                                              "marginBottom": "6px", "display": "block", "color": "#1a237e"}),
            dcc.Dropdown(
                id="filtro-mes-dropdown",
                options=[],
                multi=True, clearable=True, placeholder="Todos os meses",
                style={"width": "100%"},
            ),
        ], style={"flex": "0.7", "minWidth": "180px", "background": "#fff", "padding": "10px",
                  "borderRadius": "8px", "boxShadow": "0 1px 6px rgba(44,62,80,0.06)"}),

        html.Div([
            html.Label("Filtrar dia", style={"fontWeight": "700", "fontSize": "13px",
                                              "marginBottom": "6px", "display": "block", "color": "#1a237e"}),
            dcc.Dropdown(
                id="resumo-dia-dropdown",
                options=[],
                multi=True, clearable=True, placeholder="Todos os dias",
                style={"width": "100%"},
            ),
        ], style={"flex": "0.7", "minWidth": "180px", "background": "#fff", "padding": "10px",
                  "borderRadius": "8px", "boxShadow": "0 1px 6px rgba(44,62,80,0.06)"}),

        html.Div([
            html.Label("Filtrar empresa", style={"fontWeight": "700", "fontSize": "13px",
                                                 "marginBottom": "6px", "display": "block", "color": "#1a237e"}),
            dcc.Dropdown(
                id="filtro-empresa-dropdown",
                options=[{"label": str(e), "value": str(e)} for e in _opcoes_empresa_inicial],
                multi=True, clearable=True, placeholder="Todas as empresas",
                style={"width": "100%"},
            ),
        ], style={"flex": "1", "minWidth": "260px", "background": "#fff", "padding": "10px",
                  "borderRadius": "8px", "boxShadow": "0 1px 6px rgba(44,62,80,0.06)"}),

        html.Div([
            html.Label("Filtrar arquivo", style={"fontWeight": "700", "fontSize": "13px",
                                                  "marginBottom": "6px", "display": "block", "color": "#1a237e"}),
            dcc.Dropdown(
                id="filtro-arquivo-dropdown",
                options=[{"label": str(a), "value": str(a)} for a in _opcoes_arquivo_inicial],
                multi=True, clearable=True, placeholder="Todos os arquivos",
                style={"width": "100%"},
            ),
        ], style={"flex": "1", "minWidth": "260px", "background": "#fff", "padding": "10px",
                  "borderRadius": "8px", "boxShadow": "0 1px 6px rgba(44,62,80,0.06)"}),

    ], style={"display": "flex", "gap": "12px", "alignItems": "stretch",
              "marginBottom": "12px", "marginTop": "8px"}),

    # Conteudo principal — 3 cards principais com loading conjunto
    dcc.Loading(type="circle", children=html.Div([

        # Card: Resumo diario
        html.Div([
            html.H2("Resumo por data de processamento",
                    style={**SECTION_TITLE_STYLE, "marginBottom": "8px"}),
            dash_table.DataTable(
                id="tabela-resumo",
                columns=[{"name": COLUMN_LABELS.get(c, c), "id": c}
                          for c in ["dia", "total", "ativos", "pct_ativos", "inativos", "pct_inativos"]],
                data=[],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "center", "fontFamily": "Roboto", "fontSize": "15px",
                            "padding": "10px", "whiteSpace": "normal", "height": "auto"},
                style_header={"backgroundColor": "#3949ab", "color": "white",
                               "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "15px"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#f3f4ff"},
                    {"if": {"filter_query": '{dia} = "Total"'}, "fontWeight": "bold",
                     "backgroundColor": "#e8eaf6"},
                ],
                page_size=20,
            ),
            html.P("Dados carregados diretamente do banco de dados.",
                   style={"color": "#888", "fontSize": "13px", "marginTop": "8px"}),
        ], style={"background": "#fff", "borderRadius": "8px", "boxShadow": "0 2px 8px #e0e0e0",
                  "padding": "16px", "marginBottom": "18px"}),

        # Card: Distribuicao de respostas + grafico
        html.Div([
            html.Div([
                html.H3("Distribuicao de respostas",
                        style={**SUBTITLE_STYLE, "marginTop": "0", "marginBottom": "8px"}),
                dash_table.DataTable(
                    id="tabela-mensagens",
                    columns=[{"name": "Resposta", "id": "mensagem"},
                              {"name": "Quantidade", "id": "quantidade"}],
                    data=[],
                    style_table={"overflowX": "auto", "borderRadius": "8px",
                                 "boxShadow": "0 2px 8px #e0e0e0", "marginTop": "12px"},
                    style_cell={"textAlign": "left", "fontFamily": "Roboto", "fontSize": "14px",
                                "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                    style_header={"backgroundColor": "#3949ab", "color": "white",
                                   "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "15px"},
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": "#f3f4ff"},
                    ],
                    page_size=12,
                ),
            ], style={"background": "#fff", "borderRadius": "8px", "boxShadow": "0 2px 8px #e0e0e0",
                      "padding": "12px", "marginBottom": "22px"}),

        ], style={"width": "100%"}),

        # Card: Resultados por arquivo carregado — Visao Geral
        html.Div([
            html.H3("Resultados por arquivo carregado",
                    style={**SUBTITLE_STYLE, "marginTop": "0", "marginBottom": "6px"}),
            html.P(
                "Total = combinações CPF+UC inéditas no arquivo (primeira aparição). "
                "Processadas = combos que já rodaram na macro (consolidado + excluído + reprocessar). "
                "Pendentes = combos que nunca rodaram. "
                "Ativas = contrato confirmado. Excluídas = inativas. Reprocessar = vai rodar novamente.",
                style={"fontSize": "13px", "color": "#555", "marginBottom": "10px",
                       "background": "#e3f2fd", "padding": "8px 14px", "borderRadius": "6px",
                       "borderLeft": "4px solid #1565c0"},
            ),
            dash_table.DataTable(
                id="tabela-arquivos-geral",
                columns=[],
                data=[],
                style_table={"overflowX": "auto", "borderRadius": "8px",
                             "boxShadow": "0 2px 8px #e0e0e0", "marginTop": "4px"},
                style_cell={"textAlign": "center", "fontFamily": "Roboto", "fontSize": "13px",
                            "padding": "7px 5px", "whiteSpace": "normal", "height": "auto",
                            "minWidth": "55px", "maxWidth": "120px"},
                style_cell_conditional=[
                    {"if": {"column_id": "arquivo"}, "textAlign": "left", "minWidth": "160px", "maxWidth": "220px"},
                    {"if": {"column_id": "data_carga"}, "minWidth": "80px", "maxWidth": "90px"},
                    {"if": {"column_id": "pct_combos_ativas"}, "minWidth": "50px", "maxWidth": "65px"},
                    {"if": {"column_id": "pct_combos_excluidas"}, "minWidth": "50px", "maxWidth": "65px"},
                    {"if": {"column_id": "pct_combos_reprocessar"}, "minWidth": "50px", "maxWidth": "65px"},
                ],
                style_header={"backgroundColor": "#3949ab", "color": "white",
                               "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "11px",
                               "padding": "8px 5px", "whiteSpace": "normal"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#f3f4ff"},
                ],
                page_size=10,
            ),
        ], style={"background": "#fff", "borderRadius": "8px", "boxShadow": "0 2px 8px #e0e0e0",
                  "padding": "16px", "marginBottom": "18px"}),

        # Card: Cobertura — Novos vs Existentes
        html.Div([
            html.H3("Cobertura: combinações CPF+UC novas vs existentes",
                    style={**SUBTITLE_STYLE, "marginTop": "0", "marginBottom": "6px"}),
            html.P(
                "Combinação nova = par CPF+UC que aparece pela primeira vez considerando todos os arquivos. "
                "Combinação existente = par CPF+UC já presente em arquivo importado anteriormente.",
                style={"fontSize": "13px", "color": "#555", "marginBottom": "10px",
                       "background": "#e3f2fd", "padding": "8px 14px", "borderRadius": "6px",
                       "borderLeft": "4px solid #3949ab"},
            ),
            dash_table.DataTable(
                id="tabela-cobertura",
                columns=[],
                data=[],
                style_table={"overflowX": "auto", "borderRadius": "8px",
                             "boxShadow": "0 2px 8px #e0e0e0", "marginTop": "4px"},
                style_cell={"textAlign": "center", "fontFamily": "Roboto", "fontSize": "13px",
                            "padding": "7px 5px", "whiteSpace": "normal", "height": "auto"},
                style_cell_conditional=[
                    {"if": {"column_id": "arquivo"}, "textAlign": "left", "minWidth": "160px", "maxWidth": "220px"},
                ],
                style_header={"backgroundColor": "#3949ab", "color": "white",
                               "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "11px",
                               "padding": "8px 5px", "whiteSpace": "normal"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#f3f4ff"},
                ],
                page_size=10,
            ),
        ], style={"background": "#fff", "borderRadius": "8px", "boxShadow": "0 2px 8px #e0e0e0",
                  "padding": "16px", "marginBottom": "18px"}),

    ], style={"background": "#e8eaf6", "padding": "28px", "borderRadius": "10px", "marginBottom": "18px"})),

    html.Div(style={"height": "8px"}),

    # Auto-refresh a cada 20 minutos
    dcc.Interval(id="interval-refresh", interval=REFRESH_INTERVAL_MS, n_intervals=0),

], style={"maxWidth": "1100px", "margin": "0 auto", "fontFamily": "Roboto",
          "background": "#f0f2f8", "padding": "16px 0"})


# --------------------------------------------------------------------------
# Callbacks
# --------------------------------------------------------------------------

@app.callback(
    [
        dash.dependencies.Output("filtro-mes-dropdown",     "options"),
        dash.dependencies.Output("filtro-mes-dropdown",     "value"),
        dash.dependencies.Output("resumo-dia-dropdown",     "options"),
        dash.dependencies.Output("resumo-dia-dropdown",     "value"),
        dash.dependencies.Output("filtro-empresa-dropdown", "options"),
        dash.dependencies.Output("filtro-empresa-dropdown", "value"),
        dash.dependencies.Output("filtro-arquivo-dropdown", "options"),
        dash.dependencies.Output("filtro-arquivo-dropdown", "value"),
        dash.dependencies.Output("info-registros",          "children"),
    ],
    [
        dash.dependencies.Input("selector-tipo-macro",  "data"),
        dash.dependencies.Input("selector-fornecedor",  "value"),
        dash.dependencies.Input("interval-refresh",     "n_intervals"),
    ]
)
def atualizar_opcoes_filtros(tipo_macro, fornecedor, n_intervals):
    tipo = "macro"
    filtro_forn = fornecedor if fornecedor and fornecedor != "todos" else None
    # Se veio do interval, rodar refresh em background (cache será invalidado ao final do refresh)
    ctx = dash.callback_context
    if ctx.triggered and ctx.triggered[0]["prop_id"] == "interval-refresh.n_intervals" and n_intervals > 0:
        threading.Thread(target=_refresh_bg, daemon=True).start()
    df = loader.carregar_dados(tipo)
    if df.empty:
        loader.invalidar_cache(tipo)
        df = loader.carregar_dados(tipo)
    if df.empty:
        return [], None, [], None, [], None, [], None, f"Sem dados para {tipo.upper()} (aguardando refresh...)"
    dff = df[df["fornecedor"] == filtro_forn] if filtro_forn and "fornecedor" in df.columns else df
    opcoes_dia     = sorted(dff["dia"].dropna().unique())
    opcoes_empresa = sorted(dff["empresa"].dropna().unique()) if "empresa" in dff.columns else []
    opcoes_arquivo = sorted(dff["arquivo_origem"].dropna().unique()) if "arquivo_origem" in dff.columns else []

    # Gerar opções de mês-ano a partir dos dias disponíveis
    meses_vistos = {}
    for d in opcoes_dia:
        ds = str(d)
        if len(ds) >= 7:
            chave_mes = ds[:7]  # "2026-04"
            if chave_mes not in meses_vistos:
                try:
                    mes_num = int(chave_mes[5:7])
                    ano = chave_mes[:4]
                    meses_vistos[chave_mes] = f"{mes_num:02d}/{ano}"
                except ValueError:
                    pass
    opcoes_dropdown_mes = [{"label": meses_vistos[k], "value": k} for k in sorted(meses_vistos.keys())]

    label_forn = f" | Fornecedor: {filtro_forn}" if filtro_forn else ""
    info = (
        f"Registros: {len(dff):,}  |  "
        f"Dias: {len(opcoes_dia)}  |  "
        f"Empresas: {len(opcoes_empresa)}  |  "
        f"Arquivos: {len(opcoes_arquivo)}"
        f"{label_forn}"
    )
    return (
        opcoes_dropdown_mes,
        None,
        [{"label": str(d), "value": str(d)} for d in opcoes_dia],
        None,
        [{"label": str(e), "value": str(e)} for e in opcoes_empresa],
        None,
        [{"label": str(a), "value": str(a)} for a in opcoes_arquivo],
        None,
        info,
    )


@app.callback(
    [
        dash.dependencies.Output("tabela-resumo",         "data"),
        dash.dependencies.Output("tabela-mensagens",      "data"),
        dash.dependencies.Output("tabela-arquivos-geral", "data"),
        dash.dependencies.Output("tabela-arquivos-geral", "columns"),
        dash.dependencies.Output("tabela-cobertura",      "data"),
        dash.dependencies.Output("tabela-cobertura",      "columns"),
    ],
    [
        dash.dependencies.Input("filtro-mes-dropdown",        "value"),
        dash.dependencies.Input("resumo-dia-dropdown",        "value"),
        dash.dependencies.Input("filtro-empresa-dropdown",    "value"),
        dash.dependencies.Input("filtro-arquivo-dropdown",    "value"),
        dash.dependencies.Input("selector-tipo-macro",        "data"),
        dash.dependencies.Input("selector-fornecedor",        "value"),
    ]
)
def atualizar_dashboard(filtro_mes, resumo_sel, filtro_empresa, filtro_arquivo, tipo_macro, fornecedor):
    tipo = "macro"
    filtro_forn = fornecedor if fornecedor and fornecedor != "todos" else None
    # Combinar filtros de m\u00eas e dia numa lista \u00fanica para o orchestrator
    filtro_datas_combinado = []
    if filtro_mes:
        for m in (filtro_mes if isinstance(filtro_mes, list) else [filtro_mes]):
            filtro_datas_combinado.append(f"mes:{m}")
    if resumo_sel:
        for d in (resumo_sel if isinstance(resumo_sel, list) else [resumo_sel]):
            filtro_datas_combinado.append(str(d))
    try:
        data_resumo, data_mensagens, data_arquivos = orchestrator.build_dashboard_data(
            filtro_datas_combinado if filtro_datas_combinado else None,
            filtro_empresa, tipo_macro=tipo,
            filtro_fornecedor=filtro_forn, filtro_arquivo=filtro_arquivo,
        )
        data_cobertura = orchestrator.build_tabela_cobertura()
    except Exception as _e:
        import traceback
        print(f"[ERRO atualizar_dashboard] {_e}")
        traceback.print_exc()
        data_resumo = []
        data_mensagens = []
        data_arquivos = []
        data_cobertura = []

    # Colunas da visão geral por arquivo (combo-level inéditas)
    cols_geral = [
        {"name": "Arquivo",           "id": "arquivo"},
        {"name": "Data carga",        "id": "data_carga"},
        {"name": "CPFs no arquivo",   "id": "cpfs_no_arquivo"},
        {"name": "Combinações inéditas", "id": "ucs_ineditas"},
        {"name": "Processadas",       "id": "combos_processadas"},
        {"name": "Pendentes",         "id": "combos_pendentes"},
        {"name": "Ativas",            "id": "combos_ativas"},
        {"name": "% Ativas",          "id": "pct_combos_ativas"},
        {"name": "Excluídas",         "id": "combos_excluidas"},
        {"name": "% Excluídas",       "id": "pct_combos_excluidas"},
        {"name": "Reprocessar",       "id": "combos_reprocessar"},
        {"name": "% Reprocessar",     "id": "pct_combos_reprocessar"},
    ]

    # Colunas de cobertura
    cols_cobertura = [
        {"name": "Arquivo",        "id": "arquivo"},
        {"name": "Data carga",     "id": "data_carga"},
        {"name": "Total combos",   "id": "total_combos"},
        {"name": "Novas",          "id": "combos_novas"},
        {"name": "% Novas",        "id": "pct_novas"},
        {"name": "Existentes",     "id": "combos_existentes"},
        {"name": "% Existentes",   "id": "pct_existentes"},
    ]

    return (data_resumo, data_mensagens,
            data_arquivos, cols_geral,
            data_cobertura, cols_cobertura)





@app.server.route("/_debug/data")
def debug_data():
    try:
        print("DEBUG: Chamando build_dashboard_data")
        import traceback
        data_resumo, data_mensagens, data_arquivos = orchestrator.build_dashboard_data(
            [], None, "macro", None, None
        )
        print(f"DEBUG: Retornou {len(data_resumo)} registros no resumo")
        return jsonify({
            "data_resumo":    data_resumo,
            "data_mensagens": data_mensagens,
            "data_arquivos":  data_arquivos,
        })
    except Exception as e:
        print(f"DEBUG: Erro: {e}")
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False, use_reloader=False)
