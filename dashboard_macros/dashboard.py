import sys
import dash
from dash import dcc, html, dash_table
from flask import jsonify, request

from .data import loader
from .service import orchestrator

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

TITLE_STYLE         = {"fontFamily": "Roboto", "color": "#2c3e50", "fontWeight": "700", "fontSize": "22px"}
SECTION_TITLE_STYLE = {"fontFamily": "Roboto", "color": "#2980b9", "fontWeight": "700", "fontSize": "18px"}
SUBTITLE_STYLE      = {"fontFamily": "Roboto", "color": "#2c3e50", "fontWeight": "700", "fontSize": "16px"}

_df_inicial             = loader.carregar_dados("macro")
_opcoes_dia_inicial     = sorted(_df_inicial["dia"].dropna().unique()) if not _df_inicial.empty else []
_opcoes_empresa_inicial = sorted(_df_inicial["empresa"].dropna().unique()) if not _df_inicial.empty and "empresa" in _df_inicial.columns else []


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

    # Seletor Macro / API
    html.Div([
        html.Label("Selecionar macro:", style={"fontWeight": "700", "fontSize": "14px",
                                               "marginRight": "12px", "color": "#2c3e50"}),
        dcc.RadioItems(
            id="selector-tipo-macro",
            options=[
                {"label": "  Macro", "value": "macro"},
                {"label": "  API  (em breve)", "value": "api"},
            ],
            value="macro",
            inline=True,
            inputStyle={"marginRight": "6px", "cursor": "pointer"},
            labelStyle={"marginRight": "24px", "fontFamily": "Roboto", "fontSize": "15px",
                        "fontWeight": "600", "cursor": "pointer"},
        ),
    ], style={"background": "#eaf4fb", "padding": "10px 16px", "borderRadius": "8px",
              "marginBottom": "8px", "display": "flex", "alignItems": "center",
              "boxShadow": "0 1px 4px rgba(44,62,80,0.08)"}),

    # Seletor Fornecedor
    html.Div([
        html.Label("Fornecedor:", style={"fontWeight": "700", "fontSize": "14px",
                                         "marginRight": "12px", "color": "#2c3e50"}),
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
    ], style={"background": "#f0f7f0", "padding": "10px 16px", "borderRadius": "8px",
              "marginBottom": "12px", "display": "flex", "alignItems": "center",
              "boxShadow": "0 1px 4px rgba(44,62,80,0.08)"}),

    # Info bar
    html.Div(id="info-registros", style={"marginBottom": "12px", "fontSize": "14px", "fontWeight": "600"}),

    # Filtros
    html.Div([
        html.Div([
            html.Label("Filtrar dias", style={"fontWeight": "700", "fontSize": "13px",
                                              "marginBottom": "6px", "display": "block", "color": "#2c3e50"}),
            dcc.Dropdown(
                id="resumo-dia-dropdown",
                options=[{"label": str(d), "value": str(d)} for d in _opcoes_dia_inicial],
                multi=True, clearable=True, placeholder="Todas as datas",
                style={"width": "100%"},
            ),
        ], style={"flex": "1", "minWidth": "260px", "background": "#fff", "padding": "10px",
                  "borderRadius": "8px", "boxShadow": "0 1px 6px rgba(44,62,80,0.06)"}),

        html.Div([
            html.Label("Filtrar empresa", style={"fontWeight": "700", "fontSize": "13px",
                                                 "marginBottom": "6px", "display": "block", "color": "#2c3e50"}),
            dcc.Dropdown(
                id="filtro-empresa-dropdown",
                options=[{"label": str(e), "value": str(e)} for e in _opcoes_empresa_inicial],
                multi=True, clearable=True, placeholder="Todas as empresas",
                style={"width": "100%"},
            ),
        ], style={"flex": "1", "minWidth": "260px", "background": "#fff", "padding": "10px",
                  "borderRadius": "8px", "boxShadow": "0 1px 6px rgba(44,62,80,0.06)"}),

    ], style={"display": "flex", "gap": "12px", "alignItems": "stretch",
              "marginBottom": "12px", "marginTop": "8px"}),

    # Conteudo principal
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
                style_header={"backgroundColor": "#2980b9", "color": "white",
                               "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "15px"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"},
                    {"if": {"filter_query": '{dia} = "Total"'}, "fontWeight": "bold",
                     "backgroundColor": "#d6eaf8"},
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
                    style_header={"backgroundColor": "#2980b9", "color": "white",
                                   "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "15px"},
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"},
                    ],
                    page_size=12,
                ),
            ], style={"background": "#fff", "borderRadius": "8px", "boxShadow": "0 2px 8px #e0e0e0",
                      "padding": "12px", "marginBottom": "22px"}),

        ], style={"width": "100%"}),

        # Card: Arquivo de origem
        html.Div([
            html.H3("Registros por arquivo de origem",
                    style={**SUBTITLE_STYLE, "marginTop": "0", "marginBottom": "8px"}),
            html.P(
                "Migracao historica = registros importados sem data de extracao (ETL manual). "
                "Demais valores indicam o lote/campanha do pipeline automatico.",
                style={"fontSize": "13px", "color": "#666", "marginBottom": "10px"}
            ),
            dash_table.DataTable(
                id="tabela-origens",
                columns=[
                    {"name": "Arquivo / Origem", "id": "arquivo_origem"},
                    {"name": "Quantidade",        "id": "quantidade"},
                ],
                data=[],
                style_table={"overflowX": "auto", "borderRadius": "8px",
                             "boxShadow": "0 2px 8px #e0e0e0", "marginTop": "4px"},
                style_cell={"textAlign": "left", "fontFamily": "Roboto", "fontSize": "14px",
                            "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                style_header={"backgroundColor": "#27ae60", "color": "white",
                               "fontWeight": "bold", "fontFamily": "Roboto", "fontSize": "15px"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"},
                    {"if": {"filter_query": '{arquivo_origem} = "Migracao historica"'},
                     "backgroundColor": "#fef9e7", "fontStyle": "italic"},
                ],
                page_size=10,
            ),
        ], style={"background": "#fff", "borderRadius": "8px", "boxShadow": "0 2px 8px #e0e0e0",
                  "padding": "16px", "marginBottom": "18px"}),

    ], style={"background": "#f4f6f8", "padding": "28px", "borderRadius": "10px", "marginBottom": "32px"})),

    html.Div(style={"height": "8px"}),

], style={"maxWidth": "1100px", "margin": "0 auto", "fontFamily": "Roboto",
          "background": "#fff", "padding": "16px 0"})


# --------------------------------------------------------------------------
# Callbacks
# --------------------------------------------------------------------------

@app.callback(
    [
        dash.dependencies.Output("resumo-dia-dropdown",    "options"),
        dash.dependencies.Output("resumo-dia-dropdown",    "value"),
        dash.dependencies.Output("filtro-empresa-dropdown","options"),
        dash.dependencies.Output("filtro-empresa-dropdown","value"),
        dash.dependencies.Output("info-registros",         "children"),
    ],
    [
        dash.dependencies.Input("selector-tipo-macro",  "value"),
        dash.dependencies.Input("selector-fornecedor",  "value"),
    ]
)
def atualizar_opcoes_filtros(tipo_macro, fornecedor):
    tipo = tipo_macro or "macro"
    filtro_forn = fornecedor if fornecedor and fornecedor != "todos" else None
    df = loader.carregar_dados(tipo)
    if df.empty:
        return [], None, [], None, f"Sem dados para {tipo.upper()}"
    dff = df[df["fornecedor"] == filtro_forn] if filtro_forn and "fornecedor" in df.columns else df
    opcoes_dia     = sorted(dff["dia"].dropna().unique())
    opcoes_empresa = sorted(dff["empresa"].dropna().unique()) if "empresa" in dff.columns else []
    label_forn = f" | Fornecedor: {filtro_forn}" if filtro_forn else ""
    info = (
        f"Registros: {len(dff):,}  |  "
        f"Dias: {len(opcoes_dia)}  |  "
        f"Empresas: {len(opcoes_empresa)}"
        f"{label_forn}"
    )
    return (
        [{"label": str(d), "value": str(d)} for d in opcoes_dia],
        None,
        [{"label": str(e), "value": str(e)} for e in opcoes_empresa],
        None,
        info,
    )


@app.callback(
    [
        dash.dependencies.Output("tabela-resumo",    "data"),
        dash.dependencies.Output("tabela-mensagens", "data"),
        dash.dependencies.Output("tabela-origens",   "data"),
    ],
    [
        dash.dependencies.Input("resumo-dia-dropdown",     "value"),
        dash.dependencies.Input("filtro-empresa-dropdown", "value"),
        dash.dependencies.Input("selector-tipo-macro",     "value"),
        dash.dependencies.Input("selector-fornecedor",     "value"),
    ]
)
def atualizar_dashboard(resumo_sel, filtro_empresa, tipo_macro, fornecedor):
    tipo = tipo_macro or "macro"
    filtro_forn = fornecedor if fornecedor and fornecedor != "todos" else None
    try:
        data_resumo, data_mensagens, data_origens = orchestrator.build_dashboard_data(
            resumo_sel, filtro_empresa, tipo_macro=tipo, filtro_fornecedor=filtro_forn
        )
    except Exception:
        data_resumo = []
        data_mensagens = []
        data_origens = []
    return data_resumo, data_mensagens, data_origens


@app.server.route("/_debug/data")
def debug_data():
    try:
        data_resumo, data_mensagens, data_origens = orchestrator.build_dashboard_data(
            [], [], tipo_macro="macro"
        )
        return jsonify({
            "data_resumo":    data_resumo,
            "data_mensagens": data_mensagens,
            "data_origens":   data_origens,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False, use_reloader=False)
