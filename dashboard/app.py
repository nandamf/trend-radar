from __future__ import annotations

import sys
import threading
import webbrowser
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, dash_table, dcc, html

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.analysis import forecast_trends, trend_diagnostics
from src.data_access import fetch_trends


def _base_dataframe() -> pl.DataFrame:
    df = fetch_trends()
    return df.with_columns(pl.col("date").cast(pl.Date))


DATA = _base_dataframe()
ALL_KEYWORDS = sorted(DATA["keyword"].unique().to_list())


def _empty_figure(title: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template="plotly_white",
        title=title,
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{"text": "Sem dados para os filtros selecionados", "showarrow": False}],
    )
    return fig


def _kpi_card(title: str, value_id: str, subtitle_id: str) -> dbc.Card:
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(title, className="kpi-title"),
                html.H3(id=value_id, className="kpi-value"),
                html.Small(id=subtitle_id, className="text-muted"),
            ]
        ),
        className="kpi-card",
    )


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], suppress_callback_exceptions=True)
app.title = "AI Trend Radar"

app.layout = dbc.Container(
    [
        html.Div(
            [
                html.H1("AI Trend Radar", className="display-6 fw-bold mb-1"),
                html.P(
                    "Painel estratégico com foco em aceleração, tração e risco de reversão.",
                    className="text-muted mb-0",
                ),
            ],
            className="py-3",
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Label("Tecnologias"),
                        dcc.Dropdown(
                            id="keyword-filter",
                            options=[{"label": k.title(), "value": k} for k in ALL_KEYWORDS],
                            value=ALL_KEYWORDS[: min(6, len(ALL_KEYWORDS))],
                            multi=True,
                            placeholder="Selecione tecnologias",
                        ),
                    ],
                    md=8,
                ),
                dbc.Col(
                    [
                        html.Label("Janela de análise"),
                        dcc.Dropdown(
                            id="window-filter",
                            options=[
                                {"label": "Curto prazo (12 pontos)", "value": 12},
                                {"label": "Médio prazo (24 pontos)", "value": 24},
                                {"label": "Longo prazo (36 pontos)", "value": 36},
                            ],
                            value=12,
                            clearable=False,
                        ),
                    ],
                    md=4,
                ),
            ],
            className="g-3 mb-3",
        ),
        dbc.Row(
            [
                dbc.Col(_kpi_card("Lider Estratégico", "kpi-leader", "kpi-leader-sub"), md=3),
                dbc.Col(_kpi_card("Maior Aceleração", "kpi-accel", "kpi-accel-sub"), md=3),
                dbc.Col(_kpi_card("Risco Médio", "kpi-risk", "kpi-risk-sub"), md=3),
                dbc.Col(_kpi_card("Tendências em Alta", "kpi-up", "kpi-up-sub"), md=3),
            ],
            className="g-3 mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="line-trends", config={"displayModeBar": False}), md=8),
                dbc.Col(dcc.Graph(id="forecast-bar", config={"displayModeBar": False}), md=4),
            ],
            className="g-3 mb-3",
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="heatmap", config={"displayModeBar": False}), md=6),
                dbc.Col(dcc.Graph(id="quadrant", config={"displayModeBar": False}), md=6),
            ],
            className="g-3 mb-3",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dash_table.DataTable(
                        id="diagnostic-table",
                        page_size=10,
                        sort_action="native",
                        style_as_list_view=True,
                        style_header={"fontWeight": "bold"},
                        style_cell={"padding": "10px", "fontFamily": "Segoe UI", "fontSize": 13},
                        style_data_conditional=[
                            {
                                "if": {"filter_query": "{reversal_risk} > 12"},
                                "backgroundColor": "#fdecea",
                            },
                            {
                                "if": {"filter_query": "{acceleration} > 0"},
                                "backgroundColor": "#eaf7f0",
                            },
                        ],
                    ),
                    md=12,
                )
            ],
            className="g-3 mb-4",
        ),
    ],
    fluid=True,
    style={"maxWidth": "1500px"},
)


@app.callback(
    Output("kpi-leader", "children"),
    Output("kpi-leader-sub", "children"),
    Output("kpi-accel", "children"),
    Output("kpi-accel-sub", "children"),
    Output("kpi-risk", "children"),
    Output("kpi-risk-sub", "children"),
    Output("kpi-up", "children"),
    Output("kpi-up-sub", "children"),
    Output("line-trends", "figure"),
    Output("forecast-bar", "figure"),
    Output("heatmap", "figure"),
    Output("quadrant", "figure"),
    Output("diagnostic-table", "columns"),
    Output("diagnostic-table", "data"),
    Input("keyword-filter", "value"),
    Input("window-filter", "value"),
)
def update_dashboard(selected_keywords: list[str], window: int):
    selected_keywords = selected_keywords or ALL_KEYWORDS
    filtered = DATA.filter(pl.col("keyword").is_in(selected_keywords))

    if filtered.is_empty():
        empty = _empty_figure("Sem dados")
        return (
            "-",
            "Sem dados",
            "-",
            "Sem dados",
            "-",
            "Sem dados",
            "-",
            "Sem dados",
            empty,
            empty,
            empty,
            empty,
            [],
            [],
        )

    diagnostics = trend_diagnostics(filtered, recent_window=int(window))
    if diagnostics.is_empty():
        diagnostics = pl.DataFrame(
            {
                "keyword": [],
                "strategic_score": [],
                "acceleration": [],
                "reversal_risk": [],
                "forecast_interest": [],
            }
        )

    forecast = forecast_trends(df=filtered).head(10)
    leader = diagnostics[0, "keyword"] if diagnostics.height else "-"
    leader_score = diagnostics[0, "strategic_score"] if diagnostics.height else 0

    accel_leader = diagnostics.sort("acceleration", descending=True)
    accel_name = accel_leader[0, "keyword"] if accel_leader.height else "-"
    accel_value = accel_leader[0, "acceleration"] if accel_leader.height else 0

    risk_avg = diagnostics["reversal_risk"].mean() if diagnostics.height else 0
    up_count = forecast.filter(pl.col("trend_direction") == "up").height if forecast.height else 0

    line_df = filtered.to_pandas()
    fig_line = px.line(
        line_df,
        x="date",
        y="interest",
        color="keyword",
        markers=False,
        template="plotly_white",
        title="Evolucao temporal de interesse",
    )
    fig_line.update_layout(margin={"l": 10, "r": 10, "t": 40, "b": 10}, hovermode="x unified")

    if forecast.height:
        fig_forecast = px.bar(
            forecast.to_pandas(),
            y="keyword",
            x="forecast_interest",
            color="trend_direction",
            orientation="h",
            template="plotly_white",
            title="Projecao de interesse (top 10)",
            color_discrete_map={"up": "#1b9e77", "down": "#d95f02"},
        )
        fig_forecast.update_layout(margin={"l": 10, "r": 10, "t": 40, "b": 10})
    else:
        fig_forecast = _empty_figure("Projecao")

    heat = (
        filtered.group_by(["date", "keyword"])
        .agg(pl.col("interest").mean().alias("interest"))
        .pivot(index="keyword", on="date", values="interest")
        .fill_null(0)
    )
    heat_pd = heat.to_pandas().set_index("keyword")
    fig_heat = px.imshow(
        heat_pd,
        aspect="auto",
        color_continuous_scale="Viridis",
        title="Mapa de calor por tecnologia",
        template="plotly_white",
    )
    fig_heat.update_layout(margin={"l": 10, "r": 10, "t": 40, "b": 10})

    if diagnostics.height:
        fig_quad = px.scatter(
            diagnostics.to_pandas(),
            x="acceleration",
            y="volatility",
            size="avg_interest",
            color="reversal_risk",
            hover_name="keyword",
            template="plotly_white",
            title="Quadrante: aceleracao vs volatilidade",
            color_continuous_scale="RdYlGn_r",
        )
        fig_quad.add_hline(y=float(diagnostics["volatility"].mean()), line_dash="dot", line_color="gray")
        fig_quad.add_vline(x=0, line_dash="dot", line_color="gray")
        fig_quad.update_layout(margin={"l": 10, "r": 10, "t": 40, "b": 10})
    else:
        fig_quad = _empty_figure("Quadrante")

    table = (
        diagnostics.select(
            [
                "keyword",
                "strategic_score",
                "acceleration",
                "reversal_risk",
                "forecast_interest",
            ]
        )
        .with_columns(
            pl.col("strategic_score").round(2),
            pl.col("acceleration").round(2),
            pl.col("reversal_risk").round(2),
            pl.col("forecast_interest").round(2),
        )
    )
    columns = [{"name": col, "id": col} for col in table.columns]
    data = table.to_dicts()

    return (
        leader.title(),
        f"Strategic score: {leader_score:.2f}",
        accel_name.title(),
        f"Aceleracao: {accel_value:.2f}",
        f"{risk_avg:.2f}",
        "Indice combinado de downturn + volatilidade",
        str(up_count),
        f"de {forecast.height} com sinal de alta",
        fig_line,
        fig_forecast,
        fig_heat,
        fig_quad,
        columns,
        data,
    )


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 8050
    url = f"http://{host}:{port}"
    print(f"Dashboard disponivel em: {url}")
    threading.Timer(1.0, lambda: webbrowser.open_new(url)).start()
    app.run(debug=True, host=host, port=port, use_reloader=False)
