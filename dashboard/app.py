import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import duckdb
import polars as pl
import plotly.express as px
from pyvis.network import Network

from src.analysis import get_top_trends, forecast_trends, momentum_score
from src.trend_graph import build_trend_graph


# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="AI Trend Radar",
    page_icon="🚀",
    layout="wide"
)

# ---------------------------------------------------
# CUSTOM STYLE
# ---------------------------------------------------

st.markdown("""
<style>

.main {
    background-color: #0E1117;
}

.block-container {
    padding-top: 2rem;
}

h1, h2, h3 {
    font-weight: 700;
}

[data-testid="stMetric"] {
    background-color: #1E1E2F;
    border-radius: 12px;
    padding: 15px;
}

</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------
# HEADER
# ---------------------------------------------------

st.markdown("""
# 🚀 AI Technology Trend Radar
Real-time intelligence on emerging artificial intelligence technologies
""")

# ---------------------------------------------------
# SIDEBAR
# ---------------------------------------------------

st.sidebar.title("AI Trend Radar")

st.sidebar.markdown(
"""
Explore emerging technologies and identify which AI topics are gaining momentum.
"""
)

# ---------------------------------------------------
# LOAD DATA
# ---------------------------------------------------

con = duckdb.connect("data/trends.duckdb")

df = pl.from_arrow(
    con.execute("""
        SELECT date, keyword, interest
        FROM trends
    """).fetch_arrow_table()
)

ranking = get_top_trends()
forecast = forecast_trends()

# ---------------------------------------------------
# KPI METRICS
# ---------------------------------------------------

top_tech = ranking[0, "keyword"]
top_growth = ranking[0, "growth"]

forecast_up = forecast.filter(pl.col("trend_direction") == "up").height
forecast_down = forecast.filter(pl.col("trend_direction") == "down").height

col1, col2, col3, col4 = st.columns(4)

col1.metric("🔥 Fastest Growing", top_tech)
col2.metric("📈 Growth Score", int(top_growth))
col3.metric("🚀 Rising Trends", forecast_up)
col4.metric("⚠ Declining Trends", forecast_down)

st.divider()

# ---------------------------------------------------
# TREND EVOLUTION
# ---------------------------------------------------

st.header("📈 Technology Trend Evolution")

st.divider()

st.header("🔥 AI Trend Heatmap")

heat = (
    df.group_by(["date", "keyword"])
    .agg(pl.col("interest").mean())
)

heat_df = heat.to_pandas()

fig_heat = px.density_heatmap(
    heat_df,
    x="date",
    y="keyword",
    z="interest",
    color_continuous_scale="Turbo"
)

fig_heat.update_layout(template="plotly_dark")

st.plotly_chart(fig_heat, use_container_width=True)

keywords = df["keyword"].unique().to_list()

selected = st.multiselect(
    "Select technologies",
    options=keywords,
    default=keywords[:3]
)

filtered = df.filter(pl.col("keyword").is_in(selected))

fig = px.line(
    filtered.to_pandas(),
    x="date",
    y="interest",
    color="keyword",
    markers=True
)

fig.update_layout(
    template="plotly_dark",
    title="AI Technology Interest Over Time",
    height=450,
    margin=dict(l=0, r=0, t=40, b=0),
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------
# FORECAST
# ---------------------------------------------------

st.header("🔮 Trend Forecast")

for row in forecast.head(5).iter_rows():

    keyword, value, direction = row

    arrow = "📈" if direction == "up" else "📉"

    st.write(f"{arrow} **{keyword}** → predicted interest {value:.2f}")

st.divider()

# ---------------------------------------------------
# RANKING
# ---------------------------------------------------

st.header("🏆 Top Trending Technologies")

st.divider()

st.header("⚡ Trend Momentum Score")

momentum = momentum_score()

fig_momentum = px.bar(
    momentum.to_pandas(),
    x="momentum_score",
    y="keyword",
    orientation="h",
    color="momentum_score",
    color_continuous_scale="Plasma"
)

fig_momentum.update_layout(template="plotly_dark")

st.plotly_chart(fig_momentum, use_container_width=True)
fig_rank = px.bar(
    ranking.to_pandas(),
    x="growth",
    y="keyword",
    orientation="h",
    color="growth",
    color_continuous_scale="Turbo"
)

fig_rank.update_layout(
    template="plotly_dark",
    height=350,
    margin=dict(l=0, r=0, t=30, b=0)
)

st.plotly_chart(fig_rank, use_container_width=True)

st.divider()

# ---------------------------------------------------
# AI ECOSYSTEM GRAPH
# ---------------------------------------------------

st.divider()

st.header("📡 AI Technology Radar")

radar = momentum.head(5)

fig_radar = px.line_polar(
    radar.to_pandas(),
    r="momentum_score",
    theta="keyword",
    line_close=True
)

fig_radar.update_traces(fill="toself")

fig_radar.update_layout(template="plotly_dark")

st.plotly_chart(fig_radar, use_container_width=True)

st.header("🌐 AI Ecosystem Map")

graph = build_trend_graph()

net = Network(
    height="500px",
    width="100%",
    bgcolor="#0E1117",
    font_color="white"
)

for node in graph.nodes():
    net.add_node(node, label=node, size=30)

for edge in graph.edges():
    net.add_edge(edge[0], edge[1])

net.barnes_hut()

html = net.generate_html()

st.components.v1.html(html, height=550)