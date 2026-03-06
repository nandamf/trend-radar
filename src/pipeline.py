from extract import extract_trends
from transform import transform_data
from load import load_to_duckdb
from analysis import get_top_trends, forecast_trends
from insights import run_all_insights
from trend_graph import build_trend_graph, show_graph_info


def run_pipeline():

    print("🔍 Extracting data...")
    df = extract_trends()

    print("🔧 Transforming data...")
    df = transform_data(df)

    print("💾 Loading data...")
    load_to_duckdb(df)

    print("📊 Running trend analysis...")
    trends = get_top_trends()

    print("\n🚀 Top Emerging AI Trends\n")

    for i, row in enumerate(trends.head(5).iter_rows(), start=1):
        keyword, growth = row
        print(f"{i}️⃣ {keyword} ↑ {growth}")

    print("\n📈 Forecasting trends...\n")

    forecast = forecast_trends()

    for row in forecast.head(5).iter_rows():
        keyword, value, direction = row
        arrow = "↑" if direction == "up" else "↓"
        print(f"{keyword} {arrow} predicted interest {value:.2f}")

    print("\n🧠 Running insights...\n")

    run_all_insights()

    print("\n🌐 Building AI Trend Graph...\n")

    graph = build_trend_graph()
    show_graph_info(graph)


if __name__ == "__main__":
    run_pipeline()