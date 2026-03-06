import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.analysis import forecast_trends, get_top_trends
from src.extract import extract_trends
from src.insights import run_all_insights
from src.load import load_to_duckdb
from src.transform import transform_data
from src.trend_graph import build_trend_graph, show_graph_info


def run_pipeline() -> None:
    print("Extracting data...")
    df = extract_trends()

    print("Transforming data...")
    df = transform_data(df)

    print("Loading data...")
    load_to_duckdb(df)

    print("Running trend analysis...")
    trends = get_top_trends()

    print("\nTop Emerging AI Trends\n")
    for i, row in enumerate(trends.head(5).iter_rows(), start=1):
        keyword, growth = row
        print(f"{i}. {keyword} -> growth {growth}")

    print("\nForecasting trends...\n")
    forecast = forecast_trends()

    for row in forecast.head(5).iter_rows():
        keyword, value, direction = row
        arrow = "up" if direction == "up" else "down"
        print(f"{keyword} {arrow} predicted interest {value:.2f}")

    print("\nRunning insights...\n")
    run_all_insights()

    print("\nBuilding AI Trend Graph...\n")
    graph = build_trend_graph()
    show_graph_info(graph)


if __name__ == "__main__":
    run_pipeline()
