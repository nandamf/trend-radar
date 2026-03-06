import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.analysis import forecast_trends, get_top_trends, momentum_score, trend_diagnostics
from src.data_access import fetch_trends


def popularity_insight(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("keyword")
        .agg(pl.col("interest").mean().alias("avg_interest"))
        .sort("avg_interest", descending=True)
    )


def growth_insight(df: pl.DataFrame) -> pl.DataFrame:
    return get_top_trends(df)


def volatility_insight(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("keyword")
        .agg(pl.col("interest").std().alias("volatility"))
        .sort("volatility", descending=True)
    )


def run_all_insights() -> None:
    df = fetch_trends()

    print("\nAI TECHNOLOGY INSIGHTS\n")
    print("Most Popular Technologies")
    print(popularity_insight(df).head())

    print("\nFastest Growing Technologies")
    print(growth_insight(df).head())

    print("\nTrend Forecast")
    print(forecast_trends(df=df).head())

    print("\nMomentum Score")
    print(momentum_score(df).head())

    print("\nStrategic Diagnostics (acceleration and reversal risk)")
    print(trend_diagnostics(df).head())

    print("\nMost Volatile Technologies")
    print(volatility_insight(df).head())


if __name__ == "__main__":
    run_all_insights()
