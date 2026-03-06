import duckdb
import polars as pl
import numpy as np
from sklearn.linear_model import LinearRegression


def load_trends():

    con = duckdb.connect("data/trends.duckdb")

    df = con.execute("""
        SELECT date, keyword, interest
        FROM trends
    """).fetch_arrow_table()

    return pl.from_arrow(df)


def get_trend_scores():

    df = load_trends()

    results = []

    keywords = df.select("keyword").unique().to_series().to_list()

    for keyword in keywords:

        subset = (
            df.filter(pl.col("keyword") == keyword)
            .sort("date")
            .with_row_count("t")
        )

        X = subset["t"].to_numpy().reshape(-1, 1)
        y = subset["interest"].to_numpy()

        model = LinearRegression()
        model.fit(X, y)

        slope = model.coef_[0]

        results.append({
            "keyword": keyword,
            "trend_score": float(slope)
        })

    return (
        pl.DataFrame(results)
        .sort("trend_score", descending=True)
    )


def get_top_trends():

    df = load_trends()

    return (
        df.group_by("keyword")
        .agg([
            pl.col("interest").min().alias("min_interest"),
            pl.col("interest").max().alias("max_interest")
        ])
        .with_columns(
            (pl.col("max_interest") - pl.col("min_interest")).alias("growth")
        )
        .sort("growth", descending=True)
        .select(["keyword", "growth"])
    )

def forecast_trends(steps: int = 5):

    df = load_trends()

    forecasts = []

    keywords = df.select("keyword").unique().to_series().to_list()

    for keyword in keywords:

        subset = (
            df.filter(pl.col("keyword") == keyword)
            .sort("date")
            .with_row_count("t")
        )

        X = subset["t"].to_numpy().reshape(-1, 1)
        y = subset["interest"].to_numpy()

        model = LinearRegression()
        model.fit(X, y)

        last_t = subset["t"].max()

        future_t = np.arange(last_t + 1, last_t + steps + 1).reshape(-1, 1)

        predictions = model.predict(future_t)

        forecasts.append({
            "keyword": keyword,
            "forecast_interest": float(predictions[-1]),
            "trend_direction": "up"
            if predictions[-1] > y[-1]
            else "down"
        })

    return (
        pl.DataFrame(forecasts)
        .sort("forecast_interest", descending=True)
    )

def momentum_score():

    trends = get_top_trends()
    forecast = forecast_trends()

    df = trends.join(forecast, on="keyword")

    df = df.with_columns(
        (
            pl.col("growth") * 0.6 +
            pl.col("forecast_interest") * 0.4
        ).alias("momentum_score")
    )

    return df.sort("momentum_score", descending=True)