from __future__ import annotations

import numpy as np
import polars as pl
from sklearn.linear_model import LinearRegression

from src.data_access import fetch_trends


def _fit_slope(values: np.ndarray) -> float:
    x = np.arange(len(values)).reshape(-1, 1)
    model = LinearRegression().fit(x, values)
    return float(model.coef_[0])


def _keyword_series(df: pl.DataFrame, keyword: str) -> np.ndarray:
    return (
        df.filter(pl.col("keyword") == keyword)
        .sort("date")["interest"]
        .to_numpy()
    )


def get_top_trends(df: pl.DataFrame | None = None) -> pl.DataFrame:
    frame = fetch_trends() if df is None else df
    return (
        frame.group_by("keyword")
        .agg(
            pl.col("interest").min().alias("min_interest"),
            pl.col("interest").max().alias("max_interest"),
        )
        .with_columns((pl.col("max_interest") - pl.col("min_interest")).alias("growth"))
        .sort("growth", descending=True)
        .select(["keyword", "growth"])
    )


def forecast_trends(steps: int = 5, df: pl.DataFrame | None = None) -> pl.DataFrame:
    frame = fetch_trends() if df is None else df
    forecasts = []
    keywords = frame.select("keyword").unique().to_series().to_list()

    for keyword in keywords:
        series = _keyword_series(frame, keyword)
        if len(series) < 2:
            continue

        x = np.arange(len(series)).reshape(-1, 1)
        model = LinearRegression().fit(x, series)

        future_x = np.arange(len(series), len(series) + steps).reshape(-1, 1)
        prediction = float(model.predict(future_x)[-1])

        forecasts.append(
            {
                "keyword": keyword,
                "forecast_interest": prediction,
                "trend_direction": "up" if prediction > float(series[-1]) else "down",
            }
        )

    if not forecasts:
        return pl.DataFrame(
            schema={
                "keyword": pl.Utf8,
                "forecast_interest": pl.Float64,
                "trend_direction": pl.Utf8,
            }
        )
    return pl.DataFrame(forecasts).sort("forecast_interest", descending=True)


def momentum_score(df: pl.DataFrame | None = None) -> pl.DataFrame:
    frame = fetch_trends() if df is None else df
    trends = get_top_trends(frame)
    forecast = forecast_trends(df=frame)
    if trends.is_empty() or forecast.is_empty():
        return pl.DataFrame(
            schema={
                "keyword": pl.Utf8,
                "growth": pl.Float64,
                "forecast_interest": pl.Float64,
                "trend_direction": pl.Utf8,
                "momentum_score": pl.Float64,
            }
        )
    return (
        trends.join(forecast, on="keyword")
        .with_columns(
            (
                pl.col("growth") * 0.55
                + pl.col("forecast_interest") * 0.35
                + pl.when(pl.col("trend_direction") == "up").then(10).otherwise(-10) * 0.10
            ).alias("momentum_score")
        )
        .sort("momentum_score", descending=True)
    )


def trend_diagnostics(df: pl.DataFrame | None = None, recent_window: int = 12) -> pl.DataFrame:
    frame = fetch_trends() if df is None else df
    if frame.is_empty():
        return pl.DataFrame(
            schema={
                "keyword": pl.Utf8,
                "avg_interest": pl.Float64,
                "latest_interest": pl.Float64,
                "growth": pl.Float64,
                "volatility": pl.Float64,
                "long_slope": pl.Float64,
                "short_slope": pl.Float64,
                "acceleration": pl.Float64,
                "forecast_interest": pl.Float64,
                "reversal_risk": pl.Float64,
                "strategic_score": pl.Float64,
            }
        )

    records = []
    for keyword in frame.select("keyword").unique().to_series().to_list():
        series = _keyword_series(frame, keyword)
        if len(series) < 4:
            continue

        long_slope = _fit_slope(series)
        short_tail = series[-min(recent_window, len(series)) :]
        short_slope = _fit_slope(short_tail)
        acceleration = short_slope - long_slope

        x = np.arange(len(series)).reshape(-1, 1)
        model = LinearRegression().fit(x, series)
        forecast = float(model.predict(np.array([[len(series) + 4]]))[0])

        avg_interest = float(np.mean(series))
        latest = float(series[-1])
        growth = float(np.max(series) - np.min(series))
        volatility = float(np.std(series))
        downturn = max(0.0, -short_slope)
        reversal_risk = float((downturn * 0.65) + (volatility * 0.35))

        records.append(
            {
                "keyword": keyword,
                "avg_interest": avg_interest,
                "latest_interest": latest,
                "growth": growth,
                "volatility": volatility,
                "long_slope": long_slope,
                "short_slope": short_slope,
                "acceleration": acceleration,
                "forecast_interest": forecast,
                "reversal_risk": reversal_risk,
            }
        )

    diagnostics = pl.DataFrame(records)
    if diagnostics.is_empty():
        return diagnostics

    return (
        diagnostics.with_columns(
            (
                pl.col("avg_interest") * 0.25
                + pl.col("growth") * 0.20
                + pl.col("acceleration") * 0.35
                + (pl.col("forecast_interest") - pl.col("latest_interest")) * 0.20
                - pl.col("reversal_risk") * 0.15
            ).alias("strategic_score")
        )
        .sort("strategic_score", descending=True)
    )
