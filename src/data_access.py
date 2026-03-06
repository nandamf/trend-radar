from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

DB_PATH = Path("data/trends.duckdb")


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def ensure_trends_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS trends (
            date TIMESTAMP,
            keyword VARCHAR,
            interest INTEGER
        )
        """
    )


def fetch_trends() -> pl.DataFrame:
    with get_connection() as con:
        ensure_trends_table(con)
        table = con.execute(
            """
            SELECT date, keyword, interest
            FROM trends
            ORDER BY date, keyword
            """
        ).fetch_arrow_table()

    return pl.from_arrow(table).with_columns(
        pl.col("date").cast(pl.Datetime),
        pl.col("keyword").cast(pl.Utf8),
        pl.col("interest").cast(pl.Int64),
    )


def load_to_duckdb(df: pl.DataFrame, replace: bool = False) -> None:
    with get_connection() as con:
        ensure_trends_table(con)

        if replace:
            con.execute("DELETE FROM trends")

        con.register("df_input", df.to_arrow())
        con.execute(
            """
            INSERT INTO trends (date, keyword, interest)
            SELECT date, keyword, interest
            FROM df_input
            """
        )
