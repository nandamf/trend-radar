import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.data_access import load_to_duckdb as _load_to_duckdb
from src.extract import extract_trends
from src.transform import transform_data


def load_to_duckdb(df: pl.DataFrame, replace: bool = False) -> None:
    _load_to_duckdb(df, replace=replace)

if __name__ == "__main__":

    df = extract_trends()
    transformed = transform_data(df)
    load_to_duckdb(transformed)

    print("Data loaded to DuckDB successfully!")
