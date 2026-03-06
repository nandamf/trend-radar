import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.extract import extract_trends


def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    # Remove a coluna adicional do Google Trends (real vs interpolado).
    if "isPartial" in df.columns:
        df = df.drop("isPartial")

    # Converte de formato wide para long.
    df_long = df.unpivot(
        index="date",
        variable_name="keyword",
        value_name="interest",
    )

    # Normaliza keywords para facilitar comparacoes.
    return df_long.with_columns(pl.col("keyword").str.to_lowercase())


if __name__ == "__main__":
    df = extract_trends()
    transformed = transform_data(df)
    print(transformed.head())
