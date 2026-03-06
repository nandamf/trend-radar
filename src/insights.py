import duckdb
import polars as pl


def load_data():
    con = duckdb.connect("data/trends.duckdb")

    df = con.execute("""
        SELECT date, keyword, interest
        FROM trends
    """).fetchdf()

    return pl.from_pandas(df)


def popularity_insight(df):

    return (
        df.group_by("keyword")
        .agg(pl.col("interest").mean().alias("avg_interest"))
        .sort("avg_interest", descending=True)
    )


def growth_insight(df):

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


def volatility_insight(df):

    return (
        df.group_by("keyword")
        .agg(pl.col("interest").std().alias("volatility"))
        .sort("volatility", descending=True)
    )


def momentum_insight(df):

    latest = (
        df.sort("date")
        .group_by("keyword")
        .tail(1)
    )

    mean = (
        df.group_by("keyword")
        .agg(pl.col("interest").mean().alias("avg_interest"))
    )

    momentum = latest.join(mean, on="keyword")

    return (
        momentum.with_columns(
            (pl.col("interest") / pl.col("avg_interest")).alias("momentum")
        )
        .sort("momentum", descending=True)
        .select(["keyword", "momentum"])
    )


def trend_score(df):

    popularity = (
        df.group_by("keyword")
        .agg(pl.col("interest").mean().alias("popularity"))
    )

    growth = (
        df.group_by("keyword")
        .agg(
            (pl.col("interest").max() - pl.col("interest").min())
            .alias("growth")
        )
    )

    latest = (
        df.sort("date")
        .group_by("keyword")
        .tail(1)
    )

    mean = popularity.rename({"popularity": "avg_interest"})

    momentum = latest.join(mean, on="keyword").with_columns(
        (pl.col("interest") / pl.col("avg_interest")).alias("momentum")
    ).select(["keyword", "momentum"])

    volatility = (
        df.group_by("keyword")
        .agg(pl.col("interest").std().alias("volatility"))
    )

    score = (
        popularity
        .join(growth, on="keyword")
        .join(momentum, on="keyword")
        .join(volatility, on="keyword")
        .with_columns(
            (
                pl.col("popularity") * 0.3
                + pl.col("growth") * 0.3
                + pl.col("momentum") * 20
                + pl.col("volatility") * 0.1
            ).alias("trend_score")
        )
        .sort("trend_score", descending=True)
    )

    return score

def emerging_technologies(df):

    # ordenar por data
    df = df.sort("date")

    # últimas observações (janela recente)
    recent = (
        df.group_by("keyword")
        .tail(24)
        .group_by("keyword")
        .agg(pl.col("interest").mean().alias("recent_mean"))
    )

    # média histórica
    historical = (
        df.group_by("keyword")
        .agg(pl.col("interest").mean().alias("historical_mean"))
    )

    emerging = (
        recent
        .join(historical, on="keyword")
        .with_columns(
            (pl.col("recent_mean") / pl.col("historical_mean")).alias("emergence_score")
        )
        .sort("emergence_score", descending=True)
    )

    return emerging

def run_all_insights():

    df = load_data()

    print("\n📊 AI TECHNOLOGY INSIGHTS\n")

    print("\n🔥 Most Popular Technologies")
    print(popularity_insight(df).head())

    print("\n📈 Fastest Growing Technologies")
    print(growth_insight(df).head())

    print("\n🚀 Highest Momentum Technologies")
    print(momentum_insight(df).head())

    print("\n⚡ Most Volatile Technologies")
    print(volatility_insight(df).head())

    print("\n🏆 AI Trend Score")
    print(trend_score(df).head())

    print("\n🧠 Emerging Technologies")
    print(emerging_technologies(df).head())



if __name__ == "__main__":
    run_all_insights()