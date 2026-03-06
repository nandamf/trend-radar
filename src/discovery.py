import polars as pl


BASE_TOPICS = [
    "artificial intelligence",
    "machine learning",
    "generative ai",
    "llm",
    "data engineering"
]


AI_EXPANSIONS = [
    "ai agents",
    "rag",
    "vector database",
    "langchain",
    "mlops",
    "feature store",
    "prompt engineering",
    "ai automation",
    "foundation models",
    "multimodal ai"
]


def discover_topics():

    discovered = set()

    for topic in BASE_TOPICS:
        discovered.add(topic)

    for topic in AI_EXPANSIONS:
        discovered.add(topic)

    return list(discovered)


def discover_topics_df():

    topics = discover_topics()

    return pl.DataFrame({
        "topic": topics
    })


if __name__ == "__main__":

    df = discover_topics_df()

    print("\n🔎 Discovered AI Topics\n")

    print(df)