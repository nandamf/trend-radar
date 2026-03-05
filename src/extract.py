from pytrends.request import TrendReq
import polars as pl

TECH_KEYWORDS = [
    "Artificial Intelligence",
    "Machine Learning",
    "Data Engineering",
    "LLM",
    "Generative AI"
]

def extract_trends():

    pytrends = TrendReq(hl="en-US", tz=360)
    pytrends.build_payload(
        kw_list=TECH_KEYWORDS,
        timeframe="now 7-d",
        geo=""
    )

    data = pytrends.interest_over_time()
    df = pl.from_pandas(data.reset_index())

    return df

if __name__ == "__main__":
    df = extract_trends()
    print(df.head())