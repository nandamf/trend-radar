import polars as pl

from extract import extract_trends

def transform_data(df: pl.DataFrame) -> pl.DataFrame:

    #remover a coluna que google trends adiciona para indicar se os dados são reais ou interpolados

    if "isPartial" in df.columns:
        df = df.drop("isPartial")
    
    #transformar formato wide -> long

    df_long = df.unpivot(
        index = "date",
        variable_name=  "keyword",
        value_name= "interest"
    )

    #normalizar texto
    df_long = df_long.with_columns(
        pl.col('keyword').str.to_lowercase()
    )
    return df_long

if __name__ == "__main__":
    df = extract_trends()
    transformed = transform_data(df)
    print(transformed.head())