import duckdb
from extract import extract_trends
from transform import transform_data

def load_to_duckdb(df):

    con = duckdb.connect("data/trends.duckdb")

    con.execute("""
    CREATE TABLE IF NOT EXISTS trends (
                date TIMESTAMP,
                keyword VARCHAR,
                interest INTEGER)
    """)

    con.execute("""
    INSERT INTO trends
    SELECT * FROM df
    """)

    con.close()

if __name__ == "__main__":

    df = extract_trends()
    transformed = transform_data(df)
    load_to_duckdb(transformed)

    print("Data loaded to DuckDB successfully!")