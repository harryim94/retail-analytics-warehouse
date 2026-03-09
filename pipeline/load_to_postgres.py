import pandas as pd
import os
from sqlalchemy import create_engine, text


"""
Goal:
- Load mart CSV outputs into Postgres tables.

Steps:
1) Read connection info from ENV (no hardcode)
2) Connect to Postgres
3) Create schemas/tables if not exist (mart schema + 3 tables)
4) Read CSVs from data/mart
5) Load into Postgres (replace tables for now)
6) Validate row counts (CSV rows == DB rows)
"""

mart_dir = "data/mart"

sales_fact_csv = os.path.join(mart_dir,"sales_fact.csv")
dim_date_csv = os.path.join(mart_dir,"dim_date.csv")
dim_product_csv = os.path.join(mart_dir,"dim_product.csv")

def get_pg_url() -> str:
    """
    expect env vars:
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    """
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", '5432')
    db = os.getenv("PG_DB", "retail_dw")
    user = os.getenv("PG_USER","apple")
    pw = os.getenv("PG_PASSWORD", "")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

def ensure_schema_and_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart;"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS mart.sales_fact (
          sale_date DATE NOT NULL,
          sku TEXT NOT NULL,
          gross_quantity BIGINT NOT NULL,
          gross_revenue_local DOUBLE PRECISION NOT NULL,
          cancelled_quantity BIGINT NOT NULL,
          cancelled_revenue_local DOUBLE PRECISION NOT NULL,
          net_quantity BIGINT NOT NULL,
          net_revenue_local DOUBLE PRECISION NOT NULL,
          PRIMARY KEY (sale_date, sku)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS mart.dim_date (
          sale_date DATE PRIMARY KEY,
          year INT NOT NULL,
          month INT NOT NULL,
          weekday INT NOT NULL,
          is_weekend BOOLEAN NOT NULL
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS mart.dim_product (
          sku TEXT PRIMARY KEY
        );
        """))

def load_csv_replace(engine, csv_path: str, table: str) -> int:
    df = pd.read_csv(csv_path)

    # normalize dates
    if "sale_date" in df.columns:
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date

    # replace for now (idempotent). later we can do incremental loads.
    schema, name = table.split(".")
    df.to_sql(name=name, con=engine, schema=schema, if_exists="replace", index=False, method="multi")

    return len(df)


def count_rows(engine, table: str) -> int:
    with engine.begin() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table};")).scalar_one()


def main():
    engine = create_engine(get_pg_url())

    ensure_schema_and_tables(engine)

    loaded_fact = load_csv_replace(engine, sales_fact_csv, "mart.sales_fact")
    loaded_date = load_csv_replace(engine, dim_date_csv, "mart.dim_date")
    loaded_prod = load_csv_replace(engine, dim_product_csv, "mart.dim_product")

    db_fact = count_rows(engine, "mart.sales_fact")
    db_date = count_rows(engine, "mart.dim_date")
    db_prod = count_rows(engine, "mart.dim_product")

    print("Loaded rows (csv): sales_fact", loaded_fact, "| dim_date", loaded_date, "| dim_product", loaded_prod)
    print("DB rows:            sales_fact", db_fact, "| dim_date", db_date, "| dim_product", db_prod)
    print("Match? sales_fact", loaded_fact == db_fact, "/ dim_date", loaded_date == db_date, "/ dim_product", loaded_prod == db_prod)


if __name__ == "__main__":
    main()        