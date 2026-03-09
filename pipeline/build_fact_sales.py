import os
import pandas as pd

STG_DIR = "data/staging"
MART_DIR = "data/mart"
os.makedirs(MART_DIR, exist_ok=True)

# TODO: 파일명 네 프로젝트에 맞게 수정
AMAZON_STG_PATH = os.path.join(STG_DIR, "stg_amazon_sales.csv")
INTL_STG_PATH   = os.path.join(STG_DIR, "stg_international_sales.csv")

OUT_FACT_PATH   = os.path.join(MART_DIR, "sales_fact.csv")

def build_sales_fact(stg_amazon: pd.DataFrame, stg_international: pd.DataFrame) -> pd.DataFrame:
    # 1) union all
    # TODO: df = ...
    df = pd.concat([stg_amazon, stg_international], ignore_index=True)    
    # 2) currency INR 통일 (rule)
    # TODO: df["currency"] = ...
    df["currency"] = 'INR'
    # 3) cancelled flag 만들기
    # TODO: df["is_cancelled"] = ...
    df["is_cancelled"] = (df["status"] == "CANCELLED")
    # 4) gross / cancelled measures 분리 컬럼 만들기
    # gross_quantity, gross_revenue_local
    # cancelled_quantity, cancelled_revenue_local
    # TODO: df["gross_quantity"] = ...
    # TODO: df["gross_revenue_local"] = ...
    # TODO: df["cancelled_quantity"] = ...
    # TODO: df["cancelled_revenue_local"] = ...
    df["gross_quantity"] = df["quantity"]
    df["gross_revenue_local"] = df["revenue_local"]
    df["cancelled_quantity"] = df["quantity"].where(df["is_cancelled"], 0)
    df["cancelled_revenue_local"] = df["revenue_local"].where(df["is_cancelled"], 0)
    # 5) groupby agg (grain: sale_date, sku)
    # TODO: fact = df.groupby([...], as_index=False).agg({...})
    fact = (
        df.groupby(["sale_date", "sku"], as_index=False)
          .agg({
              "gross_quantity": "sum",
              "gross_revenue_local": "sum",
              "cancelled_quantity": "sum",
              "cancelled_revenue_local": "sum",
          })
    )
    # 6) net 계산
    # TODO: fact["net_quantity"] = ...
    # TODO: fact["net_revenue_local"] = ...
    fact["net_quantity"] = fact["gross_quantity"] - fact["cancelled_quantity"]
    fact["net_revenue_local"] = fact["gross_revenue_local"] - fact["cancelled_revenue_local"]

    return fact

def main():
    # TODO: stg read
    stg_amazon = pd.read_csv(AMAZON_STG_PATH)
    stg_international = pd.read_csv(INTL_STG_PATH)

    fact = build_sales_fact(stg_amazon, stg_international)

    # TODO: save
    fact.to_csv(OUT_FACT_PATH, index=False)
    total_stg_qty = (
    stg_amazon["quantity"].sum() +
    stg_international["quantity"].sum()
)
    total_fact_qty = fact["gross_quantity"].sum()
    stg_cancel_qty = (
        stg_amazon.loc[stg_amazon["status"] == "CANCELLED", "quantity"].sum() +
        stg_international.loc[stg_international["status"] == "CANCELLED", "quantity"].sum()
    )


    print("saved:", OUT_FACT_PATH, "rows:", len(fact))
    print("staging total quantity:", total_stg_qty)
    print("fact total gross_quantity:", total_fact_qty)
    print("quantity match:", total_stg_qty == total_fact_qty)

    print("staging cancelled quantity:", stg_cancel_qty)
    print("fact cancelled quantity:", fact["cancelled_quantity"].sum())

    print("PK duplicated?:", fact.duplicated(["sale_date","sku"]).any())

if __name__ == "__main__":
    main()