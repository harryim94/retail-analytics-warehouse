import os
import pandas as pd

mart_dir = 'data/mart'
os.makedirs(mart_dir, exist_ok=True)

fact_sale_path = os.path.join(mart_dir,'sales_fact.csv')
out_fact_path = os.path.join(mart_dir,'dim_date.csv')


def build_dim_date(fact: pd.DataFrame) -> pd.DataFrame:

    # 1) sale_date만 가져오기
    # TODO
    dim_date = fact[["sale_date"]].copy()
    # 2) 중복 제거
    # TODO
    dim_date = dim_date.drop_duplicates()
    # 3) datetime 변환
    # TODO
    dim_date["sale_date"] = pd.to_datetime(dim_date["sale_date"], errors = 'coerce')
    # 4) year, month 추출
    # TODO
    dim_date["year"] = dim_date["sale_date"].dt.year
    dim_date["month"] = dim_date["sale_date"].dt.month
    # 5) weekday 추출 (0=Monday)
    # TODO
    dim_date["weekday"] = dim_date["sale_date"].dt.weekday # 0 = mon
    # 6) is_weekend (토=5, 일=6)
    # TODO
    dim_date["is_weekend"] = dim_date["weekday"] >= 5

    dim_date = dim_date.sort_values("sale_date").reset_index(drop=True)
    return dim_date

def main():
    sales_mart = pd.read_csv(fact_sale_path)

    dim_date = build_dim_date(sales_mart)
    dim_date.to_csv(out_fact_path, index=False)

    print(dim_date.head())
    print(dim_date.dtypes)

if __name__ == "__main__":
    main()    