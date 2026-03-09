import pandas as pd
import os

mart_dir = 'data/mart'
os.makedirs(mart_dir,exist_ok=True)

fact_sale_path = os.path.join(mart_dir,'sales_fact.csv')
out_path = os.path.join(mart_dir,'dim_product.csv')

def build_dim_product(fact: pd.DataFrame) -> pd.DataFrame:
    dim_product = fact[["sku"]].copy()
    dim_product = dim_product.drop_duplicates()
    dim_product = dim_product.sort_values("sku").reset_index(drop=True)

    return dim_product

def main():
    sale_mart = pd.read_csv(fact_sale_path)
    dim_product = build_dim_product(sale_mart)
    
    dim_product.to_csv(out_path,index=False)

    print(dim_product.head(3))
    print(dim_product.dtypes)

if __name__ == "__main__":
    main()  