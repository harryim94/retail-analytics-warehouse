import os
import pandas as pd

mart_dir = 'data/mart'
out_dir = 'data/kpi'
os.makedirs(out_dir,exist_ok=True)

def build_daily_kpi(Fact:pd.DataFrame) -> pd.DataFrame:
    df = Fact[["sale_date","net_quantity","net_revenue_local"]].copy()
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = (
        df.groupby("sale_date", as_index=False)
        .agg({
            "net_quantity": "sum",
            "net_revenue_local": "sum"
        })
        .rename(columns={
            "net_quantity": "total_net_quantity",
            "net_revenue_local": "total_net_revenue"
        })
    )
    return df

def build_top_10_kpi(Fact:pd.DataFrame) -> pd.DataFrame:
    df = Fact[["sku","net_revenue_local"]].copy()
    df = (df.groupby("sku", as_index=False)
          .agg({
              "net_revenue_local":"sum"
          })
          .rename(columns={
              "net_revenue_local":"total_revenue_local"
          })
    ).sort_values(by="total_revenue_local",ascending=False)
    df_top_10_revenue = df.nlargest(10,"total_revenue_local")
    return df_top_10_revenue

def build_cancel_rate_kpi(Fact:pd.DataFrame) -> pd.DataFrame:
    df=Fact[["sku","gross_quantity","cancelled_quantity","cancelled_revenue_local","gross_revenue_local"]].copy()
    df = (df.groupby("sku",as_index=False)
          .agg({
              "gross_quantity":"sum",
              "cancelled_quantity":"sum",
              "cancelled_revenue_local":"sum",
              "gross_revenue_local":"sum"
          })
    )
    
    df["cancel_rate_qty"] = df["cancelled_quantity"].div(df["gross_quantity"]).fillna(0)
    df["cancel_rate_revenue"] = df["cancelled_revenue_local"].div(df["gross_revenue_local"]).fillna(0)
    return df

def build_monthly_trend_kpi(Fact:pd.DataFrame) -> pd.DataFrame:
    df= Fact[["sale_date","net_revenue_local"]].copy()
    df["sale_date"] = pd.to_datetime(df["sale_date"],errors='coerce')
    df["year_month"] = df["sale_date"].dt.to_period("M").astype(str)   
    df = (df.groupby("year_month",as_index=False)
          .agg({
              "net_revenue_local":"sum"
          })
          .rename(columns={
              "net_revenue_local":"total_net_revenue"
          })
    ).sort_values("year_month")

    return df
def main():
    sales = pd.read_csv(os.path.join(mart_dir,"sales_fact.csv"))
    #dailykpi
    dailykpi = build_daily_kpi(sales)
    dailykpi.to_csv(os.path.join(out_dir,"Daily_KPI.csv"), index= False)
    print(dailykpi.head())
    print(dailykpi.dtypes)
    #top10 product
    top10 = build_top_10_kpi(sales)
    top10.to_csv(os.path.join(out_dir,"top10_product.csv"),index=False)
    print(top10.head())
    #cancel rate kpi
    cancel = build_cancel_rate_kpi(sales)
    cancel.to_csv(os.path.join(out_dir,"cancel_rate_kpi.csv"),index=False)
    print(cancel.head())
    #monthly trend
    monthly = build_monthly_trend_kpi(sales)
    monthly.to_csv(os.path.join(out_dir,"monthly_trend_kpi.csv"),index=False)
    print(monthly.head())

if __name__ == "__main__":
    main()