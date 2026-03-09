import os
import pandas as pd

raw_dir = "data/raw"
stg_dir = "data/staging"

amazon_file = "Amazon Sale Report.csv"
intl_file = "International sale Report.csv"


def standardize_common(out: pd.DataFrame) -> pd.DataFrame:
    # 날짜
    out["sale_date"] = pd.to_datetime(out["sale_date"], errors="coerce").dt.date

    # sku
    out["sku"] = out["sku"].astype(str).str.strip().str.upper()

    # quantity (콤마 있을 수도 있으니 제거 후 숫자 변환)
    out["quantity"] = (
        out["quantity"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    out["quantity"] = pd.to_numeric(out["quantity"].astype(str).str.replace(",","",regex=False),errors='coerce').fillna(0).astype(int)

    # revenue_local (콤마 제거 후 숫자 변환)
    out["revenue_local"] = (
        out["revenue_local"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    out["revenue_local"] = pd.to_numeric(out["revenue_local"], errors="coerce")

    # currency / status / channel
    out["currency"] = out["currency"].astype(str).str.strip().str.upper()
    out["status"] = out["status"].astype(str).str.strip()
    out["channel"] = out["channel"].astype(str).str.strip().str.lower()

    # 컬럼 순서 고정 (항상 같은 스키마)
    out = out[["sale_date", "sku", "quantity", "revenue_local", "currency", "status", "channel"]]
    return out


def build_stg_amazon_sales() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(raw_dir, amazon_file))

    out = pd.DataFrame()
    out["sale_date"] = df["Date"]
    out["sku"] = df["SKU"]
    out["quantity"] = df["Qty"]
    out["revenue_local"] = df["Amount"]
    out["currency"] = df["currency"]
    out["status"] = df["Status"]
    out["channel"] = "amazon"

    return standardize_common(out)


def build_stg_international_sales() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(raw_dir, intl_file))

    # International 파일 컬럼:
    # ['index', 'DATE', 'Months', 'CUSTOMER', 'Style', 'SKU', 'Size', 'PCS', 'RATE', 'GROSS AMT']
    out = pd.DataFrame()
    out["sale_date"] = df["DATE"]
    out["sku"] = df["SKU"]
    out["quantity"] = df["PCS"]
    out["revenue_local"] = df["GROSS AMT"]

    # 이 파일에는 currency/status가 없으니 기본값 넣기
    out["currency"] = "UNKNOWN"
    out["status"] = "UNKNOWN"
    out["channel"] = "international"

    return standardize_common(out)


def main():
    os.makedirs(stg_dir, exist_ok=True)

    stg_amz = build_stg_amazon_sales()
    stg_amz.to_csv(os.path.join(stg_dir, "stg_amazon_sales.csv"), index=False)
    print(stg_amz.head(3))
    print("saved stg_amazon_sales.csv")

    stg_intl = build_stg_international_sales()
    stg_intl.to_csv(os.path.join(stg_dir, "stg_international_sales.csv"), index=False)
    print(stg_intl.head(3))
    print("saved stg_international_sales.csv")


if __name__ == "__main__":
    main()