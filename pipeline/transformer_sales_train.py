# =========================
# 목표: raw CSV -> staging CSV 2개 생성
# 1) Amazon Sale Report.csv  -> stg_amazon_sales.csv
# 2) International sale Report.csv -> stg_international_sales.csv
#
# staging 스키마(컬럼/타입/정규화 규칙):
#   sale_date      : 날짜(date)
#   sku            : 문자열, 공백 제거, 대문자
#   quantity       : 정수(int), 콤마 제거 후 숫자 변환, 결측은 0
#   revenue_local  : 숫자(float), 콤마 제거 후 숫자 변환
#   currency       : 문자열, 공백 제거, 대문자 (없으면 "UNKNOWN")
#   status         : 문자열, 공백 제거 (없으면 "UNKNOWN")
#   channel        : 문자열, 공백 제거, 소문자 ("amazon"/"international")
# =========================

# 0) import
# - os: 경로 join, 폴더 생성
# - pandas: csv 읽기/쓰기, 데이터 정리
import os
import pandas as pd
# 1) 상수(경로/파일명) 정의
raw_dir = "data/raw"
stg_dir = "data/staging"
amazon_file = "Amazon Sale Report.csv"
intl_file = "International sale Report.csv"
# raw_dir = "data/raw"
# stg_dir = "data/staging"
# amazon_file = "Amazon Sale Report.csv"
# intl_file = "International sale Report.csv"


# 2) 공통 정리 함수: standardize_common(out)
# 목적:
# - build_stg_* 함수가 만든 out DataFrame을 "항상 같은 형태"로 정규화
#
# 해야 할 일(순서 추천):
# (1) sale_date: datetime 변환 -> date만 남기기 (errors="coerce")
# (2) sku: str로 변환 -> strip -> upper
# (3) quantity: str로 변환 -> 콤마 제거 -> numeric 변환 -> NaN은 0 -> int
# (4) revenue_local: str로 변환 -> 콤마 제거 -> numeric 변환 (float)
# (5) currency/status/channel: str로 변환 -> strip -> (currency upper, channel lower)
# (6) 컬럼 순서 고정: ["sale_date","sku","quantity","revenue_local","currency","status","channel"]
#
# TODO: def standardize_common(out: pd.DataFrame) -> pd.DataFrame:
#   ... return out
def standardize_common(out: pd.DataFrame) -> pd.DataFrame:
    out["sale_date"] = pd.to_datetime(out["sale_date"],errors="coerce").dt.date
    out["sku"] = out["sku"].astype(str).str.strip().str.upper()
    out["quantity"] = (
        pd.to_numeric(
            out["quantity"].astype(str).str.replace(",", "", regex=False),
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )
    out["revenue_local"] = (pd.to_numeric(out["revenue_local"].astype(str).str.replace(",","",regex = False),errors="coerce"))
    out["currency"] = (out["currency"].fillna("UNKNWON").astype(str).str.strip().str.upper())
    out["status"] = (out["status"].fillna("UNKNOWN").astype(str).str.strip().str.upper())
    out["channel"] = out["channel"].astype(str).str.strip().str.lower()

    cols = ["sale_date","sku","quantity","revenue_local","currency","status","channel"]
    out = out[cols]

    return out
# 3) Amazon 변환 함수: build_stg_amazon_sales()
# 목적:
# - Amazon raw csv를 읽어서 staging 스키마로 "매핑"하고,
# - standardize_common으로 정규화한 뒤,
# - DataFrame을 리턴한다.
#
# 입력:
# - raw_dir/amazon_file 경로의 CSV
#
# Amazon raw 컬럼(너 파일 기준):
# - Date, SKU, Qty, Amount, currency, Status
#
# out 매핑 규칙:
# - out["sale_date"] = df["Date"]
# - out["sku"] = df["SKU"]
# - out["quantity"] = df["Qty"]
# - out["revenue_local"] = df["Amount"]
# - out["currency"] = df["currency"]
# - out["status"] = df["Status"]
# - out["channel"] = "amazon" (상수)
#
# TODO: def build_stg_amazon_sales() -> pd.DataFrame:
#   1) df = pd.read_csv(os.path.join(raw_dir, amazon_file))
#   2) out = pd.DataFrame()
#   3) 컬럼 매핑
#   4) return standardize_common(out)
def build_stg_amazon_sales() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(raw_dir,amazon_file))
    out = pd.DataFrame()
    out["sale_date"] = df["Date"]
    out["sku"] = df["SKU"]
    out["quantity"] = df["Qty"]
    out["revenue_local"] = df["Amount"]
    out["currency"] = df["currency"]
    out["status"] = df["Status"]
    out["channel"] = "amazon"

    return standardize_common(out)

# 4) International 변환 함수: build_stg_international_sales()
# 목적:
# - International raw csv를 읽어서 staging 스키마로 매핑하고,
# - currency/status가 없으니 기본값을 채우고,
# - standardize_common으로 정규화한 뒤 리턴한다.
#
# International raw 컬럼(에러에서 확인됨):
# ['index', 'DATE', 'Months', 'CUSTOMER', 'Style', 'SKU', 'Size', 'PCS', 'RATE', 'GROSS AMT']
#
# out 매핑 규칙:
# - out["sale_date"] = df["DATE"]
# - out["sku"] = df["SKU"]
# - out["quantity"] = df["PCS"]
# - out["revenue_local"] = df["GROSS AMT"]
# - out["currency"] = "UNKNOWN"
# - out["status"] = "UNKNOWN"
# - out["channel"] = "international"
#
# TODO: def buwild_stg_international_sales() -> pd.DataFrame:
#   1) df = pd.read_csv(os.path.join(raw_dir, intl_file))
#   2) out = pd.DataFrame()
#   3) 컬럼 매핑 + 기본값
#   4) return standardize_common(out)
def buwild_stg_international_sales() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(raw_dir,intl_file))
    date_from_date = pd.to_datetime(df["DATE"], errors="coerce", format="%m-%d-%y")
    date_from_months = pd.to_datetime(df["Months"], errors="coerce", format="%m-%d-%y")
    sale_date = date_from_date.fillna(date_from_months)
    sku = df["SKU"].fillna(df["Style"])
    qty = pd.to_numeric(df["PCS"], errors="coerce").fillna(pd.to_numeric(df["Size"], errors="coerce"))
    rev = pd.to_numeric(df["GROSS AMT"], errors="coerce").fillna(pd.to_numeric(df["RATE"], errors="coerce"))


    out = pd.DataFrame()
    out["sale_date"] = sale_date
    out["sku"] = sku
    out["quantity"] = qty
    out["revenue_local"] = rev
    out["currency"] = "UNKNOWN"
    out["status"] = "UNKNOWN"
    out["channel"] = "international"
    
    out = out[out["sale_date"].notna() & out["sku"].notna() & out["revenue_local"].notna()]

    return standardize_common(out)

# 5) main()
# 목적:
# - staging 폴더 생성
# - amazon/int'l 변환 함수 호출해서 DataFrame 만들기
# - 각각 CSV로 저장
# - head(3) 출력으로 확인
#
# 해야 할 일:
# (1) os.makedirs(stg_dir, exist_ok=True)
# (2) stg_amz = build_stg_amazon_sales()
# (3) stg_amz.to_csv(os.path.join(stg_dir,"stg_amazon_sales.csv"), index=False)
# (4) print(stg_amz.head(3)); print 저장 메시지
# (5) stg_intl = build_stg_international_sales()
# (6) stg_intl.to_csv(os.path.join(stg_dir,"stg_international_sales.csv"), index=False)
# (7) print(stg_intl.head(3)); print 저장 메시지
#
# TODO: def main():
#   ...
def main():
    os.makedirs(stg_dir,exist_ok = True)
    stg_amz = build_stg_amazon_sales()
    stg_amz.to_csv(os.path.join(stg_dir,"stg_amazon_sales.csv"), index=False)
    print(stg_amz.head(3))
    stg_intl = buwild_stg_international_sales()
    stg_intl.to_csv(os.path.join(stg_dir,"stg_international_sales.csv"), index=False)
    print(stg_intl.head(3))


# 6) 파이썬 실행 엔트리포인트
# - 이 파일을 직접 실행할 때만 main() 실행
#
# TODO:
# if __name__ == "__main__":
#     main()
if __name__ == "__main__":
    main()