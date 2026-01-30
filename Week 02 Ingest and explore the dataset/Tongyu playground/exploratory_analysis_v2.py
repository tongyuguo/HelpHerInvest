import time
import requests
import pandas as pd
import yfinance as yf


## PARAMETERS ##
## CHANGE OUTPUT PATH ##
output_dir = "/Users/christoomey/Desktop/Courses/ADAN8888_Applied_Analytics_Project"
output_file = "/stock_symbols.csv"
output_path = output_dir + output_file

SEC_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_HEADERS = {"User-Agent": "YourAppName your_email@example.com"}  # required by SEC


def get_universe_from_sec(limit):
    r = requests.get(SEC_URL, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()
    df = pd.DataFrame(j["data"], columns=j["fields"])
    df = df.rename(columns={"ticker": "symbol", "name": "company_name"})
    df["symbol"] = df["symbol"].str.upper()
    return df[["symbol", "company_name"]].drop_duplicates()

def yf_fetch_info(symbol: str) -> dict:
    # Normalize common Yahoo symbol formatting
    # BRK-B on SEC often needs BRK-B or BRK.B depending; yfinance likes BRK-B *sometimes* but BRK.B often works.
    # We'll try a small fallback.
    candidates = [symbol, symbol.replace("-", ".")]
    for sym in candidates:
        try:
            t = yf.Ticker(sym)
            info = t.get_info()  # yfinance >= 0.2.0 style
            if info and isinstance(info, dict) and info.get("quoteType") in ("EQUITY", "ETF"):
                return info
        except Exception:
            pass

    return info

def build_base_table(limit, sleep_s=0.35):
    universe = get_universe_from_sec(limit=limit)
    print("Symbols pulled:",len(universe.index))
    rows = []
    count = 0
    for sym in universe["symbol"].tolist():
        rows.append(yf_fetch_info(sym))
        count += 1
        time.sleep(sleep_s)  # throttle to avoid Yahoo blocks
        if count % 200 == 0:
            print("{} rows completed".format(count))

    facts = pd.DataFrame(rows)

    df_cols = list(facts.columns)
    added = ["symbol", "company_name"]
    cols = df_cols + added

    base = (
        universe
        .merge(facts, on="symbol", how="left")
        [cols]
        .drop_duplicates(subset=["symbol"])
    )

    return base

df_base = build_base_table(limit=1000, sleep_s=0.35)
#universe = get_universe_from_sec(limit=2000)
#print(universe)
#df_base = pd.DataFrame()

print(df_base.head(10))
print(df_base.shape)
print(df_base.columns)

## CHANGE THE OUTPUT PATH ##

df_base.to_csv(output_path,index=False)
df = pd.read_csv(output_path)

grouped = df.groupby("sector")["sector"].count()
print(grouped)