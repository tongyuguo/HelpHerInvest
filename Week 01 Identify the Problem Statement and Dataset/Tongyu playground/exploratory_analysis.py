import time
import requests
import pandas as pd
import yfinance as yf


## CHANGE OUTPUT PATH ##

output_dir = "/Users/christoomey/Desktop/Courses/ADAN8888_Applied_Analytics_Project"
output_file = "/stock_symbols.csv"
output_path = output_dir + output_file


## DATA PULL INFO / COLUMNS ##

dat = yf.Ticker("MSFT")
print("Ticker Name Information Columns")
for key,value in dat.info.items():
    print(key)

print("\n")



SEC_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_HEADERS = {"User-Agent": "YourAppName your_email@example.com"}  # required by SEC

def get_universe_from_sec(limit):
    r = requests.get(SEC_URL, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()
    df = pd.DataFrame(j["data"], columns=j["fields"])
    df = df.rename(columns={"ticker": "symbol", "name": "company_name"})
    df["symbol"] = df["symbol"].str.upper()
    return df[["symbol", "company_name", "exchange"]].drop_duplicates().head(limit)

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
                return {
                    "symbol": symbol,  # keep your canonical symbol
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "market_cap": info.get("marketCap"),
                    "price": info.get("currentPrice") or info.get("regularMarketPrice"),
                    "pe_ratio": info.get("trailingPE"),
                    "pb_ratio": info.get("priceToBook"),
                }
        except Exception:
            pass
    return {
        "symbol": symbol,
        "sector": None,
        "industry": None,
        "market_cap": None,
        "price": None,
        "pe_ratio": None,
        "pb_ratio": None,
    }

def build_base_table(limit, sleep_s=0.35):
    universe = get_universe_from_sec(limit=limit)

    rows = []
    for sym in universe["symbol"].tolist():
        rows.append(yf_fetch_info(sym))
        time.sleep(sleep_s)  # IMPORTANT: throttle to avoid Yahoo blocks

    facts = pd.DataFrame(rows)

    base = (
        universe
        .merge(facts, on="symbol", how="left")
        [["symbol", "company_name", "exchange", "sector", "industry",
          "market_cap", "price", "pe_ratio", "pb_ratio"]]
        .drop_duplicates(subset=["symbol"])
    )

    return base

df_base = build_base_table(limit=1000, sleep_s=0.35)
print(df_base.head(10))
print(df_base.shape)
print(df_base.columns)

## CHANGE THE OUTPUT PATH ##

df_base.to_csv(output_path,index=False)

df = pd.read_csv(output_path)

grouped = df.groupby("sector")["sector"].count()
print(grouped)