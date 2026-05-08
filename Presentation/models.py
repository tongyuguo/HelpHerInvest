"""
models.py  —  Pipeline integration layer
=========================================
All logic taken directly from week13_all_together.ipynb.
Two public functions used by app.py:
    load_pipeline(model_path, data_path)  ->  dict
    run_pipeline(pipeline, user_input, openai_api_key, top_n, nlp_pool)  ->  pd.DataFrame
"""

import io
import json
import pickle
import zipfile
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── Constants — must match your dataset ───────────────────────────────────────
DATE_COL   = "Date"
TICKER_COL = "Ticker"
TARGET_COL = "y"

NON_FEATURE_COLS = [
    DATE_COL, TICKER_COL,
    "fwd_excess", "fwd_return", "y", "fwd_rank", "target",
]

RANK_COLS = [
    "mom_1m", "mom_3m", "mom_6m", "mom_12m", "mom_12m_ex_1m",
    "rel_3m_spy", "rel_6m_spy", "rel_12m_spy",
    "vol_3m", "vol_6m",
    "drawdown_6m", "drawdown_12m",
    "pct_above_200dma",
]

OPENAI_MODEL = "gpt-4o-mini"


# ── Data helpers ──────────────────────────────────────────────────────────────

def _load_data(data_path: str) -> pd.DataFrame:
    p = Path(data_path)
    if not p.exists():
        raise FileNotFoundError(f"Dataset not found at: {data_path}")
    if p.suffix == ".zip":
        with zipfile.ZipFile(p) as z:
            csv_names = [n for n in z.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No .csv found inside {data_path}")
            with z.open(csv_names[0]) as f:
                df = pd.read_csv(f)
    else:
        df = pd.read_csv(p)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    return df


def _preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna().copy()
    df["y"] = (df["fwd_excess"] > 0).astype(int)
    df["log_adj_close"] = np.log(df["adj_close"])
    return df


def _apply_cross_sectional_rank(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in RANK_COLS:
        if col in df.columns:
            df[col] = df.groupby(DATE_COL)[col].rank(pct=True)
    df["fwd_rank"] = df.groupby(DATE_COL)["fwd_excess"].rank(pct=True)
    df["target"] = pd.cut(
        df["fwd_rank"], bins=[0, .2, .4, .6, .8, 1.0], labels=[0, 1, 2, 3, 4],
    )
    return df


def _get_feature_cols(df: pd.DataFrame) -> list:
    return sorted([c for c in df.columns if c not in NON_FEATURE_COLS])


def _load_model(model_path: str):
    p = Path(model_path)
    if not p.exists():
        raise FileNotFoundError(f"Model not found at: {model_path}")
    with open(p, "rb") as f:
        return pickle.load(f)


def _create_unique_ticker_df(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[TICKER_COL]
        .dropna().astype(str).str.upper().str.strip()
        .drop_duplicates().sort_values().reset_index(drop=True)
        .to_frame(name=TICKER_COL)
    )


# ── NLP step ──────────────────────────────────────────────────────────────────

def _nlp_select_tickers(
    unique_ticker: pd.DataFrame,
    user_input: str,
    openai_api_key: str,
    n: int = 30,
) -> pd.DataFrame:
    from openai import OpenAI
    client = OpenAI(api_key=openai_api_key)

    valid_tickers = (
        unique_ticker[TICKER_COL]
        .dropna().astype(str).str.upper().str.strip().drop_duplicates()
    )
    valid_ticker_set = set(valid_tickers)

    buf = io.BytesIO()
    valid_tickers.to_frame(name=TICKER_COL).to_csv(buf, index=False)
    buf.seek(0)
    buf.name = "universe.csv"
    uploaded_file = client.files.create(file=buf, purpose="user_data")

    response = client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {
                "role": "developer",
                "content": [{
                    "type": "input_text",
                    "text": (
                        "You are selecting stock tickers for a downstream quantitative model.\n"
                        "You must only use tickers from the attached CSV file.\n"
                        f"Based on the user's investment interest, return exactly {n} unique tickers.\n"
                        "Rules:\n"
                        "- Only return tickers that exist in the attached file.\n"
                        f"- Return exactly {n} unique tickers.\n"
                        "- Do not include explanations or markdown.\n"
                        f'- Return valid JSON only: {{"tickers": ["AAPL", "MSFT", "..."]}}'
                    ),
                }],
            },
            {
                "role": "user",
                "content": [
                    {"type": "input_file", "file_id": uploaded_file.id},
                    {"type": "input_text", "text": user_input},
                ],
            },
        ],
    )

    try:
        result  = json.loads(response.output_text)
        tickers = result["tickers"]
    except Exception as e:
        raise ValueError(f"OpenAI returned unexpected output:\n{response.output_text}") from e

    cleaned, seen = [], set()
    for t in tickers:
        t = str(t).upper().strip()
        if t in valid_ticker_set and t not in seen:
            cleaned.append(t)
            seen.add(t)

    if len(cleaned) < 5:
        raise ValueError(
            f"Only {len(cleaned)} valid tickers returned (expected ~{n}). "
            "Check your ticker universe and OpenAI key."
        )

    return pd.DataFrame({TICKER_COL: cleaned})


# ── Ranking step ──────────────────────────────────────────────────────────────

def _rank_tickers(
    selected_tickers_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    model,
    feature_cols: list,
    top_n: int = 10,
) -> pd.DataFrame:
    selected = selected_tickers_df.copy()
    selected[TICKER_COL] = selected[TICKER_COL].astype(str).str.upper().str.strip()

    df = feature_df.copy()
    df[TICKER_COL] = df[TICKER_COL].astype(str).str.upper().str.strip()
    df[DATE_COL]   = pd.to_datetime(df[DATE_COL])

    df = df[df[TICKER_COL].isin(set(selected[TICKER_COL]))]
    if df.empty:
        raise ValueError("No matching rows found for selected tickers in the dataset.")

    latest = df.sort_values([TICKER_COL, DATE_COL]).groupby(TICKER_COL, as_index=False).tail(1).copy()
    latest = latest.dropna(subset=feature_cols)
    if latest.empty:
        raise ValueError("All selected tickers dropped due to missing feature values.")

    latest = latest.copy()
    latest["score"] = model.predict_proba(latest[feature_cols])[:, 1]
    ranked = latest.sort_values("score", ascending=False).head(top_n).reset_index(drop=True)
    ranked["rank"] = range(1, len(ranked) + 1)

    return ranked[[TICKER_COL, DATE_COL, "score", "rank"]]


# ── Public API ────────────────────────────────────────────────────────────────

def load_pipeline(model_path: str, data_path: str) -> dict:
    """Load and preprocess the dataset + model. Call once; pass result to run_pipeline."""
    df_raw       = _load_data(data_path)
    df_clean     = _preprocess(df_raw)
    df_rank      = _apply_cross_sectional_rank(df_clean)
    feature_cols = _get_feature_cols(df_rank)
    tickers      = _create_unique_ticker_df(df_rank)
    model        = _load_model(model_path)
    return {
        "df_rank":      df_rank,
        "feature_cols": feature_cols,
        "tickers":      tickers,
        "model":        model,
    }


def run_pipeline(
    pipeline: dict,
    user_input: str,
    openai_api_key: str,
    top_n: int = 10,
    nlp_pool: int = 30,
) -> pd.DataFrame:
    """
    1. NLP  → shortlist `nlp_pool` tickers matching the user's theme
    2. LGBM → score & rank by P(fwd_excess > 0)
    3. Return top `top_n` as DataFrame: Ticker | Date | score | rank
    """
    selected = _nlp_select_tickers(
        unique_ticker=pipeline["tickers"],
        user_input=user_input,
        openai_api_key=openai_api_key,
        n=nlp_pool,
    )
    return _rank_tickers(
        selected_tickers_df=selected,
        feature_df=pipeline["df_rank"],
        model=pipeline["model"],
        feature_cols=pipeline["feature_cols"],
        top_n=top_n,
    )
