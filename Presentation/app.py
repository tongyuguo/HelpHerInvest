"""
Stock Recommendation System — Streamlit App
Input: user investment thesis (free text)
Output: top ranked stocks from NLP + LightGBM pipeline
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(page_title="Stock Recommender", page_icon="📈", layout="wide")

# ── Minimal CSS (no external font dependencies that could break) ───────────────
st.markdown("""
<style>
.title  { font-size: 2rem; font-weight: 800; color: #00e5b0; margin-bottom: 0.2rem; }
.sub    { font-size: 0.8rem; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 1.5rem; }
.card   { background: #111827; border: 1px solid #1f2937; border-radius: 10px;
          padding: 0.8rem 1.1rem; margin-bottom: 0.45rem; display: flex;
          align-items: center; gap: 1rem; }
.c-rank { font-weight: 800; font-size: 1rem; color: #6b7280; min-width: 2.5rem; }
.c-tick { font-weight: 700; font-size: 1.05rem; color: #f9fafb; min-width: 5rem; }
.c-scr  { font-size: 0.82rem; font-family: monospace; color: #00e5b0;
          background: rgba(0,229,176,0.1); border-radius: 5px; padding: 2px 8px; }
.c-bar  { flex: 1; background: #1f2937; border-radius: 4px; height: 5px; }
.c-fill { height: 5px; border-radius: 4px;
          background: linear-gradient(90deg, #00e5b0, #4f8ef7); }
.stApp  { background: #0a0f1a; color: #e5e7eb; }
section[data-testid="stSidebar"] { background: #0f172a; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar: settings ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    st.divider()

    openai_key = st.text_input("OpenAI API Key", type="password", placeholder="sk-...")

    st.divider()
    st.markdown("**File Paths**")
    model_path = st.text_input(
        "LightGBM model (.pkl)",
        value="HelpHerInvest/artifacts/lgbm_model.pkl",
    )
    data_path = st.text_input(
        "Dataset (.csv or .csv.zip)",
        value="HelpHerInvest/Data/final_dataset_20260224v2.csv.zip",
    )

    st.divider()
    st.markdown("**Output**")
    top_n    = st.slider("Top N stocks", min_value=5, max_value=15, value=10)
    nlp_pool = st.slider("NLP candidate pool", min_value=20, max_value=50, value=30, step=5)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="title">📈 Stock Recommender</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">NLP ticker selection · LightGBM ranking · fwd_excess prediction</div>', unsafe_allow_html=True)

# ── Input ─────────────────────────────────────────────────────────────────────
st.markdown("### Your investment thesis")

# Quick-fill example buttons
examples = [
    "I want to invest in sustainable technology companies",
    "High-growth healthcare and biotech stocks",
    "Defensive consumer staples with strong dividends",
    "AI and semiconductor companies",
]
cols = st.columns(len(examples))
for col, ex in zip(cols, examples):
    with col:
        if st.button(ex, use_container_width=True):
            st.session_state["prompt"] = ex
            st.rerun()

user_prompt = st.text_area(
    label="prompt",
    value=st.session_state.get("prompt", ""),
    placeholder="Describe the types of companies or sectors you want to invest in...",
    height=90,
    label_visibility="collapsed",
)

run_clicked = st.button("🔍 Find Stocks", type="primary", use_container_width=False)

if not openai_key:
    st.info("💡 Add your OpenAI API key in the sidebar to run the recommender.")

# ── Pipeline ──────────────────────────────────────────────────────────────────
if run_clicked:
    if not user_prompt.strip():
        st.error("Please enter an investment thesis.")
        st.stop()
    if not openai_key:
        st.error("OpenAI API key required — add it in the sidebar.")
        st.stop()

    with st.status("Running recommendation pipeline...", expanded=True) as status:
        try:
            st.write("⏳ Loading dataset and model...")
            from models import load_pipeline, run_pipeline

            # Cache the heavy load so re-runs are fast
            @st.cache_resource(show_spinner=False)
            def get_pipeline(mp, dp):
                return load_pipeline(model_path=mp, data_path=dp)

            pipeline = get_pipeline(model_path, data_path)

            st.write("🧠 NLP model selecting candidate tickers...")
            st.write("📊 LightGBM scoring and ranking candidates...")

            results = run_pipeline(
                pipeline=pipeline,
                user_input=user_prompt,
                openai_api_key=openai_key,
                top_n=top_n,
                nlp_pool=nlp_pool,
            )

            status.update(label="✅ Done!", state="complete", expanded=False)
            st.session_state["results"] = results
            st.session_state["prompt_used"] = user_prompt

        except Exception as err:
            status.update(label="❌ Pipeline failed", state="error")
            st.error(str(err))
            with st.expander("Troubleshooting"):
                st.markdown("""
**Common fixes:**
- Make sure `models.py` is in the **same folder** as `app.py`
- Check the **model path** (`.pkl` file) and **dataset path** in the sidebar
- Confirm your **OpenAI API key** is valid
- Install dependencies: `pip install -r requirements.txt`
                """)
            st.stop()

# ── Results ───────────────────────────────────────────────────────────────────
if "results" in st.session_state:
    results: pd.DataFrame = st.session_state["results"]
    prompt_used = st.session_state.get("prompt_used", "")

    st.divider()

    # KPI metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stocks Found",   len(results))
    c2.metric("Avg Confidence", f"{results['score'].mean():.1%}")
    c3.metric("Top Score",      f"{results['score'].max():.1%}")
    c4.metric("Bullish (>50%)", f"{(results['score'] > 0.5).sum()} of {len(results)}")

    st.caption(f'Prompt: *"{prompt_used}"*')
    st.markdown("---")

    # Two columns: ranked cards | bar chart
    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown("#### Top Recommendations")
        max_score = results["score"].max()
        for _, row in results.iterrows():
            pct = int((row["score"] / max_score) * 100)
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(int(row["rank"]), f"#{int(row['rank'])}")
            st.markdown(f"""
            <div class="card">
              <div class="c-rank">{medal}</div>
              <div class="c-tick">{row['Ticker']}</div>
              <div class="c-scr">{row['score']:.1%}</div>
              <div class="c-bar"><div class="c-fill" style="width:{pct}%"></div></div>
            </div>""", unsafe_allow_html=True)

    with right:
        st.markdown("#### Confidence Scores")
        fig = go.Figure(go.Bar(
            x=results["score"].values,
            y=results["Ticker"].values,
            orientation="h",
            marker=dict(
                color=results["score"].values,
                colorscale=[[0, "#1f2937"], [0.5, "#4f8ef7"], [1, "#00e5b0"]],
                showscale=False,
            ),
            text=[f"{s:.1%}" for s in results["score"].values],
            textposition="outside",
            textfont=dict(size=11, color="#e5e7eb"),
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(autorange="reversed", tickfont=dict(color="#e5e7eb", size=12), gridcolor="#1f2937"),
            xaxis=dict(tickformat=".0%", range=[0, min(results["score"].max() * 1.3, 1.0)],
                       gridcolor="#1f2937", tickfont=dict(color="#6b7280", size=10)),
            margin=dict(l=10, r=70, t=10, b=10),
            height=max(280, len(results) * 34),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Full table
    st.markdown("#### Full Results Table")
    display = results.copy()
    display["Confidence"] = display["score"].map("{:.2%}".format)
    display["Signal"] = display["score"].apply(
        lambda s: "🟢 Strong Buy" if s >= 0.65 else ("🔵 Buy" if s >= 0.50 else "⚪ Neutral")
    )
    show = ["rank", "Ticker", "Confidence", "Signal"]
    if "Date" in display.columns:
        display["As-of Date"] = pd.to_datetime(display["Date"]).dt.strftime("%Y-%m-%d")
        show.append("As-of Date")

    st.dataframe(display[show].rename(columns={"rank": "Rank"}), use_container_width=True, hide_index=True)

    st.download_button(
        "⬇️ Download CSV",
        data=results.to_csv(index=False),
        file_name="stock_recommendations.csv",
        mime="text/csv",
    )

# ── Empty state ───────────────────────────────────────────────────────────────
elif not run_clicked:
    st.markdown("""
    <div style="text-align:center; padding:3rem 1rem; color:#6b7280;">
      <div style="font-size:2.5rem; margin-bottom:0.8rem;">📊</div>
      <div style="font-size:1rem; font-weight:600; color:#d1d5db; margin-bottom:0.5rem;">
        Enter your investment thesis above and click Find Stocks
      </div>
      <div style="font-size:0.82rem; line-height:1.9;">
        The NLP model will shortlist thematically relevant tickers from the dataset,<br>
        then LightGBM ranks them by predicted forward excess return.
      </div>
    </div>
    """, unsafe_allow_html=True)
