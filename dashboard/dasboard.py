import streamlit as st
import pandas as pd
import boto3
from pyathena import connect
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYSE Market Intelligence",
    page_icon="📈",
    layout="wide",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #0a0e1a;
    color: #e2e8f0;
}

.main { background-color: #0a0e1a; }

h1, h2, h3 {
    font-family: 'IBM Plex Mono', monospace;
    color: #f0f4ff;
}

.metric-card {
    background: linear-gradient(135deg, #111827 0%, #1a2235 100%);
    border: 1px solid #1e3a5f;
    border-radius: 8px;
    padding: 20px;
    margin: 8px 0;
}

.metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 28px;
    font-weight: 600;
    color: #38bdf8;
}

.metric-label {
    font-size: 12px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: #38bdf8;
    text-transform: uppercase;
    letter-spacing: 3px;
    margin-bottom: 4px;
}

div[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace;
    color: #38bdf8;
}

div[data-testid="stMetricLabel"] {
    color: #94a3b8;
}

.stTabs [data-baseweb="tab-list"] {
    background-color: #111827;
    border-bottom: 1px solid #1e3a5f;
}

.stTabs [data-baseweb="tab"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    color: #64748b;
}

.stTabs [aria-selected="true"] {
    color: #38bdf8 !important;
    border-bottom: 2px solid #38bdf8;
}
</style>
""", unsafe_allow_html=True)

# ── Athena connection ─────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    conn = connect(
        s3_staging_dir="s3://nyse-market-pipeline-athena-results/",
        region_name="eu-north-1",
        schema_name="nyse_analytics",
    )

    sector_df = pd.read_sql(
        "SELECT * FROM mart_sector_performance ORDER BY avg_risk_adj_return DESC",
        conn
    )

    fundamentals_df = pd.read_sql(
        "SELECT * FROM mart_fundamentals_vs_return ORDER BY stock_year, company_name",
        conn
    )

    return sector_df, fundamentals_df

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<p class="section-header">NYSE Market Intelligence Pipeline</p>', unsafe_allow_html=True)
st.title("Market Performance Dashboard")
st.markdown("*Which sectors deliver the best risk-adjusted returns — and do strong fundamentals predict outperformance?*")
st.divider()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Querying Athena..."):
    try:
        sector_df, fundamentals_df = load_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Athena connection failed: {e}")
        data_loaded = False

if not data_loaded:
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

best_sector = sector_df.iloc[0]
worst_sector = sector_df.iloc[-1]

with col1:
    st.metric("Best Sector (Risk-Adj)", best_sector["sector"],
              f"{best_sector['avg_risk_adj_return']:.2f} Sharpe proxy")
with col2:
    st.metric("Best Avg Return", f"{sector_df['avg_yearly_return'].max():.1%}",
              sector_df.loc[sector_df['avg_yearly_return'].idxmax(), 'sector'])
with col3:
    st.metric("Lowest Volatility", f"{sector_df['avg_volatility'].min():.2f}",
              sector_df.loc[sector_df['avg_volatility'].idxmin(), 'sector'])
with col4:
    st.metric("Companies Tracked", f"{fundamentals_df['company_name'].nunique():,}")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊  Sector Performance", "🔬  Fundamentals vs Return", "📋  Raw Data"])

# ── Tab 1: Sector Performance ─────────────────────────────────────────────────
with tab1:
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Risk-Adjusted Return by Sector")
        fig = px.bar(
            sector_df.sort_values("avg_risk_adj_return"),
            x="avg_risk_adj_return",
            y="sector",
            orientation="h",
            color="avg_risk_adj_return",
            color_continuous_scale=["#1e3a5f", "#38bdf8", "#7dd3fc"],
            labels={"avg_risk_adj_return": "Risk-Adj Return", "sector": ""},
        )
        fig.update_layout(
            paper_bgcolor="#0a0e1a",
            plot_bgcolor="#111827",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
            coloraxis_showscale=False,
            margin=dict(l=0, r=20, t=20, b=20),
            height=420,
            xaxis=dict(gridcolor="#1e3a5f", zerolinecolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Return vs Volatility")
        fig2 = px.scatter(
            sector_df,
            x="avg_volatility",
            y="avg_yearly_return",
            text="sector",
            size_max=14,
            color="avg_risk_adj_return",
            color_continuous_scale=["#1e3a5f", "#38bdf8"],
            labels={
                "avg_volatility": "Avg Volatility (daily range $)",
                "avg_yearly_return": "Avg Yearly Return",
                "avg_risk_adj_return": "Risk-Adj"
            },
        )
        fig2.update_traces(textposition="top center", textfont=dict(size=9, color="#94a3b8"))
        fig2.update_layout(
            paper_bgcolor="#0a0e1a",
            plot_bgcolor="#111827",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
            coloraxis_showscale=False,
            margin=dict(l=0, r=20, t=20, b=20),
            height=420,
            xaxis=dict(gridcolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f", tickformat=".0%"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Sector table
    st.subheader("Sector Summary Table")
    display_df = sector_df.copy()
    display_df["avg_yearly_return"] = display_df["avg_yearly_return"].map("{:.1%}".format)
    display_df["avg_volatility"] = display_df["avg_volatility"].map("{:.2f}".format)
    display_df["avg_risk_adj_return"] = display_df["avg_risk_adj_return"].map("{:.3f}".format)
    display_df.columns = ["Sector", "Avg Yearly Return", "Avg Volatility", "Risk-Adj Return"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ── Tab 2: Fundamentals vs Return ─────────────────────────────────────────────
with tab2:
    st.subheader("Do Strong Fundamentals Predict Outperformance?")

    col_f1, col_f2 = st.columns(2)

    with col_f1:
        st.markdown("**EPS vs Yearly Return**")
        fig3 = px.scatter(
            fundamentals_df[fundamentals_df["eps"].between(-50, 50)],
            x="eps",
            y="yearly_return",
            color="gross_margin",
            color_continuous_scale=["#1e3a5f", "#0ea5e9", "#7dd3fc"],
            opacity=0.6,
            labels={
                "eps": "Earnings Per Share",
                "yearly_return": "Yearly Return",
                "gross_margin": "Gross Margin"
            },
            hover_data=["company_name", "stock_year"],
        )
        fig3.update_layout(
            paper_bgcolor="#0a0e1a",
            plot_bgcolor="#111827",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
            margin=dict(l=0, r=20, t=20, b=20),
            height=380,
            xaxis=dict(gridcolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f", tickformat=".0%"),
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col_f2:
        st.markdown("**Debt-to-Equity vs Yearly Return**")
        fig4 = px.scatter(
            fundamentals_df[fundamentals_df["debt_to_equity"].between(0, 5)],
            x="debt_to_equity",
            y="yearly_return",
            color="yearly_return",
            color_continuous_scale=["#ef4444", "#94a3b8", "#22c55e"],
            color_continuous_midpoint=0,
            opacity=0.6,
            labels={
                "debt_to_equity": "Debt-to-Equity Ratio",
                "yearly_return": "Yearly Return",
            },
            hover_data=["company_name", "stock_year"],
        )
        fig4.update_layout(
            paper_bgcolor="#0a0e1a",
            plot_bgcolor="#111827",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
            coloraxis_showscale=False,
            margin=dict(l=0, r=20, t=20, b=20),
            height=380,
            xaxis=dict(gridcolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f", tickformat=".0%"),
        )
        st.plotly_chart(fig4, use_container_width=True)

    # Year filter
    st.subheader("Top Performers by Year")
    years = sorted(fundamentals_df["stock_year"].dropna().unique().tolist())
    selected_year = st.select_slider("Select Year", options=years, value=years[-1])

    top_year = (
        fundamentals_df[fundamentals_df["stock_year"] == selected_year]
        .nlargest(10, "yearly_return")[["company_name", "yearly_return", "eps", "gross_margin", "debt_to_equity"]]
    )
    top_year["yearly_return"] = top_year["yearly_return"].map("{:.1%}".format)
    top_year.columns = ["Company", "Yearly Return", "EPS", "Gross Margin", "Debt/Equity"]
    st.dataframe(top_year, use_container_width=True, hide_index=True)

# ── Tab 3: Raw Data ────────────────────────────────────────────────────────────
with tab3:
    st.subheader("mart_sector_performance")
    st.dataframe(sector_df, use_container_width=True, hide_index=True)

    st.subheader("mart_fundamentals_vs_return")
    st.dataframe(fundamentals_df, use_container_width=True, hide_index=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    '<p style="font-family: IBM Plex Mono; font-size: 11px; color: #334155; text-align: center;">'
    'NYSE Market Intelligence Pipeline · Stack: Airflow · Spark · dbt · Athena · S3 · Streamlit'
    '</p>',
    unsafe_allow_html=True
)