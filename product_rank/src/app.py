import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
from datetime import timedelta

st.set_page_config(
    page_title="Product Rank Dash",
    page_icon="📊",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=DM+Mono:wght@300;400;500&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,300&display=swap');

/* ── Root & Body ── */
:root {
    --bg-base:       #0d0f14;
    --bg-card:       #13161d;
    --bg-card-alt:   #181c25;
    --bg-hover:      #1e2330;
    --border:        #252b3a;
    --border-bright: #2e3649;
    --accent:        #3d8ef8;
    --accent-dim:    #2563c4;
    --accent-glow:   rgba(61, 142, 248, 0.12);
    --teal:          #22d3c8;
    --amber:         #f5a623;
    --rose:          #f43f5e;
    --green:         #22c55e;
    --text-primary:  #e8ecf4;
    --text-secondary:#8b95aa;
    --text-muted:    #4d5669;
    --radius:        8px;
    --radius-lg:     12px;
}

/* Global font override */
html, body, [class*="css"], .stApp, .stMarkdown, p, span, div, label {
    font-family: 'DM Sans', sans-serif !important;
    color: var(--text-primary);
}

/* App background */
.stApp {
    background-color: var(--bg-base) !important;
    background-image:
        radial-gradient(ellipse 80% 40% at 50% -10%, rgba(61,142,248,0.08) 0%, transparent 60%),
        radial-gradient(ellipse 40% 30% at 90% 80%, rgba(34,211,200,0.04) 0%, transparent 50%);
}

/* Main content area */
.main .block-container {
    padding: 2rem 2.5rem 4rem !important;
    max-width: 1600px !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: var(--bg-card) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] .stMarkdown h1,
[data-testid="stSidebar"] .stMarkdown h2,
[data-testid="stSidebar"] .stMarkdown h3 {
    font-family: 'Syne', sans-serif !important;
    color: var(--text-primary) !important;
    letter-spacing: 0.04em;
}
[data-testid="stSidebar"] .stTitle > * {
    font-family: 'Syne', sans-serif !important;
    font-size: 1.1rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: var(--accent) !important;
}

/* Sidebar labels */
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stDateInput label,
[data-testid="stSidebar"] .stToggle label {
    font-size: 0.7rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
}

/* ── Headings ── */
h1, h2, h3, h4 {
    font-family: 'Syne', sans-serif !important;
    color: var(--text-primary) !important;
}
h1 { font-size: 1.8rem !important; font-weight: 800 !important; letter-spacing: -0.01em !important; }
h2 { font-size: 1.25rem !important; font-weight: 700 !important; letter-spacing: 0.01em !important; }
h3 { font-size: 1rem !important; font-weight: 600 !important; }

/* Title */
[data-testid="stHeading"] h1 {
    background: linear-gradient(135deg, var(--text-primary) 0%, var(--accent) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-size: 2rem !important;
    font-weight: 800 !important;
    letter-spacing: -0.02em !important;
    padding-bottom: 0.1em;
}

/* Caption / helper text */
.stCaptionContainer, [data-testid="stCaptionContainer"], small, caption {
    color: var(--text-secondary) !important;
    font-size: 0.78rem !important;
    line-height: 1.5 !important;
}

/* ── Metric Cards ── */
[data-testid="stMetric"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-lg) !important;
    padding: 1rem 1.25rem !important;
    transition: border-color 0.2s, box-shadow 0.2s !important;
    position: relative;
    overflow: hidden;
}
[data-testid="stMetric"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--teal));
    opacity: 0;
    transition: opacity 0.2s;
}
[data-testid="stMetric"]:hover {
    border-color: var(--border-bright) !important;
    box-shadow: 0 0 0 1px var(--border-bright), 0 4px 20px rgba(0,0,0,0.4) !important;
}
[data-testid="stMetric"]:hover::before {
    opacity: 1;
}
[data-testid="stMetricLabel"] {
    font-size: 0.68rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
    font-family: 'DM Sans', sans-serif !important;
}
[data-testid="stMetricValue"] {
    font-family: 'DM Mono', monospace !important;
    font-size: 1.5rem !important;
    font-weight: 500 !important;
    color: var(--text-primary) !important;
    line-height: 1.2 !important;
}
[data-testid="stMetricDelta"] {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.75rem !important;
}
[data-testid="stMetricDelta"] svg { display: none !important; }

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    border-bottom: 1px solid var(--border) !important;
    gap: 0 !important;
    background: transparent !important;
}
[data-testid="stTabs"] [role="tab"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.07em !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
    padding: 0.6rem 1.25rem !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
    background: transparent !important;
    transition: color 0.15s, border-color 0.15s !important;
}
[data-testid="stTabs"] [role="tab"]:hover {
    color: var(--text-secondary) !important;
    border-bottom-color: var(--border-bright) !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom-color: var(--accent) !important;
    background: transparent !important;
}

/* ── Divider ── */
hr {
    border: none !important;
    border-top: 1px solid var(--border) !important;
    margin: 2rem 0 !important;
}

/* ── Inputs & Selects ── */
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stTextInput > div > div > input,
.stDateInput > div > div > input {
    background-color: var(--bg-card-alt) !important;
    border: 1px solid var(--border-bright) !important;
    border-radius: var(--radius) !important;
    color: var(--text-primary) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.85rem !important;
    transition: border-color 0.15s !important;
}
.stSelectbox > div > div:focus-within,
.stMultiSelect > div > div:focus-within,
.stTextInput > div > div > input:focus,
.stDateInput > div > div > input:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 2px var(--accent-glow) !important;
    outline: none !important;
}

/* Dropdown options */
[data-baseweb="menu"] {
    background-color: var(--bg-card-alt) !important;
    border: 1px solid var(--border-bright) !important;
    border-radius: var(--radius) !important;
}
[data-baseweb="menu"] li {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.85rem !important;
    color: var(--text-primary) !important;
}
[data-baseweb="menu"] li:hover {
    background-color: var(--bg-hover) !important;
}

/* Multiselect tags */
[data-baseweb="tag"] {
    background-color: var(--accent-dim) !important;
    border: none !important;
    border-radius: 4px !important;
    font-size: 0.75rem !important;
}

/* ── Radio Buttons ── */
.stRadio > div {
    gap: 0.5rem !important;
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    display: inline-flex !important;
}
.stRadio label {
    font-size: 0.75rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    padding: 0.3rem 0.85rem !important;
    border-radius: 6px !important;
    cursor: pointer !important;
    color: var(--text-secondary) !important;
    background: transparent !important;
    transition: color 0.15s !important;
}
.stRadio [data-checked="true"] + label,
.stRadio input:checked + div {
    background: transparent !important;
    color: var(--accent) !important;
    font-weight: 600 !important;
}

/* ── Toggle ── */
.stToggle [data-checked] {
    background-color: var(--accent) !important;
}

/* ── Dataframe / Table ── */
[data-testid="stDataFrame"],
.stDataFrame {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-lg) !important;
    overflow: hidden !important;
}
[data-testid="stDataFrame"] thead th {
    background: var(--bg-card-alt) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
    border-bottom: 1px solid var(--border-bright) !important;
    padding: 0.6rem 0.8rem !important;
}
[data-testid="stDataFrame"] tbody td {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.82rem !important;
    color: var(--text-primary) !important;
    border-bottom: 1px solid var(--border) !important;
    padding: 0.5rem 0.8rem !important;
    background: var(--bg-card) !important;
}
[data-testid="stDataFrame"] tbody tr:hover td {
    background: var(--bg-hover) !important;
}

/* ── Buttons ── */
.stButton > button {
    background: var(--accent) !important;
    color: white !important;
    border: none !important;
    border-radius: var(--radius) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.05em !important;
    padding: 0.5rem 1.25rem !important;
    transition: all 0.15s !important;
}
.stButton > button:hover {
    background: var(--accent-dim) !important;
    box-shadow: 0 4px 12px rgba(61,142,248,0.3) !important;
    transform: translateY(-1px) !important;
}

/* ── Subheader styling ── */
[data-testid="stHeading"] h2,
.stMarkdown h2 {
    color: var(--text-primary) !important;
    font-size: 1.1rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.02em !important;
    padding-top: 0.25rem !important;
    padding-bottom: 0.5rem !important;
    border-bottom: 1px solid var(--border) !important;
    margin-bottom: 1rem !important;
}

/* ── Info / Warning boxes ── */
[data-testid="stInfo"] {
    background: rgba(61,142,248,0.08) !important;
    border: 1px solid rgba(61,142,248,0.25) !important;
    border-radius: var(--radius) !important;
    color: var(--accent) !important;
    font-size: 0.85rem !important;
}
[data-testid="stWarning"] {
    background: rgba(245,166,35,0.08) !important;
    border: 1px solid rgba(245,166,35,0.25) !important;
    border-radius: var(--radius) !important;
    color: var(--amber) !important;
}

/* Caption row under title */
[data-testid="stCaptionContainer"] p {
    color: var(--text-muted) !important;
    font-size: 0.78rem !important;
    font-family: 'DM Mono', monospace !important;
    letter-spacing: 0.05em !important;
}

/* ── Section label (bold markdown) ── */
.stMarkdown strong {
    color: var(--text-primary) !important;
    font-weight: 600 !important;
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg-base); }
::-webkit-scrollbar-thumb { background: var(--border-bright); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
</style>
""", unsafe_allow_html=True)

# ── Plotly dark theme ──────────────────────────────────────────────────────────
import plotly.io as pio

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans, sans-serif", color="#8b95aa", size=12),
    xaxis=dict(
        gridcolor="#1e2330",
        linecolor="#252b3a",
        tickcolor="#252b3a",
        zerolinecolor="#252b3a",
    ),
    yaxis=dict(
        gridcolor="#1e2330",
        linecolor="#252b3a",
        tickcolor="#252b3a",
        zerolinecolor="#252b3a",
    ),
    legend=dict(
        bgcolor="rgba(19,22,29,0.8)",
        bordercolor="#252b3a",
        borderwidth=1,
        font=dict(size=11, color="#8b95aa"),
    ),
    colorway=["#3d8ef8", "#22d3c8", "#f5a623", "#f43f5e", "#a78bfa", "#22c55e"],
)

def apply_dark_theme(fig, **extra):
    layout = {**PLOT_LAYOUT, **extra}
    fig.update_layout(**layout)
    return fig

# ── Load data ─────────────────────────────────────────────────────────────────
from rec_query import get_data

@st.cache_data(ttl=86400)  # refreshes every 24 hours
def load_data():
    return get_data()
df_raw = load_data()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Filters")

    if "call_date" in df_raw.columns and df_raw["call_date"].notna().any():
        min_d = df_raw["call_date"].min().date()
        max_d = df_raw["call_date"].max().date()
        default_start = max(min_d, max_d - timedelta(days=6))
        date_range = st.date_input(
            "Date Range",
            value=(default_start, max_d),
            min_value=min_d,
            max_value=max_d,
            key="filter_date_range",
        )
    else:
        date_range = None

    centers_opts  = sorted(df_raw["center_location"].dropna().unique().tolist()) if "center_location" in df_raw.columns else []
    mkt_opts      = sorted(df_raw["marketing_bucket"].dropna().unique().tolist()) if "marketing_bucket" in df_raw.columns else []
    serp_opts     = sorted(df_raw["site_serp"].dropna().unique().tolist()) if "site_serp" in df_raw.columns else []
    mov_opts      = sorted(df_raw["mover_switcher"].dropna().unique().tolist()) if "mover_switcher" in df_raw.columns else []
    quartile_opts = sorted(df_raw["performance_quartile"].dropna().unique().tolist()) if "performance_quartile" in df_raw.columns else []

    # ── NEW: Agent multi-select ───────────────────────────────────────────────
    agent_opts = sorted(df_raw["agent_name"].dropna().unique().tolist()) if "agent_name" in df_raw.columns else []

    center_defaults = [c for c in ["Durban", "Jamaica"] if c in centers_opts]
    sel_center   = st.multiselect("Center",           options=centers_opts,  default=center_defaults, key="filter_center")
    sel_mkt      = st.multiselect("Marketing Bucket", options=mkt_opts,      default=[], key="filter_mkt")
    sel_serp     = st.multiselect("Site / SERP",      options=serp_opts,     default=[], key="filter_serp")
    sel_mov      = st.multiselect("Mover / Switcher", options=mov_opts,      default=[], key="filter_mov")
    sel_quartile = st.multiselect("Agent Quartile",   options=quartile_opts, default=[], key="filter_quartile")

    # Agent filter with search — st.multiselect has built-in search when there are many options
    sel_agent    = st.multiselect(
        "Agent",
        options=agent_opts,
        default=[],
        key="filter_agent",
        placeholder="Search agents…",
    )

    rec_type_opts = sorted(df_raw["top_recommended_plan_type"].dropna().unique().tolist()) if "top_recommended_plan_type" in df_raw.columns else []
    sel_rec_type  = st.multiselect("Rec Product Type", options=rec_type_opts, default=[], key="filter_rec_type")

    happy_only = st.toggle("Happy Path Calls Only", value=True, key="filter_happy_path")

# ── Apply filters ─────────────────────────────────────────────────────────────
def apply_non_date_filters(base):
    d = base.copy()
    if happy_only and "happy_path" in d.columns:
        d = d[d["happy_path"] == 1]
    if sel_center and "center_location" in d.columns:
        d = d[d["center_location"].isin(sel_center)]
    if sel_mkt and "marketing_bucket" in d.columns:
        d = d[d["marketing_bucket"].isin(sel_mkt)]
    if sel_serp and "site_serp" in d.columns:
        d = d[d["site_serp"].isin(sel_serp)]
    if sel_mov and "mover_switcher" in d.columns:
        d = d[d["mover_switcher"].isin(sel_mov)]
    if sel_quartile and "performance_quartile" in d.columns:
        d = d[d["performance_quartile"].isin(sel_quartile)]
    if sel_agent and "agent_name" in d.columns:
        d = d[d["agent_name"].isin(sel_agent)]
    if sel_rec_type and "top_recommended_plan_type" in d.columns:
        d = d[d["top_recommended_plan_type"].isin(sel_rec_type)]
    return d

df_nodatefilter = apply_non_date_filters(df_raw)

df = df_nodatefilter.copy()
if date_range and len(date_range) == 2 and "call_date" in df.columns:
    df = df[(df["call_date"].dt.date >= date_range[0]) & (df["call_date"].dt.date <= date_range[1])]

# ── Shared helpers ────────────────────────────────────────────────────────────
PERIOD_OPTIONS = ["Daily", "Weekly", "Monthly"]
PERIOD_CODE    = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
PERIOD_FMT     = {"Daily": "%b %d", "Weekly": "%b %d", "Monthly": "%b %Y"}

def period_start_dates(date_series: pd.Series, period: str) -> pd.Series:
    code = PERIOD_CODE[period]
    return date_series.dt.to_period(code).apply(lambda p: p.start_time)

def period_labels(date_series: pd.Series, period: str) -> pd.Series:
    return period_start_dates(date_series, period).dt.strftime("%Y-%m-%d")

def period_display(label_series: pd.Series, period: str) -> pd.Series:
    fmt = PERIOD_FMT[period]
    return pd.to_datetime(label_series).dt.strftime(fmt)

def fmt_week(s):
    try:
        return pd.to_datetime(str(s).split("/")[0]).strftime("%b %d")
    except Exception:
        return str(s)

def week_mix(source_df, plan_type):
    if "call_date" not in source_df.columns or "top_recommended_plan_type" not in source_df.columns:
        return None, None
    tmp = source_df.dropna(subset=["call_date", "top_recommended_plan_type"]).copy()
    tmp["week"] = tmp["call_date"].dt.to_period("W")
    weeks = sorted(tmp["week"].unique())
    if len(weeks) < 2:
        return None, None
    this_w, prev_w = weeks[-1], weeks[-2]
    def mix_for(w):
        sub = tmp[tmp["week"] == w]
        if len(sub) == 0:
            return None
        return (sub["top_recommended_plan_type"] == plan_type).mean() * 100
    return mix_for(this_w), mix_for(prev_w)

def kpi_delta(source_df, metric_fn, label=""):
    if "call_date" not in source_df.columns:
        return None, None
    max_date = source_df["call_date"].max()
    if pd.isna(max_date):
        return None, None
    this_start = max_date - timedelta(days=6)
    prev_start = max_date - timedelta(days=13)
    prev_end   = max_date - timedelta(days=7)
    this_w = source_df[(source_df["call_date"].dt.date >= this_start.date()) &
                       (source_df["call_date"].dt.date <= max_date.date())]
    prev_w = source_df[(source_df["call_date"].dt.date >= prev_start.date()) &
                       (source_df["call_date"].dt.date <= prev_end.date())]
    return metric_fn(this_w), metric_fn(prev_w)

# ── Header ────────────────────────────────────────────────────────────────────
date_str = ""
if "call_date" in df.columns and df["call_date"].notna().any():
    mn = df["call_date"].min().strftime("%b %d")
    mx = df["call_date"].max().strftime("%b %d, %Y")
    date_str = f"{mn} – {mx}"

st.title("📊 Product Rank Dash")
st.caption(f"{date_str}  ·  {len(df):,} calls in view")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_model, tab_agent, tab_agent_level = st.tabs(["Model Outputs", "Agent Behavior & Performance", "Agent Level"])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — MODEL OUTPUTS
# ════════════════════════════════════════════════════════════════════════════════
with tab_model:

    # ── Section 1: Recommendation Mix ────────────────────────────────────────
    st.subheader("Recommendation Mix Over Time")

    if "top_recommended_plan_type" in df_nodatefilter.columns:
        plan_types_all = sorted(df_nodatefilter["top_recommended_plan_type"].dropna().unique().tolist())
        trend_cols = st.columns(len(plan_types_all))
        for i, pt in enumerate(plan_types_all):
            this_pct, prev_pct = week_mix(df_nodatefilter, pt)
            if this_pct is not None:
                delta_str = None
                if prev_pct is not None:
                    delta_str = f"{this_pct - prev_pct:+.1f}pp vs prior week"
                trend_cols[i].metric(
                    label=f"{pt} — Last Week",
                    value=f"{this_pct:.1f}%",
                    delta=delta_str,
                    help="Computed from the two most recent full ISO weeks, ignoring the date filter.",
                )

    granularity = st.radio(
        "Granularity",
        PERIOD_OPTIONS,
        index=0,  # Daily default
        horizontal=True,
        key="rec_mix_granularity",
    )

    if "call_date" in df.columns and "top_recommended_plan_type" in df.columns:
        period_col = period_labels(df["call_date"], granularity)

        rec_ts = (
            df.dropna(subset=["call_date", "top_recommended_plan_type"])
            .assign(period=period_col)
            .groupby(["period", "top_recommended_plan_type"])
            .size()
            .reset_index(name="n")
            .sort_values("period")
        )
        rec_ts["period_display"] = period_display(rec_ts["period"], granularity)
        totals = rec_ts.groupby("period")["n"].transform("sum")
        rec_ts["pct"] = rec_ts["n"] / totals * 100

        plan_types = sorted(rec_ts["top_recommended_plan_type"].unique().tolist())

        fig_mix = go.Figure()
        for pt in plan_types:
            sub = rec_ts[rec_ts["top_recommended_plan_type"] == pt]
            fig_mix.add_trace(go.Scatter(
                x=sub["period_display"], y=sub["pct"],
                name=pt, mode="lines+markers",
                line=dict(width=2),
                marker=dict(size=5),
            ))
        apply_dark_theme(fig_mix,
            yaxis_ticksuffix="%", height=340,
            margin=dict(l=40, r=20, t=10, b=40),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_mix, use_container_width=True)

        st.caption("Recommendation mix — share of calls (%) per plan type")
        rec_ts_tbl = rec_ts.copy()
        rec_pivot = (
            rec_ts_tbl.pivot(index="top_recommended_plan_type", columns="period_display", values="pct")
            .reset_index()
            .rename(columns={"top_recommended_plan_type": "Plan Type"})
        )
        for col in rec_pivot.columns[1:]:
            rec_pivot[col] = rec_pivot[col].round(1).astype(str) + "%"
        st.dataframe(rec_pivot, use_container_width=True, hide_index=True)
    else:
        st.info("call_date or top_recommended_plan_type column missing.")

    # ── NEW: Product-Level Recommendation Mix ─────────────────────────────────
    st.markdown("---")
    st.markdown("**Product-Level Recommendation Mix**")
    st.caption(
        "Share of calls (%) where a specific product appears in the Diamond or Gold rec slot over time. "
        "Use the dropdowns to filter by pitch slot and specific products."
    )

    # Determine which columns hold the ranked product recommendations
    # Expected: recommended_in_order (list/string of product names in rank order)
    # and first_pitch_type to identify Diamond vs Gold slot
    prod_col_candidates = ["recommended_in_order", "pitches_canonical_in_order", "pitches_in_order"]
    prod_col = next((c for c in prod_col_candidates if c in df.columns), None)

    if prod_col is not None and "call_date" in df.columns:
        import re as _re_prod

        def extract_product_at_slot(series_str, slot_idx):
            """Parse a list-like string and return the item at slot_idx (0-based)."""
            if not isinstance(series_str, str) or series_str.strip() in ("", "None", "nan", "null", "[]"):
                return None
            items = _re_prod.findall(r"'([^']+)'|\"([^\"]+)\"|([^\[\],\s][^\[\],]*[^\[\],\s]|[^\[\],\s]+)", series_str)
            flat = [next(g for g in grp if g) for grp in items]
            flat = [f.strip() for f in flat if f.strip() and f.strip() not in ("None", "nan", "null")]
            return flat[slot_idx] if slot_idx < len(flat) else None

        # Build a dataframe with diamond product and gold product per call
        prod_df = df.dropna(subset=["call_date"]).copy()
        prod_df["diamond_product"] = prod_df[prod_col].apply(lambda x: extract_product_at_slot(x, 0))
        prod_df["gold_product"]    = prod_df[prod_col].apply(lambda x: extract_product_at_slot(x, 1))

        all_diamond_products = sorted(prod_df["diamond_product"].dropna().unique().tolist())
        all_gold_products    = sorted(prod_df["gold_product"].dropna().unique().tolist())

        pml_c1, pml_c2 = st.columns(2)
        with pml_c1:
            pm_slot = st.selectbox(
                "Pitch Slot",
                options=["Diamond", "Gold"],
                key="pm_slot",
            )
        with pml_c2:
            slot_product_opts = all_diamond_products if pm_slot == "Diamond" else all_gold_products
            pm_products = st.multiselect(
                "Products (leave blank for all)",
                options=slot_product_opts,
                default=[],
                key="pm_products",
            )

        slot_product_col = "diamond_product" if pm_slot == "Diamond" else "gold_product"
        pm_df = prod_df.dropna(subset=[slot_product_col]).copy()

        if pm_products:
            pm_df = pm_df[pm_df[slot_product_col].isin(pm_products)]
            products_to_plot = pm_products
        else:
            # Show top 10 by frequency to avoid chart overload
            top_products = (
                pm_df[slot_product_col].value_counts().head(10).index.tolist()
            )
            pm_df = pm_df[pm_df[slot_product_col].isin(top_products)]
            products_to_plot = top_products

        if len(pm_df) > 0:
            pm_df["period"] = period_labels(pm_df["call_date"], granularity)
            # Total calls per period (from full df, not pm_df, for proper denominator)
            period_totals = (
                prod_df.assign(period=period_labels(prod_df["call_date"], granularity))
                .groupby("period")
                .size()
                .rename("total")
                .reset_index()
            )

            pm_ts = (
                pm_df.groupby(["period", slot_product_col])
                .size()
                .reset_index(name="n")
                .sort_values("period")
            )
            pm_ts = pm_ts.merge(period_totals, on="period", how="left")
            pm_ts["pct"] = pm_ts["n"] / pm_ts["total"] * 100
            pm_ts["period_display"] = period_display(pm_ts["period"], granularity)

            fig_pm = go.Figure()
            for prod in products_to_plot:
                sub = pm_ts[pm_ts[slot_product_col] == prod]
                if sub.empty:
                    continue
                fig_pm.add_trace(go.Scatter(
                    x=sub["period_display"],
                    y=sub["pct"],
                    name=prod,
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
            apply_dark_theme(fig_pm,
                yaxis_ticksuffix="%",
                height=340,
                margin=dict(l=40, r=20, t=10, b=40),
                legend=dict(orientation="h", y=-0.25),
                yaxis_title=f"% of calls with product in {pm_slot} slot",
            )
            st.plotly_chart(fig_pm, use_container_width=True)
            if not pm_products:
                st.caption(f"Showing top 10 products by volume in the {pm_slot} slot. Use the filter above to select specific products.")
        else:
            st.info("No data available for the selected slot / product combination.")
    else:
        st.info("Product recommendation column not found. Expected one of: recommended_in_order, pitches_canonical_in_order, pitches_in_order.")

    st.divider()

    # ── Section 2: Model Confidence & Raw Conversion Probabilities ────────────
    st.subheader("Model Confidence & Raw Conversion Probabilities")
    st.caption(
        "The model outputs raw conversion probabilities for Fixed, Tiered, and Bundled on every call, "
        "which are combined with plan points to produce expected-points scores and a ranked recommendation. "
        "This section examines those raw outputs: how confident the model is, whether that confidence is "
        "warranted, and what happens when agents follow or ignore high-confidence recommendations."
    )

    prob_cols_needed = {
        "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled",
        "expected_points_gap_1_2", "top_recommended_plan_type",
        "classification_bucket", "adhered_call", "order_count",
        "gcv_on_first_pitch", "gcv",
    }

    if prob_cols_needed.issubset(df.columns):

        import numpy as np

        COL_MAP = {"Fixed": "raw_prob_fixed", "Tiered": "raw_prob_tiered", "Bundled": "raw_prob_bundled"}
        GAP_LABELS = ["Q1\nLowest", "Q2", "Q3", "Q4", "Q5\nHighest"]

        ra_cols = st.columns(4)
        for i, (pt, col) in enumerate([("Fixed", "raw_prob_fixed"), ("Tiered", "raw_prob_tiered"),
                                        ("Bundled", "raw_prob_bundled")]):
            if col in df.columns:
                avg_p = df[col].mean() * 100
                ra_cols[i].metric(f"Avg P(convert) — {pt}", f"{avg_p:.1f}%",
                                  help=f"Mean raw conversion probability for {pt} across all calls in view")
        avg_gap = df["expected_points_gap_1_2"].mean()
        ra_cols[3].metric("Avg Confidence Gap", f"{avg_gap:.2f} pts",
                          help="Mean expected-points gap between #1 and #2 recommendations — higher = model more certain")

        st.markdown("**Raw Probability Distributions**")
        st.caption(
            "Left: violin plots of each product's raw conversion probability across all calls, regardless "
            "of what was recommended. Right: histogram of the expected-points gap between #1 and #2 recommendations."
        )
        rb1, rb2 = st.columns(2)

        with rb1:
            fig_violin = go.Figure()
            for pt, col in [("Fixed", "raw_prob_fixed"), ("Tiered", "raw_prob_tiered"), ("Bundled", "raw_prob_bundled")]:
                if col in df.columns:
                    fig_violin.add_trace(go.Violin(
                        y=df[col].dropna(),
                        name=pt,
                        box_visible=True,
                        meanline_visible=True,
                        points=False,
                    ))
            apply_dark_theme(fig_violin,
                yaxis_title="Raw Conversion Probability",
                yaxis_tickformat=".0%",
                height=320,
                margin=dict(l=40, r=20, t=10, b=40),
                showlegend=False,
            )
            st.plotly_chart(fig_violin, use_container_width=True)

        with rb2:
            gap_vals = df["expected_points_gap_1_2"].dropna()
            p25, p75 = gap_vals.quantile(0.25), gap_vals.quantile(0.75)
            pct_low  = (gap_vals < p25).mean() * 100
            pct_high = (gap_vals > p75).mean() * 100
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(x=gap_vals, nbinsx=40, marker_color="#3d8ef8", opacity=0.8))
            fig_hist.add_vline(x=float(p25), line_dash="dash", line_color="#4d5669",
                               annotation_text=f"25th ({p25:.2f})", annotation_position="top right",
                               annotation_font_color="#8b95aa")
            fig_hist.add_vline(x=float(p75), line_dash="dash", line_color="#4d5669",
                               annotation_text=f"75th ({p75:.2f})", annotation_position="top left",
                               annotation_font_color="#8b95aa")
            apply_dark_theme(fig_hist,
                xaxis_title="Expected Points Gap (#1 vs #2)",
                yaxis_title="Calls",
                height=320,
                margin=dict(l=40, r=20, t=10, b=40),
                showlegend=False,
            )
            st.plotly_chart(fig_hist, use_container_width=True)
            st.caption(f"**{pct_low:.0f}%** of calls are low-confidence (gap < {p25:.2f} pts) · "
                       f"**{pct_high:.0f}%** are high-confidence (gap > {p75:.2f} pts)")

        st.markdown("**Does Model Confidence Predict Outcome? — By Confidence Gap Quintile**")
        st.caption(
            "Calls bucketed by the model's confidence gap (expected-points difference between #1 and #2). "
            "Left: do agents adhere more when the model is confident? "
            "Right: does following the recommendation pay off more when the model is confident?"
        )

        df_gap = df.dropna(subset=["expected_points_gap_1_2", "adhered_call"]).copy()
        df_gap["gap_bucket"] = pd.qcut(
            df_gap["expected_points_gap_1_2"], q=5,
            labels=GAP_LABELS, duplicates="drop",
        )

        rd1, rd2 = st.columns(2)

        with rd1:
            adh_by_gap = (
                df_gap.groupby("gap_bucket", observed=True)
                .agg(
                    adherence=("adhered_call", "mean"),
                    calls=("adhered_call", "count"),
                    gap_med=("expected_points_gap_1_2", "median"),
                )
                .reset_index()
            )
            fig_adh_gap = go.Figure()
            fig_adh_gap.add_trace(go.Bar(
                x=adh_by_gap["gap_bucket"].astype(str),
                y=adh_by_gap["adherence"] * 100,
                text=(adh_by_gap["adherence"] * 100).round(1).astype(str) + "%",
                textposition="outside",
                textfont=dict(color="#8b95aa", size=11),
                marker_color="#3d8ef8",
                marker_line_width=0,
                customdata=adh_by_gap[["calls", "gap_med"]],
                hovertemplate="Calls: %{customdata[0]:,}<br>Median gap: %{customdata[1]:.2f}<extra></extra>",
            ))
            apply_dark_theme(fig_adh_gap,
                xaxis_title="Confidence Gap Quintile",
                yaxis_title="Adherence Rate",
                yaxis_ticksuffix="%",
                height=300,
                margin=dict(l=40, r=20, t=10, b=50),
                showlegend=False,
            )
            st.plotly_chart(fig_adh_gap, use_container_width=True)

        with rd2:
            metric_choice = st.radio(
                "Outcome metric",
                ["1st Pitch CR", "Overall CR", "GCV / Call"],
                horizontal=True,
                key="conf_gap_metric",
            )
            col_map2 = {"1st Pitch CR": "gcv_on_first_pitch", "Overall CR": "order_count", "GCV / Call": "gcv"}

            df_gap2 = df_gap[df_gap["classification_bucket"].isin(["Adherence", "Slide"])].copy()
            if len(df_gap2) > 0:
                gap_out = (
                    df_gap2.groupby(["gap_bucket", "classification_bucket"], observed=True)
                    .agg(val=(col_map2[metric_choice],
                              lambda x: x.mean() if metric_choice == "GCV / Call" else (x > 0).mean() * 100),
                         calls=("gcv", "count"))
                    .reset_index()
                )
                is_dollar = metric_choice == "GCV / Call"
                fig_out = go.Figure()
                for label, dash in [("Adherence", "solid"), ("Slide", "dot")]:
                    sub = gap_out[gap_out["classification_bucket"] == label]
                    fig_out.add_trace(go.Scatter(
                        x=sub["gap_bucket"].astype(str),
                        y=sub["val"],
                        name=label,
                        mode="lines+markers",
                        line=dict(dash=dash, width=2),
                        marker=dict(size=5),
                    ))
                apply_dark_theme(fig_out,
                    xaxis_title="Confidence Gap Quintile",
                    yaxis_tickprefix="$" if is_dollar else "",
                    yaxis_ticksuffix="" if is_dollar else "%",
                    height=300,
                    margin=dict(l=40, r=20, t=10, b=50),
                    legend=dict(orientation="h", y=-0.25),
                )
                st.plotly_chart(fig_out, use_container_width=True)

    else:
        missing = prob_cols_needed - set(df.columns)
        st.info(f"Columns missing for this section: {', '.join(sorted(missing))}")

    st.divider()

    # ── Section 3: Top Rec vs. Slide Conversion Comparison ───────────────────
    st.subheader("Top Rec vs. Slide — Conversion & Value Comparison")
    st.caption(
        "Compares calls where the agent pitched the model's top recommendation first "
        "(Adherence) versus calls where they pitched the slide product first (Slide)."
    )

    needed = {"classification_bucket", "gcv_on_first_pitch", "order_count", "gcv"}

    if needed.issubset(df.columns):
        top_df   = df[df["classification_bucket"] == "Adherence"]
        slide_df = df[df["classification_bucket"] == "Slide"]

        def safe_mean(s):
            return s.mean() if len(s) else float("nan")

        top_fp_cr    = (top_df["gcv_on_first_pitch"] > 0).mean() * 100
        slide_fp_cr  = (slide_df["gcv_on_first_pitch"] > 0).mean() * 100
        top_cr       = (top_df["order_count"] > 0).mean() * 100
        slide_cr     = (slide_df["order_count"] > 0).mean() * 100
        top_gcv      = safe_mean(top_df["gcv"])
        slide_gcv    = safe_mean(slide_df["gcv"])
        # FIX: GCV / 1st Pitch = total first-pitch GCV / all calls (expected value, not conditional mean)
        top_gcv_fp   = safe_mean(top_df["gcv_on_first_pitch"])
        slide_gcv_fp = safe_mean(slide_df["gcv_on_first_pitch"])

        ca1, ca2, ca3, ca4 = st.columns(4)
        ca1.metric("Top Rec — 1st Pitch CR",  f"{top_fp_cr:.1f}%",
                   delta=f"{top_fp_cr - slide_fp_cr:+.1f}pp vs Slide")
        ca2.metric("Slide — 1st Pitch CR",    f"{slide_fp_cr:.1f}%")
        ca3.metric("Top Rec — GCV / Call",    f"${top_gcv:,.0f}",
                   delta=f"${top_gcv - slide_gcv:+,.0f} vs Slide")
        ca4.metric("Slide — GCV / Call",      f"${slide_gcv:,.0f}")

        cb1, cb2 = st.columns(2)

        with cb1:
            st.markdown("**Conversion Rate**")
            fig_cr = go.Figure()
            fig_cr.add_trace(go.Bar(
                name="Top Rec", x=["1st Pitch CR", "Overall CR"],
                y=[top_fp_cr, top_cr],
                text=[f"{top_fp_cr:.1f}%", f"{top_cr:.1f}%"], textposition="outside",
                textfont=dict(color="#8b95aa", size=11),
                marker_color="#3d8ef8", marker_line_width=0,
            ))
            fig_cr.add_trace(go.Bar(
                name="Slide", x=["1st Pitch CR", "Overall CR"],
                y=[slide_fp_cr, slide_cr],
                text=[f"{slide_fp_cr:.1f}%", f"{slide_cr:.1f}%"], textposition="outside",
                textfont=dict(color="#8b95aa", size=11),
                marker_color="#22d3c8", marker_line_width=0,
            ))
            apply_dark_theme(fig_cr,
                barmode="group", yaxis_ticksuffix="%", height=300,
                margin=dict(l=40, r=20, t=10, b=40),
                legend=dict(orientation="h", y=-0.25),
            )
            st.plotly_chart(fig_cr, use_container_width=True)

        with cb2:
            st.markdown("**GCV**")
            st.caption("GCV / 1st Pitch = total first-pitch GCV ÷ all calls in group (expected value per call)")
            fig_gcv = go.Figure()
            fig_gcv.add_trace(go.Bar(
                name="Top Rec", x=["GCV / Call", "GCV / 1st Pitch"],
                y=[top_gcv, top_gcv_fp],
                text=[f"${top_gcv:,.0f}", f"${top_gcv_fp:,.0f}"], textposition="outside",
                textfont=dict(color="#8b95aa", size=11),
                marker_color="#3d8ef8", marker_line_width=0,
            ))
            fig_gcv.add_trace(go.Bar(
                name="Slide", x=["GCV / Call", "GCV / 1st Pitch"],
                y=[slide_gcv, slide_gcv_fp],
                text=[f"${slide_gcv:,.0f}", f"${slide_gcv_fp:,.0f}"], textposition="outside",
                textfont=dict(color="#8b95aa", size=11),
                marker_color="#22d3c8", marker_line_width=0,
            ))
            apply_dark_theme(fig_gcv,
                barmode="group", yaxis_tickprefix="$", height=300,
                margin=dict(l=40, r=20, t=10, b=40),
                legend=dict(orientation="h", y=-0.25),
            )
            st.plotly_chart(fig_gcv, use_container_width=True)

        st.markdown("**Conversion by Plan Type — Top Rec vs. Slide**")
        if "top_recommended_plan_type" in df.columns:
            plan_cmp = (
                df[df["classification_bucket"].isin(["Adherence", "Slide"])]
                .groupby(["top_recommended_plan_type", "classification_bucket"])
                .agg(
                    calls=("call_id", "count"),
                    fp_cr=("gcv_on_first_pitch", lambda x: (x > 0).mean()),
                    overall_cr=("order_count", lambda x: (x > 0).mean()),
                    gcv_call=("gcv", "mean"),
                    # FIX: GCV / 1st Pitch EV = mean over all calls (zeros included)
                    gcv_fp_ev=("gcv_on_first_pitch", "mean"),
                )
                .reset_index()
            )
            plan_cmp["fp_cr"]      = (plan_cmp["fp_cr"] * 100).round(1).astype(str) + "%"
            plan_cmp["overall_cr"] = (plan_cmp["overall_cr"] * 100).round(1).astype(str) + "%"
            plan_cmp["gcv_call"]   = plan_cmp["gcv_call"].round(0).apply(lambda x: f"${x:,.0f}")
            plan_cmp["gcv_fp_ev"]  = plan_cmp["gcv_fp_ev"].round(0).apply(lambda x: f"${x:,.0f}")
            plan_cmp = plan_cmp.rename(columns={
                "top_recommended_plan_type": "Plan Type",
                "classification_bucket": "Pitched",
                "calls": "Calls",
                "fp_cr": "1st Pitch CR",
                "overall_cr": "Overall CR",
                "gcv_call": "GCV / Call",
                "gcv_fp_ev": "GCV / 1st Pitch",
            })
            st.dataframe(plan_cmp, use_container_width=True, hide_index=True)
    else:
        st.info("One or more required columns are missing for this section.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — AGENT BEHAVIOR & PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
with tab_agent:

    agent_granularity = st.radio(
        "Granularity",
        PERIOD_OPTIONS,
        index=0,  # Daily default
        horizontal=True,
        key="agent_granularity",
    )

    if "first_pitch_type" in df_nodatefilter.columns and "call_date" in df_nodatefilter.columns:
        tmp_fp = df_nodatefilter.dropna(subset=["call_date", "first_pitch_type"]).copy()
        tmp_fp["week"] = tmp_fp["call_date"].dt.to_period("W")
        fp_weeks = sorted(tmp_fp["week"].unique())
        fp_m1, fp_m2 = (fp_weeks[-1] if len(fp_weeks) >= 1 else None,
                        fp_weeks[-2] if len(fp_weeks) >= 2 else None)

        def fp_rate(week_period, tier):
            if week_period is None:
                return None
            sub = tmp_fp[tmp_fp["week"] == week_period]
            return (sub["first_pitch_type"] == tier).mean() * 100 if len(sub) else None

        fp_cols = st.columns(4)
        for i, tier in enumerate(["Diamond", "Gold", "Silver", "Bronze"]):
            this_v = fp_rate(fp_m1, tier)
            prev_v = fp_rate(fp_m2, tier)
            if this_v is not None:
                delta_str = (f"{this_v - prev_v:+.1f}pp vs prior week"
                             if prev_v is not None else None)
                fp_cols[i].metric(
                    label=f"{tier} FP Rate",
                    value=f"{this_v:.1f}%",
                    delta=delta_str,
                    help="Share of calls where this tier was pitched first · last full ISO week · ignores date filter",
                )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Adherence Over Time")
        if "call_date" in df.columns and "adhered_call" in df.columns:
            ts = (
                df.dropna(subset=["call_date"])
                .assign(period=period_labels(df["call_date"], agent_granularity))
                .groupby("period")
                .agg(
                    adherence=("adhered_call", "mean"),
                    slide=("slide_call", "mean"),
                    all_plans=("all_plans_call", "mean"),
                )
                .reset_index()
                .sort_values("period")
            )
            ts["period_display"] = period_display(ts["period"], agent_granularity)
            ts[["adherence", "slide", "all_plans"]] *= 100

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["adherence"], name="Adherence",
                                     mode="lines+markers", line=dict(width=2), marker=dict(size=5)))
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["slide"], name="Slide",
                                     mode="lines+markers", line=dict(dash="dot", width=2), marker=dict(size=5)))
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["all_plans"], name="All Plans",
                                     mode="lines+markers", line=dict(dash="dash", width=2), marker=dict(size=5)))
            apply_dark_theme(fig,
                yaxis_ticksuffix="%", height=320,
                margin=dict(l=40, r=20, t=20, b=40),
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("call_date or adhered_call column missing.")

    with col2:
        st.subheader("First Pitch Mix Over Time")
        if "call_date" in df.columns and "first_pitch_type" in df.columns:
            pitch_ts = (
                df.dropna(subset=["call_date", "first_pitch_type"])
                .assign(period=period_labels(df["call_date"], agent_granularity))
                .groupby(["period", "first_pitch_type"])
                .size()
                .reset_index(name="n")
                .sort_values("period")
            )
            pitch_ts["period_display"] = period_display(pitch_ts["period"], agent_granularity)
            totals = pitch_ts.groupby("period")["n"].transform("sum")
            pitch_ts["pct"] = pitch_ts["n"] / totals * 100

            fig2 = go.Figure()
            for pt in ["Diamond", "Gold", "Silver", "Bronze"]:
                sub = pitch_ts[pitch_ts["first_pitch_type"] == pt]
                if sub.empty:
                    continue
                fig2.add_trace(go.Scatter(x=sub["period_display"], y=sub["pct"], name=pt,
                                          mode="lines+markers", line=dict(width=2), marker=dict(size=5)))
            apply_dark_theme(fig2,
                yaxis_ticksuffix="%", height=320,
                margin=dict(l=40, r=20, t=20, b=40),
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("call_date or first_pitch_type column missing.")

    st.divider()

    # ── Performance over time ─────────────────────────────────────────────────
    st.subheader("Performance Over Time")

    pot_c1, pot_c2 = st.columns(2)
    with pot_c1:
        pot_metric = st.selectbox(
            "Metric",
            ["1st Pitch CR", "Overall CR", "GCV / 1st Pitch", "GCV / Call", "RPO"],
            key="pot_metric",
        )
    with pot_c2:
        fp_type_opts = ["All"] + ["Diamond", "Gold", "Silver", "Bronze"]
        pot_fp_filter = st.selectbox(
            "First Pitch Type",
            fp_type_opts,
            key="pot_fp_filter",
        )

    if "call_date" in df.columns:
        pot_df = df.dropna(subset=["call_date"]).copy()

        if pot_fp_filter != "All" and "first_pitch_type" in pot_df.columns:
            pot_df = pot_df[pot_df["first_pitch_type"] == pot_fp_filter]

        pot_df["period"] = period_labels(pot_df["call_date"], agent_granularity)

        def pot_agg(grp):
            if pot_metric == "1st Pitch CR":
                return (grp["gcv_on_first_pitch"] > 0).mean() * 100
            elif pot_metric == "Overall CR":
                return (grp["order_count"] > 0).mean() * 100
            elif pot_metric == "GCV / 1st Pitch":
                # FIX: expected value — mean over ALL calls, zeros included
                return grp["gcv_on_first_pitch"].mean()
            elif pot_metric == "GCV / Call":
                return grp["gcv"].mean()
            elif pot_metric == "RPO":
                orders = grp[grp["order_count"] > 0]
                return orders["gcv"].mean() if len(orders) else float("nan")

        needed_pot = {"gcv_on_first_pitch", "order_count", "gcv"}
        if needed_pot.issubset(pot_df.columns):
            pot_ts = (
                pot_df.groupby("period")
                .apply(pot_agg)
                .reset_index()
                .rename(columns={0: "value"})
                .sort_values("period")
            )
            pot_ts["period_display"] = period_display(pot_ts["period"], agent_granularity)

            is_dollar = pot_metric in ("GCV / 1st Pitch", "GCV / Call", "RPO")
            fig_pot = go.Figure()
            fig_pot.add_trace(go.Scatter(
                x=pot_ts["period_display"],
                y=pot_ts["value"],
                mode="lines+markers",
                name=pot_metric,
                line=dict(color="#3d8ef8", width=2),
                marker=dict(size=5, color="#3d8ef8"),
                fill="tozeroy",
                fillcolor="rgba(61,142,248,0.06)",
            ))
            apply_dark_theme(fig_pot,
                yaxis_tickprefix="$" if is_dollar else "",
                yaxis_ticksuffix="" if is_dollar else "%",
                height=320,
                margin=dict(l=40, r=20, t=10, b=40),
                showlegend=False,
            )
            st.plotly_chart(fig_pot, use_container_width=True)
        else:
            st.info("Required columns missing for this chart.")
    else:
        st.info("call_date column missing.")

    st.divider()

    # ── Period-over-period comparison table ───────────────────────────────────
    st.subheader("Period-over-Period Comparison")
    st.caption(
        "Select two date ranges to compare. Defaults to the last two full weeks. "
        "Delta cells are colored green (improvement) or red (decline)."
    )

    needed_cols = {
        "call_date", "top_recommended_plan_type", "classification_bucket",
        "adhered_call", "slide_call", "all_plans_call",
        "gcv_on_first_pitch", "order_count", "gcv",
    }

    if needed_cols.issubset(df_raw.columns):

        raw_min = df_raw["call_date"].min().date()
        raw_max = df_raw["call_date"].max().date()

        import datetime as _dt
        _last_mon  = raw_max - timedelta(days=raw_max.weekday())
        _post_def_start = _last_mon - timedelta(days=7)
        _post_def_end   = _post_def_start + timedelta(days=6)
        _pre_def_start  = _post_def_start - timedelta(days=7)
        _pre_def_end    = _pre_def_start + timedelta(days=6)
        _post_def_start = max(_post_def_start, raw_min)
        _post_def_end   = min(_post_def_end,   raw_max)
        _pre_def_start  = max(_pre_def_start,  raw_min)
        _pre_def_end    = min(_pre_def_end,     raw_max)

        tc1, tc2 = st.columns(2)
        with tc1:
            pre_range = st.date_input(
                "Pre period",
                value=(_pre_def_start, _pre_def_end),
                min_value=raw_min,
                max_value=raw_max,
                key="cmp_pre_range",
            )
        with tc2:
            post_range = st.date_input(
                "Post period",
                value=(_post_def_start, _post_def_end),
                min_value=raw_min,
                max_value=raw_max,
                key="cmp_post_range",
            )

        if len(pre_range) == 2 and len(post_range) == 2:

            def slice_period(base, start, end):
                return base[
                    (base["call_date"].dt.date >= start) &
                    (base["call_date"].dt.date <= end)
                ]

            pre_df  = slice_period(df_nodatefilter, pre_range[0],  pre_range[1])
            post_df = slice_period(df_nodatefilter, post_range[0], post_range[1])

            def overall_metric(source, metric):
                if len(source) == 0:
                    return float("nan")
                if metric == "fp_cr":
                    return (source["gcv_on_first_pitch"] > 0).mean() * 100
                if metric == "ov_cr":
                    return (source["order_count"] > 0).mean() * 100
                if metric == "gcv_fp":
                    # FIX: expected value over all calls
                    return source["gcv_on_first_pitch"].mean()
                if metric == "gcv_call":
                    return source["gcv"].mean()
                return float("nan")

            hm_specs = [
                ("fp_cr",    "1st Pitch CR",         "pct"),
                ("ov_cr",    "Overall CR",            "pct"),
                ("gcv_fp",   "GCV / 1st Pitch",  "dollar"),
                ("gcv_call", "GCV / Call",            "dollar"),
            ]

            hm_cols = st.columns(4)
            for i, (metric, label, fmt) in enumerate(hm_specs):
                post_v = overall_metric(post_df, metric)
                pre_v  = overall_metric(pre_df,  metric)
                if fmt == "pct":
                    val_str   = f"{post_v:.1f}%" if not pd.isna(post_v) else "—"
                    delta_str = (f"{post_v - pre_v:+.1f}pp" if not pd.isna(post_v) and not pd.isna(pre_v) else None)
                else:
                    val_str   = f"${post_v:,.0f}" if not pd.isna(post_v) else "—"
                    delta_str = (f"${post_v - pre_v:+,.0f}" if not pd.isna(post_v) and not pd.isna(pre_v) else None)
                hm_cols[i].metric(label=label, value=val_str, delta=delta_str,
                                  help=f"Post period value · delta vs pre period")

            def compute_metrics(source):
                if len(source) == 0:
                    return pd.DataFrame()

                rec_totals = (
                    source.dropna(subset=["top_recommended_plan_type"])
                    .groupby("top_recommended_plan_type")
                    ["call_id"].count()
                    .rename("rec_total")
                )

                rows = []
                for rec_type, behavior, mask_col in [
                    ("top_recommended_plan_type", "Adhered",   "adhered_call"),
                    ("top_recommended_plan_type", "Slide",     "slide_call"),
                    ("top_recommended_plan_type", "All Plans", "all_plans_call"),
                ]:
                    grp = (
                        source.dropna(subset=["top_recommended_plan_type"])
                        .groupby("top_recommended_plan_type")
                    )
                    for rtype, g in grp:
                        sub = g[g[mask_col] == 1] if mask_col in g.columns else g.iloc[0:0]
                        n_sub    = len(sub)
                        n_rec    = len(g)
                        mix      = n_sub / rec_totals.get(rtype, n_rec) * 100 if rec_totals.get(rtype, 0) > 0 else float("nan")
                        fp_cr    = (sub["gcv_on_first_pitch"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        ov_cr    = (sub["order_count"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        # FIX: GCV / 1st Pitch EV = mean over all calls in subset (zeros included)
                        gcv_fp   = sub["gcv_on_first_pitch"].mean() if n_sub > 0 else float("nan")
                        gcv_call = sub["gcv"].mean() if n_sub > 0 else float("nan")
                        rows.append({
                            "rec_type": rtype,
                            "behavior": behavior,
                            "mix":      mix,
                            "fp_cr":    fp_cr,
                            "ov_cr":    ov_cr,
                            "gcv_fp":   gcv_fp,
                            "gcv_call": gcv_call,
                        })
                return pd.DataFrame(rows)

            pre_metrics  = compute_metrics(pre_df)
            post_metrics = compute_metrics(post_df)

            if not pre_metrics.empty and not post_metrics.empty:

                merged = pre_metrics.merge(
                    post_metrics,
                    on=["rec_type", "behavior"],
                    suffixes=("_pre", "_post"),
                )

                pre_label  = f"{pre_range[0].strftime('%-m/%-d')}-{pre_range[1].strftime('%-m/%-d')}"
                post_label = f"{post_range[0].strftime('%-m/%-d')}-{post_range[1].strftime('%-m/%-d')}"

                METRICS = [
                    ("mix",      "Mix",                    "pct",    False),
                    ("fp_cr",    "First Pitch CR",          "pct",    False),
                    ("ov_cr",    "Overall CR",              "pct",    False),
                    ("gcv_fp",   "GCV / First Pitch",  "dollar", True),
                    ("gcv_call", "GCV / Call",              "dollar", True),
                ]

                def fmt_val(v, fmt):
                    if pd.isna(v): return "—"
                    if fmt == "pct":    return f"{v:.0f}%"
                    if fmt == "dollar": return f"${v:,.1f}"
                    return str(v)

                def fmt_delta(pre, post, fmt):
                    if pd.isna(pre) or pd.isna(post) or pre == 0:
                        return "—"
                    pct_chg = (post - pre) / abs(pre) * 100
                    return f"{pct_chg:+.0f}%"

                def color_delta_cell(val):
                    if val == "—" or val == "":
                        return ""
                    try:
                        num = float(val.replace("%", "").replace("+", ""))
                    except Exception:
                        return ""
                    if abs(num) < 3:
                        return "background-color: #2a2a1a; color: #c8a000"
                    if num > 0:
                        return "background-color: #0f2a1a; color: #22c55e"
                    return "background-color: #2a1018; color: #f43f5e"

                BEH_ORDER = ["Adhered", "Slide", "All Plans"]
                rec_types = sorted(merged["rec_type"].unique())

                display_rows = []
                row_idx = 0
                for rt in rec_types:
                    for beh in BEH_ORDER:
                        match = merged[(merged["rec_type"] == rt) & (merged["behavior"] == beh)]
                        if match.empty:
                            continue
                        r = match.iloc[0]
                        row = {
                            "Rec Type":  rt if beh == "Adhered" else "",
                            "Behavior":  beh,
                        }
                        for col, label, fmt, hib in METRICS:
                            pre_v  = r[f"{col}_pre"]
                            post_v = r[f"{col}_post"]
                            row[f"{label} {pre_label}"]  = fmt_val(pre_v,  fmt)
                            row[f"{label} {post_label}"] = fmt_val(post_v, fmt)
                            row[f"{label} Delta"] = fmt_delta(pre_v, post_v, fmt)
                        display_rows.append(row)
                        row_idx += 1

                display_df = pd.DataFrame(display_rows)
                delta_cols = [f"{label} Delta" for _, label, _, _ in METRICS]
                styler = display_df.style.map(color_delta_cell, subset=delta_cols)

                col_order = ["Rec Type", "Behavior"]
                for _, label, _, _ in METRICS:
                    col_order += [f"{label} {pre_label}", f"{label} {post_label}", f"{label} Delta"]

                styler = styler.set_properties(**{"text-align": "right"}, subset=col_order[2:])
                styler = styler.set_properties(**{"text-align": "left"},  subset=["Rec Type", "Behavior"])

                st.dataframe(styler, use_container_width=True, hide_index=True, column_order=col_order)

                st.subheader("Overall Comparison")

                def compute_overall_metrics(source):
                    if len(source) == 0:
                        return pd.DataFrame()
                    total_calls = len(source)
                    rows = []
                    for behavior, mask_col in [
                        ("Adhered",   "adhered_call"),
                        ("Slide",     "slide_call"),
                        ("All Plans", "all_plans_call"),
                    ]:
                        sub = source[source[mask_col] == 1] if mask_col in source.columns else source.iloc[0:0]
                        n_sub = len(sub)
                        mix      = n_sub / total_calls * 100 if total_calls > 0 else float("nan")
                        fp_cr    = (sub["gcv_on_first_pitch"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        ov_cr    = (sub["order_count"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        # FIX: GCV / 1st Pitch EV = mean over all calls (zeros included)
                        gcv_fp   = sub["gcv_on_first_pitch"].mean() if n_sub > 0 else float("nan")
                        gcv_call = sub["gcv"].mean() if n_sub > 0 else float("nan")
                        rows.append({
                            "behavior": behavior,
                            "mix":      mix,
                            "fp_cr":    fp_cr,
                            "ov_cr":    ov_cr,
                            "gcv_fp":   gcv_fp,
                            "gcv_call": gcv_call,
                        })
                    return pd.DataFrame(rows)

                pre_overall  = compute_overall_metrics(pre_df)
                post_overall = compute_overall_metrics(post_df)

                if not pre_overall.empty and not post_overall.empty:
                    merged_ov = pre_overall.merge(post_overall, on="behavior", suffixes=("_pre", "_post"))

                    ov_rows = []
                    for _, r in merged_ov.iterrows():
                        row = {"Behavior": r["behavior"]}
                        for col, label, fmt, hib in METRICS:
                            pre_v  = r[f"{col}_pre"]
                            post_v = r[f"{col}_post"]
                            row[f"{label} {pre_label}"]  = fmt_val(pre_v,  fmt)
                            row[f"{label} {post_label}"] = fmt_val(post_v, fmt)
                            row[f"{label} Delta"]        = fmt_delta(pre_v, post_v, fmt)
                        ov_rows.append(row)

                    ov_df = pd.DataFrame(ov_rows)
                    ov_styler = ov_df.style.map(color_delta_cell, subset=delta_cols)

                    ov_col_order = ["Behavior"]
                    for _, label, _, _ in METRICS:
                        ov_col_order += [f"{label} {pre_label}", f"{label} {post_label}", f"{label} Delta"]

                    ov_styler = ov_styler.set_properties(**{"text-align": "right"}, subset=ov_col_order[1:])
                    ov_styler = ov_styler.set_properties(**{"text-align": "left"},  subset=["Behavior"])

                    st.dataframe(ov_styler, use_container_width=True, hide_index=True, column_order=ov_col_order)

            else:
                st.info("Not enough data in selected date ranges to compute metrics.")
    else:
        missing = needed_cols - set(df_raw.columns)
        st.info(f"Columns missing for comparison table: {', '.join(sorted(missing))}")

    st.divider()

    # ── Confusion Matrix ──────────────────────────────────────────────────────
    st.subheader("Confusion Matrix — First Pitch vs. Recommended Plan Type")
    st.caption(
        "Rows: plan type of the rec slot the agent pitched first. "
        "Columns: top recommended plan type. "
        "Diagonal cells = agent pitched a rec of the same plan type as the top rec. "
        "Other row = first pitch was outside all rec slots (Silver / Bronze tier)."
    )

    cm_needed = {
        "first_pitch_type", "first_pitch_plan_category",
        "top_recommended_plan_type", "recommended_plan_types_in_order",
        "order_count", "gcv",
    }

    if cm_needed.issubset(df.columns):
        import ast as _ast
        import re as _re

        def norm_plan_type(x):
            if not isinstance(x, str):
                return None
            x = x.strip()
            if _re.search(r'\bFixed\b',   x, _re.IGNORECASE): return "Fixed"
            if _re.search(r'\bTiered\b',  x, _re.IGNORECASE): return "Tiered"
            if _re.search(r'\bBundled\b', x, _re.IGNORECASE): return "Bundled"
            return None

        def safe_parse_list(v):
            if isinstance(v, list):
                return v
            if not isinstance(v, str) or v.strip() in ("", "None", "nan", "null", "[]"):
                return []
            import re
            return re.findall(r'\b(Fixed|Tiered|Bundled)\b', v)

        cm_df = df.dropna(subset=["first_pitch_type", "top_recommended_plan_type"]).copy()
        cm_df = cm_df[~cm_df["recommended_plan_types_in_order"].astype(str).isin(
            ["", "None", "nan", "null", "[]"]
        )]
        cm_df["_rec_types"] = cm_df["recommended_plan_types_in_order"].apply(safe_parse_list)

        def get_row_col(row):
            fpt       = row["first_pitch_type"]
            fp_ptype  = norm_plan_type(row.get("first_pitch_plan_category"))
            top_ptype = norm_plan_type(row.get("top_recommended_plan_type"))
            rec_types = [norm_plan_type(t) for t in row["_rec_types"]]

            if fpt == "Diamond":
                row_label = rec_types[0] if rec_types else fp_ptype
                col_label = top_ptype
            elif fpt == "Gold":
                slide_types = rec_types[1:] if len(rec_types) > 1 else []
                matched = [t for t in slide_types if t == fp_ptype]
                row_label = matched[0] if matched else (slide_types[0] if slide_types else fp_ptype)
                col_label = top_ptype
            else:
                row_label = "Other"
                col_label = top_ptype
            return pd.Series({"row_label": row_label, "col_label": col_label})

        cm_df[["row_label", "col_label"]] = cm_df.apply(get_row_col, axis=1)
        cm_df = cm_df[cm_df["row_label"].notna() & cm_df["col_label"].notna()]
        total_calls = len(cm_df)

        ROW_LABELS = ["Fixed", "Tiered", "Bundled", "Other"]
        COL_LABELS = ["Fixed", "Tiered", "Bundled"]

        z_counts   = []
        text_cells = []

        for row_label in ROW_LABELS:
            z_row, t_row = [], []
            for col_label in COL_LABELS:
                subset = cm_df[
                    (cm_df["row_label"] == row_label) &
                    (cm_df["col_label"] == col_label)
                ]
                n = len(subset)
                z_row.append(n)
                if n == 0:
                    t_row.append("")
                else:
                    cr      = (subset["order_count"] > 0).mean()
                    prop    = n / total_calls
                    avg_gcv = subset["gcv"].sum() / n
                    t_row.append(
                        f"CR={cr:.1%}<br>n={n:,}<br>p={prop:.1%}<br>avg GCV=${avg_gcv:,.0f}"
                    )
            z_counts.append(z_row)
            text_cells.append(t_row)

        fig_cm = go.Figure(go.Heatmap(
            z=z_counts,
            x=COL_LABELS,
            y=ROW_LABELS,
            text=text_cells,
            texttemplate="%{text}",
            colorscale=[[0, "#0d1520"], [0.5, "#1a3a6e"], [1.0, "#3d8ef8"]],
            colorbar=dict(
                title="Calls",
                title_font=dict(color="#8b95aa", size=11),
                tickfont=dict(color="#8b95aa", size=10),
                bgcolor="rgba(0,0,0,0)",
                bordercolor="#252b3a",
            ),
            hoverongaps=False,
        ))
        apply_dark_theme(fig_cm,
            xaxis=dict(
                title="Recommended plan type",
                side="bottom",
                gridcolor="rgba(0,0,0,0)",
                linecolor="#252b3a",
            ),
            yaxis=dict(
                title="First pitch (canonical rec match → plan type)",
                autorange="reversed",
                gridcolor="rgba(0,0,0,0)",
                linecolor="#252b3a",
            ),
            height=460,
            margin=dict(l=100, r=40, t=20, b=80),
        )
        st.plotly_chart(fig_cm, use_container_width=True)
        st.caption(f"Total calls in view: {total_calls:,}  ·  "
                   f"Diamond: {(cm_df['first_pitch_type']=='Diamond').sum():,}  ·  "
                   f"Gold: {(cm_df['first_pitch_type']=='Gold').sum():,}  ·  "
                   f"Other (Silver/Bronze): {cm_df['row_label'].eq('Other').sum():,}")
    else:
        missing = cm_needed - set(df.columns)
        st.info(f"Columns missing for confusion matrix: {', '.join(sorted(missing))}")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — AGENT LEVEL
# ════════════════════════════════════════════════════════════════════════════════
with tab_agent_level:
    st.subheader("Agent-Level Performance")
    st.caption(
        "One row per agent. First-pitch tier rates show share of that agent's calls "
        "where each tier was pitched first. Conversion and GCV metrics are per-call "
        "and per-first-pitch. GCV / 1st Pitch is an expected value (all calls, not just converting). "
        "All sidebar filters apply."
    )

    agent_needed = {
        "agent_name", "first_pitch_type",
        "gcv_on_first_pitch", "order_count", "gcv", "points",
    }

    if agent_needed.issubset(df.columns):

        al_c1, al_c2, al_c3 = st.columns([2, 2, 1])
        with al_c1:
            agent_search = st.text_input("Search Agent Name", key="agent_search", placeholder="Type to filter…")
        with al_c2:
            sort_col = st.selectbox(
                "Sort by",
                ["Calls", "Diamond %", "Gold %", "Silver %", "Bronze %",
                 "1st Pitch CR", "Overall CR", "GCV / Call", "GCV / 1st Pitch", "Points / Call"],
                key="agent_sort_col",
            )
        with al_c3:
            sort_asc = st.radio("Order", ["Desc", "Asc"], horizontal=True, key="agent_sort_order") == "Asc"

        ag = df.copy()
        if agent_search:
            ag = ag[ag["agent_name"].astype(str).str.contains(agent_search, case=False, na=False)]

        def agent_agg(g):
            n = len(g)
            fp_counts = g["first_pitch_type"].value_counts()
            def fp_pct(tier):
                return fp_counts.get(tier, 0) / n * 100 if n else float("nan")

            fp_cr    = (g["gcv_on_first_pitch"] > 0).mean() * 100
            ov_cr    = (g["order_count"] > 0).mean() * 100
            gcv_call = g["gcv"].mean()
            # FIX: GCV / 1st Pitch EV = mean over ALL calls (zeros included)
            gcv_fp   = g["gcv_on_first_pitch"].mean()
            pts_call = g["points"].mean() if "points" in g.columns else float("nan")

            return pd.Series({
                "Calls":               n,
                "Diamond %":           fp_pct("Diamond"),
                "Gold %":              fp_pct("Gold"),
                "Silver %":            fp_pct("Silver"),
                "Bronze %":            fp_pct("Bronze"),
                "1st Pitch CR":        fp_cr,
                "Overall CR":          ov_cr,
                "GCV / Call":          gcv_call,
                "GCV / 1st Pitch": gcv_fp,
                "Points / Call":       pts_call,
            })

        agent_df = (
            ag.groupby("agent_name")
            .apply(agent_agg)
            .reset_index()
            .rename(columns={"agent_name": "Agent"})
        )

        if sort_col in agent_df.columns:
            agent_df = agent_df.sort_values(sort_col, ascending=sort_asc)

        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Agents", f"{len(agent_df):,}")
        sc2.metric("Avg Diamond %", f"{agent_df['Diamond %'].mean():.1f}%")
        sc3.metric("Avg 1st Pitch CR", f"{agent_df['1st Pitch CR'].mean():.1f}%")
        sc4.metric("Avg GCV / Call", f"${agent_df['GCV / Call'].mean():,.0f}")

        dc1, dc2 = st.columns(2)

        with dc1:
            st.markdown("**Diamond % Distribution**")
            fig_d = go.Figure(go.Histogram(
                x=agent_df["Diamond %"], nbinsx=20,
                marker_color="#3d8ef8", opacity=0.8,
                marker_line_color="#252b3a", marker_line_width=1,
            ))
            apply_dark_theme(fig_d,
                xaxis_title="Diamond First-Pitch Rate (%)",
                yaxis_title="Agents",
                height=240,
                margin=dict(l=40, r=20, t=10, b=40),
            )
            st.plotly_chart(fig_d, use_container_width=True)

        with dc2:
            st.markdown("**GCV / Call Distribution**")
            fig_g = go.Figure(go.Histogram(
                x=agent_df["GCV / Call"].dropna(), nbinsx=20,
                marker_color="#22d3c8", opacity=0.8,
                marker_line_color="#252b3a", marker_line_width=1,
            ))
            apply_dark_theme(fig_g,
                xaxis_title="GCV / Call ($)",
                yaxis_title="Agents",
                height=240,
                margin=dict(l=40, r=20, t=10, b=40),
            )
            st.plotly_chart(fig_g, use_container_width=True)

        fmt_df = agent_df.copy()
        for col in ["Diamond %", "Gold %", "Silver %", "Bronze %", "1st Pitch CR", "Overall CR"]:
            fmt_df[col] = fmt_df[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
        for col in ["GCV / Call", "GCV / 1st Pitch"]:
            fmt_df[col] = fmt_df[col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
        fmt_df["Points / Call"] = fmt_df["Points / Call"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        fmt_df["Calls"] = fmt_df["Calls"].apply(lambda x: f"{x:,}")

        st.dataframe(fmt_df, use_container_width=True, hide_index=True)

    else:
        missing = agent_needed - set(df.columns)
        st.info(f"Columns missing for agent table: {', '.join(sorted(missing))}")