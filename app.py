import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
from datetime import timedelta

st.set_page_config(
    page_title="Arcadia | Agent Performance",
    page_icon="⚡",
    layout="wide",
)

# ── Load data ─────────────────────────────────────────────────────────────────
DATA_FILE = "call_level_pitches_and_recs.csv"

@st.cache_data(ttl=3600)
def load_data():
    df = pd.read_csv(DATA_FILE)
    if "call_date" in df.columns:
        df["call_date"] = pd.to_datetime(df["call_date"], errors="coerce")
    for col in ["pitches_in_order", "recommended_in_order", "pitch_types_in_order",
                "pitches_canonical_in_order", "recommended_plan_types_in_order"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    for col in ["order_count", "gcv", "gcv_on_first_pitch", "points",
                "adhered_call", "slide_call", "all_plans_call"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

if not os.path.exists(DATA_FILE):
    st.warning("No data file found. The daily query may not have run yet.")
    st.stop()

df_raw = load_data()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Filters")

    # 1. Date range — top of sidebar, defaults to last 7 days
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

    # 2. Remaining filters — no default selection (empty = show all)
    centers_opts  = sorted(df_raw["center_location"].dropna().unique().tolist()) if "center_location" in df_raw.columns else []
    mkt_opts      = sorted(df_raw["marketing_bucket"].dropna().unique().tolist()) if "marketing_bucket" in df_raw.columns else []
    serp_opts     = sorted(df_raw["site_serp"].dropna().unique().tolist()) if "site_serp" in df_raw.columns else []
    mov_opts      = sorted(df_raw["mover_switcher"].dropna().unique().tolist()) if "mover_switcher" in df_raw.columns else []
    quartile_opts = sorted(df_raw["performance_quartile"].dropna().unique().tolist()) if "performance_quartile" in df_raw.columns else []

    center_defaults = [c for c in ["Durban", "Jamaica"] if c in centers_opts]
    sel_center   = st.multiselect("Center",           options=centers_opts,  default=center_defaults, key="filter_center")
    sel_mkt      = st.multiselect("Marketing Bucket", options=mkt_opts,      default=[], key="filter_mkt")
    sel_serp     = st.multiselect("Site / SERP",      options=serp_opts,     default=[], key="filter_serp")
    sel_mov      = st.multiselect("Mover / Switcher", options=mov_opts,      default=[], key="filter_mov")
    sel_quartile = st.multiselect("Agent Quartile",   options=quartile_opts, default=[], key="filter_quartile")

    rec_type_opts = sorted(df_raw["top_recommended_plan_type"].dropna().unique().tolist()) if "top_recommended_plan_type" in df_raw.columns else []
    sel_rec_type  = st.multiselect("Rec Product Type", options=rec_type_opts, default=[], key="filter_rec_type")

    # 3. Happy path — on by default
    happy_only = st.toggle("Happy Path Calls Only", value=True, key="filter_happy_path")

# ── Apply filters (two versions: with and without date filter) ────────────────
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
    if sel_rec_type and "top_recommended_plan_type" in d.columns:
        d = d[d["top_recommended_plan_type"].isin(sel_rec_type)]
    return d

# df_nodatefilter: all filters applied EXCEPT date — used for trend cards & KPI deltas
df_nodatefilter = apply_non_date_filters(df_raw)

# df: full filter set including date
df = df_nodatefilter.copy()
if date_range and len(date_range) == 2 and "call_date" in df.columns:
    df = df[(df["call_date"].dt.date >= date_range[0]) & (df["call_date"].dt.date <= date_range[1])]

# ── Shared helpers ────────────────────────────────────────────────────────────
PERIOD_OPTIONS = ["Daily", "Weekly", "Monthly"]
PERIOD_CODE    = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
PERIOD_FMT     = {"Daily": "%b %d", "Weekly": "%b %d", "Monthly": "%b %Y"}

def period_start_dates(date_series: pd.Series, period: str) -> pd.Series:
    """Return the period start as a proper datetime — sortable and Plotly-friendly."""
    code = PERIOD_CODE[period]
    return date_series.dt.to_period(code).apply(lambda p: p.start_time)

def period_labels(date_series: pd.Series, period: str) -> pd.Series:
    """Return sortable ISO date strings (YYYY-MM-DD) for groupby keys."""
    return period_start_dates(date_series, period).dt.strftime("%Y-%m-%d")

def period_display(label_series: pd.Series, period: str) -> pd.Series:
    """Convert YYYY-MM-DD groupby keys to human-readable axis labels."""
    fmt = PERIOD_FMT[period]
    return pd.to_datetime(label_series).dt.strftime(fmt)

def fmt_week(s):
    try:
        return pd.to_datetime(str(s).split("/")[0]).strftime("%b %d")
    except Exception:
        return str(s)

def week_mix(source_df, plan_type):
    """Return (this_week_pct, prev_week_pct) for a given plan type using the two most recent full weeks."""
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
    """Compute metric for last 7 days vs prior 7 days from source_df (date-unfiltered)."""
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

st.title("⚡ Arcadia Performance")
st.caption(f"{date_str}  ·  {len(df):,} calls in view")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_model, tab_agent = st.tabs(["Model Outputs", "Agent Behavior & Performance"])


# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — MODEL OUTPUTS
# ════════════════════════════════════════════════════════════════════════════════
with tab_model:

    # ── Section 1: Recommendation Mix ────────────────────────────────────────
    st.subheader("Recommendation Mix Over Time")

    # Trend cards — last full week vs prior week, static (df_nodatefilter)
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
        index=0,
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

        # Plain (non-stacked) line chart
        fig_mix = go.Figure()
        for pt in plan_types:
            sub = rec_ts[rec_ts["top_recommended_plan_type"] == pt]
            fig_mix.add_trace(go.Scatter(
                x=sub["period_display"], y=sub["pct"],
                name=pt, mode="lines+markers",
            ))
        fig_mix.update_layout(
            yaxis_ticksuffix="%", height=340,
            margin=dict(l=40, r=20, t=10, b=40),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_mix, use_container_width=True)

        # Table: plan types as rows, periods as columns (use display labels as column headers)
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

        # ── Row A: headline probability summary cards ─────────────────────────
        # Show avg raw prob for each product type, plus avg confidence gap
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

        # ── Row B: raw probability distributions + confidence gap histogram ───
        st.markdown("**Raw Probability Distributions**")
        st.caption(
            "Left: violin plots of each product's raw conversion probability across all calls, regardless "
            "of what was recommended. This shows the model's baseline view of each product type. "
            "Right: histogram of the expected-points gap between the #1 and #2 recommendations. "
            "A right-skewed distribution means the model usually has a clear preferred product; "
            "a spike near zero means many calls are close-call toss-ups."
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
            fig_violin.update_layout(
                yaxis_title="Raw Conversion Probability",
                yaxis_tickformat=".0%",
                height=320,
                margin=dict(l=40, r=20, t=10, b=40),
                showlegend=False,
            )
            st.plotly_chart(fig_violin, use_container_width=True)

        with rb2:
            gap_vals = df["expected_points_gap_1_2"].dropna()
            # Confidence tiers: Low <25th pct, Medium 25-75th, High >75th
            p25, p75 = gap_vals.quantile(0.25), gap_vals.quantile(0.75)
            pct_low  = (gap_vals < p25).mean() * 100
            pct_high = (gap_vals > p75).mean() * 100
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(x=gap_vals, nbinsx=40))
            fig_hist.add_vline(x=float(p25), line_dash="dash", line_color="gray",
                               annotation_text=f"25th pct ({p25:.2f})", annotation_position="top right")
            fig_hist.add_vline(x=float(p75), line_dash="dash", line_color="gray",
                               annotation_text=f"75th pct ({p75:.2f})", annotation_position="top left")
            fig_hist.update_layout(
                xaxis_title="Expected Points Gap (#1 vs #2)",
                yaxis_title="Calls",
                height=320,
                margin=dict(l=40, r=20, t=10, b=40),
                showlegend=False,
            )
            st.plotly_chart(fig_hist, use_container_width=True)
            st.caption(f"**{pct_low:.0f}%** of calls are low-confidence (gap < {p25:.2f} pts) · "
                       f"**{pct_high:.0f}%** are high-confidence (gap > {p75:.2f} pts)")

        # ── Row D: confidence gap quintile × adherence & outcomes ─────────────
        st.markdown("**Does Model Confidence Predict Outcome? — By Confidence Gap Quintile**")
        st.caption(
            "Calls bucketed by the model's confidence gap (expected-points difference between #1 and #2). "
            "Left: do agents adhere more when the model is confident? "
            "Right: does following the recommendation pay off more when the model is confident? "
            "A well-calibrated model should show widening benefit-of-adherence as confidence increases."
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
                customdata=adh_by_gap[["calls", "gap_med"]],
                hovertemplate="Calls: %{customdata[0]:,}<br>Median gap: %{customdata[1]:.2f}<extra></extra>",
            ))
            fig_adh_gap.update_layout(
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
                def outcome_agg(x, metric):
                    if metric == "GCV / Call":
                        return x.mean()
                    return (x > 0).mean() * 100

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
                        line=dict(dash=dash),
                    ))
                fig_out.update_layout(
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
        top_gcv_fp   = safe_mean(top_df.loc[top_df["gcv_on_first_pitch"] > 0, "gcv_on_first_pitch"])
        slide_gcv_fp = safe_mean(slide_df.loc[slide_df["gcv_on_first_pitch"] > 0, "gcv_on_first_pitch"])

        # Headline metrics
        ca1, ca2, ca3, ca4 = st.columns(4)
        ca1.metric("Top Rec — 1st Pitch CR",  f"{top_fp_cr:.1f}%",
                   delta=f"{top_fp_cr - slide_fp_cr:+.1f}pp vs Slide")
        ca2.metric("Slide — 1st Pitch CR",    f"{slide_fp_cr:.1f}%")
        ca3.metric("Top Rec — GCV / Call",    f"${top_gcv:,.0f}",
                   delta=f"${top_gcv - slide_gcv:+,.0f} vs Slide")
        ca4.metric("Slide — GCV / Call",      f"${slide_gcv:,.0f}")

        # Side-by-side bar charts: CR and GCV
        cb1, cb2 = st.columns(2)

        with cb1:
            st.markdown("**Conversion Rate**")
            fig_cr = go.Figure()
            fig_cr.add_trace(go.Bar(
                name="Top Rec", x=["1st Pitch CR", "Overall CR"],
                y=[top_fp_cr, top_cr],
                text=[f"{top_fp_cr:.1f}%", f"{top_cr:.1f}%"], textposition="outside",
            ))
            fig_cr.add_trace(go.Bar(
                name="Slide", x=["1st Pitch CR", "Overall CR"],
                y=[slide_fp_cr, slide_cr],
                text=[f"{slide_fp_cr:.1f}%", f"{slide_cr:.1f}%"], textposition="outside",
            ))
            fig_cr.update_layout(barmode="group", yaxis_ticksuffix="%", height=300,
                                 margin=dict(l=40, r=20, t=10, b=40),
                                 legend=dict(orientation="h", y=-0.25))
            st.plotly_chart(fig_cr, use_container_width=True)

        with cb2:
            st.markdown("**GCV**")
            fig_gcv = go.Figure()
            fig_gcv.add_trace(go.Bar(
                name="Top Rec", x=["GCV / Call", "GCV / 1st Pitch"],
                y=[top_gcv, top_gcv_fp],
                text=[f"${top_gcv:,.0f}", f"${top_gcv_fp:,.0f}"], textposition="outside",
            ))
            fig_gcv.add_trace(go.Bar(
                name="Slide", x=["GCV / Call", "GCV / 1st Pitch"],
                y=[slide_gcv, slide_gcv_fp],
                text=[f"${slide_gcv:,.0f}", f"${slide_gcv_fp:,.0f}"], textposition="outside",
            ))
            fig_gcv.update_layout(barmode="group", yaxis_tickprefix="$", height=300,
                                  margin=dict(l=40, r=20, t=10, b=40),
                                  legend=dict(orientation="h", y=-0.25))
            st.plotly_chart(fig_gcv, use_container_width=True)

        # Breakdown by plan type
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
                )
                .reset_index()
            )
            plan_cmp["fp_cr"]      = (plan_cmp["fp_cr"] * 100).round(1).astype(str) + "%"
            plan_cmp["overall_cr"] = (plan_cmp["overall_cr"] * 100).round(1).astype(str) + "%"
            plan_cmp["gcv_call"]   = plan_cmp["gcv_call"].round(0).apply(lambda x: f"${x:,.0f}")
            plan_cmp = plan_cmp.rename(columns={
                "top_recommended_plan_type": "Plan Type",
                "classification_bucket": "Pitched",
                "calls": "Calls",
                "fp_cr": "1st Pitch CR",
                "overall_cr": "Overall CR",
                "gcv_call": "GCV / Call",
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
        index=1,
        horizontal=True,
        key="agent_granularity",
    )

    # ── First pitch tier trend cards (date-filter-immune, uses df_nodatefilter) ──
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
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["adherence"], name="Adherence", mode="lines+markers"))
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["slide"], name="Slide", mode="lines+markers", line=dict(dash="dot")))
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["all_plans"], name="All Plans", mode="lines+markers", line=dict(dash="dash")))
            fig.update_layout(yaxis_ticksuffix="%", height=320,
                              margin=dict(l=40, r=20, t=20, b=40),
                              legend=dict(orientation="h", y=-0.2))
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
                fig2.add_trace(go.Scatter(x=sub["period_display"], y=sub["pct"], name=pt, mode="lines+markers"))
            fig2.update_layout(yaxis_ticksuffix="%", height=320,
                               margin=dict(l=40, r=20, t=20, b=40),
                               legend=dict(orientation="h", y=-0.2))
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

        # Apply first pitch type filter
        if pot_fp_filter != "All" and "first_pitch_type" in pot_df.columns:
            pot_df = pot_df[pot_df["first_pitch_type"] == pot_fp_filter]

        # Group by granularity period
        pot_df["period"] = period_labels(pot_df["call_date"], agent_granularity)

        def pot_agg(grp):
            if pot_metric == "1st Pitch CR":
                return (grp["gcv_on_first_pitch"] > 0).mean() * 100
            elif pot_metric == "Overall CR":
                return (grp["order_count"] > 0).mean() * 100
            elif pot_metric == "GCV / 1st Pitch":
                s = grp.loc[grp["gcv_on_first_pitch"] > 0, "gcv_on_first_pitch"]
                return s.mean() if len(s) else float("nan")
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
            ))
            fig_pot.update_layout(
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

        # Default to last two full ISO weeks
        raw_min = df_raw["call_date"].min().date()
        raw_max = df_raw["call_date"].max().date()

        import datetime as _dt
        _last_mon  = raw_max - timedelta(days=raw_max.weekday())          # Mon of current/last week
        _post_def_start = _last_mon - timedelta(days=7)                   # Mon of most recent full week
        _post_def_end   = _post_def_start + timedelta(days=6)             # Sun of most recent full week
        _pre_def_start  = _post_def_start - timedelta(days=7)             # Mon of prior week
        _pre_def_end    = _pre_def_start + timedelta(days=6)              # Sun of prior week
        # Clamp to available data
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

            # Apply all non-date sidebar filters to raw data, then split by period
            def slice_period(base, start, end):
                return base[
                    (base["call_date"].dt.date >= start) &
                    (base["call_date"].dt.date <= end)
                ]

            pre_df  = slice_period(df_nodatefilter, pre_range[0],  pre_range[1])
            post_df = slice_period(df_nodatefilter, post_range[0], post_range[1])

            # ── Headline metric cards ─────────────────────────────────────────
            def overall_metric(source, metric):
                if len(source) == 0:
                    return float("nan")
                if metric == "fp_cr":
                    return (source["gcv_on_first_pitch"] > 0).mean() * 100
                if metric == "ov_cr":
                    return (source["order_count"] > 0).mean() * 100
                if metric == "gcv_fp":
                    s = source.loc[source["gcv_on_first_pitch"] > 0, "gcv_on_first_pitch"]
                    return s.mean() if len(s) else float("nan")
                if metric == "gcv_call":
                    return source["gcv"].mean()
                return float("nan")

            hm_specs = [
                ("fp_cr",    "1st Pitch CR",    "pct"),
                ("ov_cr",    "Overall CR",      "pct"),
                ("gcv_fp",   "GCV / 1st Pitch", "dollar"),
                ("gcv_call", "GCV / Call",      "dollar"),
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
                """Aggregate per rec_type × behavior bucket."""
                if len(source) == 0:
                    return pd.DataFrame()

                # Total calls per rec type — for mix denominator
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
                        gcv_fp   = sub.loc[sub["gcv_on_first_pitch"] > 0, "gcv_on_first_pitch"].mean() if n_sub > 0 else float("nan")
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
                    ("mix",      "Mix",                "pct",    False),
                    ("fp_cr",    "First Pitch CR",     "pct",    False),
                    ("ov_cr",    "Overall CR",         "pct",    False),
                    ("gcv_fp",   "GCV / First Pitch",  "dollar", True),
                    ("gcv_call", "GCV / Call",         "dollar", True),
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

                def delta_color(pre, post, higher_is_better):
                    if pd.isna(pre) or pd.isna(post) or pre == 0:
                        return ""
                    pct_chg = (post - pre) / abs(pre) * 100
                    if abs(pct_chg) < 3:
                        return "background-color: #fff9c4"   # yellow — negligible
                    if (pct_chg > 0) == higher_is_better:
                        return "background-color: #c8e6c9"   # green
                    return "background-color: #ffcdd2"        # red

                # Build display rows
                BEH_ORDER = ["Adhered", "Slide", "All Plans"]
                rec_types = sorted(merged["rec_type"].unique())

                display_rows = []
                style_map    = {}  # (row_idx, col_name) -> css

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
                            delta_str = fmt_delta(pre_v, post_v, fmt)
                            row[f"{label} Delta"] = delta_str
                            style_map[(row_idx, f"{label} Delta")] = delta_color(pre_v, post_v, hib)
                        display_rows.append(row)
                        row_idx += 1

                display_df = pd.DataFrame(display_rows)

                # Apply cell styling via pandas Styler
                def apply_styles(styler):
                    for (ri, col), css in style_map.items():
                        if css and col in styler.data.columns:
                            styler.data  # ensure exists
                            styler = styler.set_properties(
                                subset=pd.IndexSlice[ri, col], **{"background-color": css.split(": ")[1]}
                            )
                    return styler

                # Build styler manually with applymap-style logic
                delta_cols = [f"{label} Delta" for _, label, _, _ in METRICS]

                def color_delta_cell(val):
                    if val == "—" or val == "":
                        return ""
                    try:
                        num = float(val.replace("%", "").replace("+", ""))
                    except Exception:
                        return ""
                    if abs(num) < 3:
                        return "background-color: #fff9c4"
                    if num > 0:
                        return "background-color: #c8e6c9"
                    return "background-color: #ffcdd2"

                # We need to know which delta cols are "higher is bad" (none in this table)
                # All metrics here: higher mix/CR/GCV = good, so positive delta = green
                styler = display_df.style.map(color_delta_cell, subset=delta_cols)

                # Column order: Rec Type, Behavior, then metric groups
                col_order = ["Rec Type", "Behavior"]
                for _, label, _, _ in METRICS:
                    col_order += [f"{label} {pre_label}", f"{label} {post_label}", f"{label} Delta"]

                styler = styler.set_properties(**{"text-align": "right"}, subset=col_order[2:])
                styler = styler.set_properties(**{"text-align": "left"},  subset=["Rec Type", "Behavior"])

                st.dataframe(styler, use_container_width=True, hide_index=True, column_order=col_order)

                # ── Overall (no rec type split) comparison table ──────────────
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
                        gcv_fp   = sub.loc[sub["gcv_on_first_pitch"] > 0, "gcv_on_first_pitch"].mean() if n_sub > 0 else float("nan")
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

    # ── Confusion Matrix: First Pitch vs. Recommended Plan Type ───────────────
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

        def norm_plan_type(x):
            if not isinstance(x, str):
                return None
            x = x.strip().lower()
            if "fixed"  in x: return "Fixed"
            if "tier"   in x: return "Tiered"
            if "bund"   in x: return "Bundled"
            return None

        def safe_parse_list(v):
            # Robustly parse rec type lists however Spark/CSV serialised them.
            # Handles: actual list, ['a','b'], [a, b], "a,b", None/nan strings
            if isinstance(v, list):
                return v
            if not isinstance(v, str):
                return []
            s = v.strip()
            if s in ("", "None", "nan", "null", "[]"):
                return []
            if s.startswith("[") and s.endswith("]"):
                inner = s[1:-1].strip()
                if not inner:
                    return []
                try:
                    result = _ast.literal_eval(s)
                    if isinstance(result, list):
                        return [str(x).strip() for x in result]
                except Exception:
                    pass
                # Unquoted Spark output e.g. [Fixed, Tiered, Bundled]
                parts = inner.split(",")
                return [p.strip().strip("'").strip('"') for p in parts if p.strip()]
            parts = s.split(",")
            return [p.strip().strip("'").strip('"') for p in parts if p.strip()]

        cm_df = df.dropna(subset=["first_pitch_type", "top_recommended_plan_type"]).copy()
        # Also exclude rows where recommended_plan_types_in_order is a null-ish string
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

        # ── Debug expander — helps diagnose misclassified rows ────────────────
        with st.expander("🔍 Debug: row classification sample (remove when confirmed)"):
            st.markdown("**Sample of Diamond calls where top rec = Tiered**")
            tiered_diamond = cm_df[
                (cm_df["first_pitch_type"] == "Diamond") &
                (cm_df["top_recommended_plan_type"].str.lower().str.contains("tier", na=False))
            ][["first_pitch_type", "first_pitch_plan_category", "top_recommended_plan_type",
               "recommended_plan_types_in_order", "_rec_types", "row_label", "col_label"]].head(20)
            st.dataframe(tiered_diamond, use_container_width=True)

            st.markdown("**Sample of Gold calls where top rec = Tiered**")
            tiered_gold = cm_df[
                (cm_df["first_pitch_type"] == "Gold") &
                (cm_df["top_recommended_plan_type"].str.lower().str.contains("tier", na=False))
            ][["first_pitch_type", "first_pitch_plan_category", "top_recommended_plan_type",
               "recommended_plan_types_in_order", "_rec_types", "row_label", "col_label"]].head(20)
            st.dataframe(tiered_gold, use_container_width=True)

            st.markdown("**row_label value counts**")
            st.dataframe(cm_df["row_label"].value_counts().reset_index(), use_container_width=True)

            st.markdown("**col_label value counts**")
            st.dataframe(cm_df["col_label"].value_counts().reset_index(), use_container_width=True)

            st.markdown("**Sample raw recommended_plan_types_in_order strings (first 10)**")
            st.write(cm_df["recommended_plan_types_in_order"].dropna().head(10).tolist())

        cm_df = cm_df[cm_df["row_label"].notna() & cm_df["col_label"].notna()]

        total_calls = len(cm_df)
        ROW_LABELS = ["Fixed", "Tiered", "Bundled", "Other"]
        COL_LABELS = ["Fixed", "Tiered", "Bundled"]

        # Build cell data
        z_counts   = []   # heatmap color values (call count)
        text_cells = []   # annotation strings

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
            colorscale="Blues",
            colorbar=dict(title="Call count"),
            hoverongaps=False,
        ))
        fig_cm.update_layout(
            xaxis=dict(title="Recommended plan type (top_recommended_plan_type)", side="bottom"),
            yaxis=dict(title="First pitch (canonical rec match → plan type)", autorange="reversed"),
            height=460,
            margin=dict(l=100, r=40, t=20, b=80),
            font=dict(size=12),
        )
        st.plotly_chart(fig_cm, use_container_width=True)
        st.caption(f"Total calls in view: {total_calls:,}  ·  "
                   f"Diamond: {(cm_df['first_pitch_type']=='Diamond').sum():,}  ·  "
                   f"Gold: {(cm_df['first_pitch_type']=='Gold').sum():,}  ·  "
                   f"Other (Silver/Bronze): {cm_df['row_label'].eq('Other').sum():,}")
    else:
        missing = cm_needed - set(df.columns)
        st.info(f"Columns missing for confusion matrix: {', '.join(sorted(missing))}")