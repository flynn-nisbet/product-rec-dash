import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import os
import glob
import hashlib
import plotly.graph_objects as go
from datetime import date, timedelta
from openai import OpenAI as _OpenAI

from ai_analyst_prompt import AI_ANALYST_SYSTEM_PROMPT
from dotenv import load_dotenv
import traceback as _tb
import json as _json

load_dotenv()

st.set_page_config(
    page_title="Product Rank Dash",
    page_icon="📊",
    layout="wide",
)

import theme

theme.init_browser_query_state()

from charts import (
    PLOT_COLORWAY,
    apply_chart_theme,
    area_fill_primary,
    bar_outside_textfont,
    chart_hist_stroke_and_title,
    chart_hline_reference,
    chart_muted,
    heatmap_colorbar_dict,
    heatmap_colorscale,
    histogram_marker_line,
    plotly_axis_lines,
)

# Marketing buckets treated as Brand for the Brand/Non-Brand sidebar shortcut
# (matches pipeline values such as ``Brand-Partner``; includes a space variant if present).
BRAND_MARKETING_BUCKETS = frozenset({"Brand-Partner", "Brand Partner", "Competitor", "NRG"})
_BRAND_LOWER = {b.lower() for b in BRAND_MARKETING_BUCKETS}


def _marketing_bucket_is_brand(series: pd.Series) -> pd.Series:
    """True where ``marketing_bucket`` is one of the Brand shortcut buckets (case-insensitive)."""
    mb = series
    ok = mb.notna()
    norm = mb.astype(str).str.strip().str.lower()
    return ok & norm.isin(_BRAND_LOWER)


# ── Load data ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl="24h")
def load_data():
    """Load call-level rows from ``data/call_level_data_*.csv`` shards (union), or legacy ``call_level_data.csv``."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    pattern = os.path.join(data_dir, "call_level_data_*.csv")
    paths = sorted(glob.glob(pattern))
    if paths:
        parts = [pd.read_csv(p) for p in paths]
        return pd.concat(parts, ignore_index=True)
    legacy = os.path.join(base_dir, "call_level_data.csv")
    if os.path.isfile(legacy):
        return pd.read_csv(legacy)
    raise FileNotFoundError(
        f"No call-level data: expected sharded CSVs matching {pattern!r}, or {legacy!r}."
    )

df_raw = load_data()
df_raw["call_date"] = pd.to_datetime(df_raw["call_date"])

PERIOD_OPTIONS = ["Daily", "Weekly", "Monthly"]
SALE_TIER_ORDER = ["Diamond", "Gold", "Silver", "Bronze"]
PERIOD_CODE = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
PERIOD_FMT = {"Daily": "%b %d", "Weekly": "%b %d", "Monthly": "%b %Y"}


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


# ── Sidebar: Filters ────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")

    if "call_date" in df_raw.columns and df_raw["call_date"].notna().any():
        min_d = pd.to_datetime(df_raw["call_date"].min()).date()
        max_d = pd.to_datetime(df_raw["call_date"].max()).date()
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

    st.selectbox(
        "Time granularity",
        options=PERIOD_OPTIONS,
        index=0,
        key="global_granularity",
        help="Used for time bucketing in Model Outputs, Agent Behavior & Performance, and Sale Mixes.",
    )

    centers_opts  = sorted(df_raw["center_location"].dropna().unique().tolist()) if "center_location" in df_raw.columns else []
    mkt_opts      = sorted(df_raw["marketing_bucket"].dropna().unique().tolist()) if "marketing_bucket" in df_raw.columns else []
    serp_opts     = sorted(df_raw["site_serp"].dropna().unique().tolist()) if "site_serp" in df_raw.columns else []
    mov_opts      = sorted(df_raw["mover_switcher"].dropna().unique().tolist()) if "mover_switcher" in df_raw.columns else []
    quartile_opts = sorted(df_raw["performance_quartile"].dropna().unique().tolist()) if "performance_quartile" in df_raw.columns else []

    # ── NEW: Agent multi-select ───────────────────────────────────────────────
    agent_opts = sorted(df_raw["agent_name"].dropna().unique().tolist()) if "agent_name" in df_raw.columns else []

    center_defaults = [c for c in ["Durban", "Jamaica"] if c in centers_opts]
    sel_center   = st.multiselect("Center",           options=centers_opts,  default=center_defaults, key="filter_center")
    sel_brand_nonbrand = st.multiselect(
        "Brand/Non-Brand",
        options=["Brand", "Non-Brand"],
        default=[],
        key="filter_brand_nonbrand",
        help="Shortcut: Brand = Brand-Partner, Competitor, and NRG; Non-Brand = all other buckets. "
        "Leave empty for no filter. Refines together with Marketing Bucket when that is also set. "
        "Selecting both is equivalent to no filter.",
    )
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

    happy_path_tf = st.selectbox(
        "Happy Path Only",
        options=["True", "False"],
        index=0,
        key="filter_happy_path_only",
        help="True: happy_path = 1 (Arcadia target, no failed qualification, no Payless pitch, no Low rec). False: all calls.",
    )
    happy_only = happy_path_tf == "True"

    st.divider()

# ── Apply filters ─────────────────────────────────────────────────────────────
def apply_non_date_filters(base):
    d = base.copy()
    if happy_only and "happy_path" in d.columns:
        d = d[d["happy_path"] == 1]
    if sel_center and "center_location" in d.columns:
        d = d[d["center_location"].isin(sel_center)]
    if "marketing_bucket" in d.columns and sel_brand_nonbrand:
        _is_brand = _marketing_bucket_is_brand(d["marketing_bucket"])
        _sel_bn = set(sel_brand_nonbrand)
        if _sel_bn == {"Brand", "Non-Brand"}:
            pass
        elif "Brand" in _sel_bn:
            d = d.loc[_is_brand]
        elif "Non-Brand" in _sel_bn:
            d = d.loc[~_is_brand]
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
def report_through_date() -> date:
    """Last full calendar day for WTD / P4WA comparisons (excludes unreliable intra-day today)."""
    return date.today() - timedelta(days=1)


def monday_of_week_containing(d: date) -> date:
    """Monday-start calendar week containing ``d`` (``weekday()``: Mon=0 … Sun=6)."""
    return d - timedelta(days=d.weekday())


def default_period_comparison_week_ranges(
    data_max: date, data_min: date
) -> tuple[tuple[date, date], tuple[date, date]]:
    """Default Pre/Post ranges for period-over-period date pickers.

    **Post** is the most recent complete Monday–Sunday week that ends on or before ``data_max``.
    **Pre** is the four Monday–Sunday weeks immediately before that week (28 consecutive days,
    ending the Sunday before the post week starts).

    Ranges are clipped to ``[data_min, data_max]`` with **pre strictly before post** and
    ``pre_start <= pre_end`` so ``st.date_input`` always receives valid ordered tuples.
    """
    if data_max < data_min:
        data_min, data_max = data_max, data_min

    last_sun = data_max - timedelta(days=(data_max.weekday() + 1) % 7)
    post_start = last_sun - timedelta(days=6)
    post_end = last_sun
    post_start = max(post_start, data_min)
    post_end = min(post_end, data_max)
    if post_start > post_end:
        post_start = post_end = data_max

    pre_end = post_start - timedelta(days=1)
    pre_start = pre_end - timedelta(days=27)
    pre_start = max(pre_start, data_min)
    pre_end = min(pre_end, data_max, post_start - timedelta(days=1))
    if pre_start > pre_end:
        pre_end = min(post_start - timedelta(days=1), data_max)
        pre_start = max(data_min, pre_end - timedelta(days=27))
    if pre_start > pre_end:
        pre_start = pre_end = max(data_min, min(pre_end, post_start - timedelta(days=1)))
    if pre_end >= post_start:
        pre_end = post_start - timedelta(days=1)
        pre_start = max(data_min, pre_end - timedelta(days=27))
        if pre_start > pre_end:
            pre_start = pre_end
    return (pre_start, pre_end), (post_start, post_end)


def streamlit_safe_period_defaults(
    data_max: date, data_min: date
) -> tuple[tuple[date, date], tuple[date, date]]:
    """Pre/Post ranges for ``st.date_input`` that always lie in ``[data_min, data_max]``.

    ``default_period_comparison_week_ranges`` can produce pre/post edges outside a **narrow**
    filtered date window (e.g. sidebar filter). Streamlit requires every date in ``value=`` to
    satisfy ``min_value``/``max_value``. When clipped ranges overlap or invert, this falls back
    to splitting the available span (first half vs second half).
    """
    if data_max < data_min:
        data_min, data_max = data_max, data_min
    if data_min == data_max:
        t = (data_min, data_max)
        return t, t

    pre_t, post_t = default_period_comparison_week_ranges(data_max, data_min)

    def _clip_pair(t: tuple[date, date]) -> tuple[date, date] | None:
        s, e = sorted(t)
        a = max(data_min, min(s, data_max))
        b = max(data_min, min(e, data_max))
        if a <= b:
            return (a, b)
        return None

    pre = _clip_pair(pre_t)
    post = _clip_pair(post_t)
    if pre is not None and post is not None and pre[1] < post[0]:
        return pre, post

    n = (data_max - data_min).days + 1
    k = max(1, n // 2)
    pre_end = data_min + timedelta(days=k - 1)
    post_start = pre_end + timedelta(days=1)
    if post_start > data_max:
        return (data_min, data_min), (data_max, data_max)
    return (data_min, pre_end), (post_start, data_max)


def _extract_ranked_slot_product(series_str, slot_idx: int):
    """Parse a list-like recommendation string; return the product at ``slot_idx`` (0=Diamond, 1=Gold)."""
    import re as _re_slot

    if not isinstance(series_str, str) or series_str.strip() in ("", "None", "nan", "null", "[]"):
        return None
    items = _re_slot.findall(
        r"'([^']+)'|\"([^\"]+)\"|([^\[\],\s][^\[\],]*[^\[\],\s]|[^\[\],\s]+)",
        series_str,
    )
    flat = [next(g for g in grp if g) for grp in items]
    flat = [f.strip() for f in flat if f.strip() and f.strip() not in ("None", "nan", "null")]
    return flat[slot_idx] if slot_idx < len(flat) else None


def _ai_analyst_time_bundle(raw: pd.DataFrame) -> dict:
    """Dates for WTD/MTD/P4WA aligned with dashboard: min(report_through_date(), max call_date)."""
    cal_today = date.today()
    rtd = report_through_date()
    if "call_date" in raw.columns and raw["call_date"].notna().any():
        data_max = pd.to_datetime(raw["call_date"].max()).date()
        as_of = min(rtd, data_max)
    else:
        data_max = None
        as_of = rtd
    wtd_start = monday_of_week_containing(as_of)
    mtd_start = date(as_of.year, as_of.month, 1)
    ytd_start = date(as_of.year, 1, 1)
    p4_start = wtd_start - timedelta(days=28)
    p4_end = wtd_start - timedelta(days=1)
    data_line = (
        f"- **Latest call_date in raw df**: {data_max:%Y-%m-%d} ({data_max:%A})"
        if data_max is not None
        else "- **Latest call_date in raw df**: (missing column or no rows)"
    )
    md = "\n".join(
        [
            "═══════════════════════════════════════════════",
            "CURRENT ANALYSIS DATE (use for WTD, MTD, YTD, P4WA)",
            "═══════════════════════════════════════════════",
            "",
            "When the user says **WTD**, **week to date**, **MTD**, **month to date**, **YTD**, "
            "or similar **without explicit dates**, use the inclusive windows below. "
            "Treat **Analysis as-of** as \"today\" for this dataset — not calendar today if it differs.",
            "",
            f"- **Calendar today** (informational): {cal_today:%A, %B %d, %Y}",
            f"- **Report-through** (last reliable calendar day for dashboard metrics): {rtd:%Y-%m-%d} ({rtd:%A})",
            data_line,
            (
                f"- **Analysis as-of** (end inclusive for WTD / MTD / YTD): **{as_of:%Y-%m-%d}** "
                f"({as_of:%A, %B %d, %Y}) — min(report-through, latest call_date)"
            ),
            "",
            f"- **WTD** (Monday of the week containing analysis as-of → analysis as-of): **{wtd_start:%Y-%m-%d}** → **{as_of:%Y-%m-%d}**",
            f"- **MTD** (first day of that calendar month → analysis as-of): **{mtd_start:%Y-%m-%d}** → **{as_of:%Y-%m-%d}**",
            f"- **YTD** (Jan 1 of that calendar year → analysis as-of): **{ytd_start:%Y-%m-%d}** → **{as_of:%Y-%m-%d}**",
            f"- **P4WA pooled window** (four full Mon–Sun weeks before the week containing analysis as-of): **{p4_start:%Y-%m-%d}** → **{p4_end:%Y-%m-%d}**",
            "",
            "In execute_python these date objects are in scope: analysis_as_of, analysis_wtd_start, "
            "analysis_mtd_start, analysis_ytd_start, analysis_p4wa_start, analysis_p4wa_end, "
            "analysis_report_through, analysis_data_max (or None), analysis_calendar_today.",
        ]
    )
    ns = {
        "analysis_as_of": as_of,
        "analysis_calendar_today": cal_today,
        "analysis_report_through": rtd,
        "analysis_data_max": data_max,
        "analysis_wtd_start": wtd_start,
        "analysis_mtd_start": mtd_start,
        "analysis_ytd_start": ytd_start,
        "analysis_p4wa_start": p4_start,
        "analysis_p4wa_end": p4_end,
    }
    return {"markdown": md, "namespace": ns}


def build_schema_context(d: pd.DataFrame) -> str:
    _t = _ai_analyst_time_bundle(d)["namespace"]
    lines = [
        "⚠️  SCOPE: This dataset contains passed-credit calls only. "
        "Compass/IVR, queue, and failed-credit metrics are upstream and not present.",
        "",
        "═══ ANALYSIS DATE (WTD / MTD / YTD; matches AI Analyst) ═══",
        (
            f"Analysis as-of: {_t['analysis_as_of']}  |  WTD: {_t['analysis_wtd_start']} → {_t['analysis_as_of']}  "
            f"|  MTD: {_t['analysis_mtd_start']} → {_t['analysis_as_of']}"
        ),
        "",
        "═══ DATA SCOPE ═══",
        f"df (raw, default):     {d.shape[0]:,} rows × {d.shape[1]} columns",
        f"df_nodatefilter:       {df_nodatefilter.shape[0]:,} rows (sidebar filters, no date window)",
        f"df_filtered:           {df.shape[0]:,} rows (sidebar + date filters)",
    ]

    if "call_date" in d.columns and d["call_date"].notna().any():
        lines.append(f"Raw date range:        {d['call_date'].min().date()} – {d['call_date'].max().date()}")
    if "call_date" in df.columns and df["call_date"].notna().any():
        lines.append(f"Filtered date range:   {df['call_date'].min().date()} – {df['call_date'].max().date()}")

    lines.append("\n═══ KEY COLUMN VALUES (raw df) ═══")
    key_cats = [
        "center_location", "top_recommended_plan_type", "classification_bucket",
        "first_pitch_type", "sale_type", "mover_switcher", "marketing_bucket",
    ]
    for col in key_cats:
        if col in d.columns:
            vc = d[col].value_counts(dropna=False)
            vals = "  |  ".join(f"{k}: {v:,}" for k, v in vc.items())
            lines.append(f"  {col}: {vals}")

    lines.append("\n═══ ALL COLUMNS (name | dtype | sample) ═══")
    for col in d.columns:
        sample = d[col].dropna().iloc[0] if d[col].notna().any() else "null"
        lines.append(f"  {col:45s} | {str(d[col].dtype):10s} | e.g. {sample}")

    return "\n".join(lines)


# ── Sidebar: Settings (after filtered frames exist for schema text) ─────────────
with st.sidebar:
    _product_rec_theme_choice = theme.render_app_theme_toggle()
    with st.expander("AI Analyst Dataset Schema", expanded=False):
        st.code(build_schema_context(df_raw), language="text")

theme.inject_app_styles(light=_product_rec_theme_choice == "Light")
_chart_granularity = st.session_state.get("global_granularity", "Daily")


def wtd_vs_four_week_pooled(source: pd.DataFrame, metric_fn, date_col: str = "call_date"):
    """Partial Mon–Sun week (Mon through ``as_of``) vs pooled P4WA on four prior full Mon–Sun weeks.

    ``as_of`` is ``min(report_through_date(), max call date in ``source``)``. P4WA runs ``metric_fn``
    once on all calls from Mon ``week_start − 28`` through Sun ``week_start − 1`` (pooled, not an
    average of weekly KPIs).
    """
    if date_col not in source.columns:
        return None, None
    tmp = source.dropna(subset=[date_col]).copy()
    if tmp.empty:
        return None, None
    data_max = pd.to_datetime(tmp[date_col].max()).date()
    as_of = min(report_through_date(), data_max)
    week_start = monday_of_week_containing(as_of)

    def _slice(d0: date, d1: date):
        m = (tmp[date_col].dt.date >= d0) & (tmp[date_col].dt.date <= d1)
        return tmp.loc[m]

    cur = metric_fn(_slice(week_start, as_of))
    pool_start = week_start - timedelta(days=28)
    pool_end = week_start - timedelta(days=1)
    baseline = metric_fn(_slice(pool_start, pool_end))
    if baseline is not None and isinstance(baseline, float) and pd.isna(baseline):
        baseline = float("nan")
    return cur, baseline


def wk_pct_delta_vs_avg(cur, baseline):
    """Streamlit metric delta string: percent change of current vs pooled baseline (Arcadia-style)."""
    if cur is None or baseline is None:
        return None
    try:
        if pd.isna(cur) or pd.isna(baseline):
            return None
    except TypeError:
        return None
    if float(baseline) == 0:
        return None
    return f"{(float(cur) / float(baseline) - 1) * 100:+.1f}% vs P4WA"


def fmt_metric_val_pct(x):
    try:
        if x is None or pd.isna(x):
            return "—"
    except TypeError:
        return "—"
    return f"{float(x):.1f}%"


def fmt_metric_val_float(x, nd: int = 2):
    try:
        if x is None or pd.isna(x):
            return "—"
    except TypeError:
        return "—"
    return f"{float(x):.{nd}f}"


def fmt_metric_val_dollar(x):
    try:
        if x is None or pd.isna(x):
            return "—"
    except TypeError:
        return "—"
    return f"${float(x):,.0f}"


def dataframe_display_height(n_rows: int, min_rows: int = 4, row_px: int = 36, header_px: int = 52, cap: int = 2200) -> int:
    try:
        n = max(min_rows, int(n_rows))
    except (TypeError, ValueError):
        n = min_rows
    return int(min(cap, header_px + row_px * n))


def table_export_row(
    display_df: pd.DataFrame,
    download_filename: str,
    copy_label: str = "Copy",
    *,
    key_suffix: str = "",
) -> None:
    """Renders download + copy actions (place below ``st.dataframe``). Copy button sized to match Streamlit download."""
    tsv = display_df.to_csv(index=False, sep="\t")
    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    uid = hashlib.md5((download_filename + "\0" + key_suffix).encode(), usedforsecurity=False).hexdigest()[:12]
    b1, b2 = st.columns([1, 1])
    with b1:
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=download_filename,
            mime="text/csv",
            key=f"dl_{uid}",
        )
    with b2:
        tsv_literal = _json.dumps(tsv)
        lbl_literal = _json.dumps(copy_label)
        components.html(
            f"""<div style="font-family:DM Sans,sans-serif;padding:0;margin:0;">
<button type="button" id="cpbtn_{uid}"
  style="background:#3d8ef8;color:#fff;border:none;border-radius:0.5rem;box-sizing:border-box;
  width:100%;min-height:2.625rem;height:2.625rem;padding:0 1.25rem;font-size:0.875rem;font-weight:600;
  line-height:1.2;cursor:pointer;display:flex;align-items:center;justify-content:center;">{copy_label}</button>
</div>
<script>
(function() {{
  var text = {tsv_literal};
  var orig = {lbl_literal};
  var b = document.getElementById("cpbtn_{uid}");
  if (!b) return;
  b.addEventListener("click", function() {{
    function fallbackCopy() {{
      try {{
        var ta = document.createElement("textarea");
        ta.value = text;
        ta.setAttribute("readonly", "");
        ta.style.position = "fixed";
        ta.style.left = "-9999px";
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        ta.setSelectionRange(0, 999999);
        document.execCommand("copy");
        document.body.removeChild(ta);
      }} catch (e) {{}}
    }}
    if (navigator.clipboard && window.isSecureContext) {{
      navigator.clipboard.writeText(text).catch(fallbackCopy);
    }} else {{
      fallbackCopy();
    }}
    b.textContent = "Copied";
    setTimeout(function() {{ b.textContent = orig; }}, 1600);
  }});
}})();
</script>""",
            height=52,
        )


def mix_share_pct(slice_df: pd.DataFrame, plan_type: str) -> float:
    if slice_df.empty or "top_recommended_plan_type" not in slice_df.columns:
        return float("nan")
    return (slice_df["top_recommended_plan_type"] == plan_type).mean() * 100


def prepare_agent_behavior_dataframe(d: pd.DataFrame, adherence_mode: str):
    """Build agent-tab frame with ``agent_tier_display`` and column spec for adherence / bucket.

    Returns ``(frame, spec, effective_mode)``. If ``Sale`` is requested but required columns
    are missing, falls back to first-pitch columns and ``effective_mode == "First Pitch"``.
    """
    out = d.copy()
    base_spec = {
        "adh": "adhered_call",
        "slide": "slide_call",
        "ap": "all_plans_call",
        "cls": "classification_bucket",
    }
    sale_needed = {
        "sale_type", "order_count", "has_top_rec_pitch_view",
        "has_slide_recs_pitch_view", "has_all_plans_pitch_view",
    }
    if adherence_mode != "Sale" or not sale_needed.issubset(out.columns):
        if "first_pitch_type" in out.columns:
            out["agent_tier_display"] = out["first_pitch_type"]
        else:
            out["agent_tier_display"] = pd.NA
        eff = "First Pitch" if adherence_mode == "Sale" else adherence_mode
        return out, base_spec, eff

    ord_pos = out["order_count"].fillna(0) > 0
    stype = out["sale_type"]
    has_top = out["has_top_rec_pitch_view"].fillna(False).astype(bool)
    has_slide = out["has_slide_recs_pitch_view"].fillna(False).astype(bool)
    has_all = out["has_all_plans_pitch_view"].fillna(False).astype(bool)

    sale_adhered = (ord_pos & (stype == "Diamond") & has_top).astype("float64")
    sale_slide = (ord_pos & (stype == "Gold") & has_slide).astype("float64")
    sale_all = (has_all & (sale_adhered < 1) & (sale_slide < 1)).astype("float64")

    out["sale_adhered_call"] = sale_adhered
    out["sale_slide_call"] = sale_slide
    out["sale_all_plans_call"] = sale_all

    out["sale_classification_bucket"] = "Unclassified"
    out.loc[sale_adhered >= 1, "sale_classification_bucket"] = "Adherence"
    out.loc[(sale_slide >= 1) & (sale_adhered < 1), "sale_classification_bucket"] = "Slide"
    out.loc[(sale_all >= 1) & (sale_adhered < 1) & (sale_slide < 1), "sale_classification_bucket"] = "All Plans"

    # Tier mix / tier filters: only defined on converting calls with a canonical tier
    # (non-sales, missing sale_type, or non-D/G/S/B values stay NA — excluded from mix charts).
    out["agent_tier_display"] = pd.NA
    tier_known = ord_pos & stype.notna() & stype.isin(SALE_TIER_ORDER)
    out.loc[tier_known, "agent_tier_display"] = stype.loc[tier_known].astype(str)

    sale_spec = {
        "adh": "sale_adhered_call",
        "slide": "sale_slide_call",
        "ap": "sale_all_plans_call",
        "cls": "sale_classification_bucket",
    }
    return out, sale_spec, "Sale"


# Plotly: side-by-side charts share height + margins so x-axes line up at the same baseline.
PAIR_CHART_HEIGHT = 400
PAIR_CHART_MARGIN = dict(l=52, r=24, t=56, b=104)
PAIR_CHART_LAYOUT = dict(height=PAIR_CHART_HEIGHT, margin=PAIR_CHART_MARGIN)
PAIR_LEGEND_BELOW = dict(orientation="h", yanchor="top", y=-0.28, x=0.5, xanchor="center")
date_str = ""
if "call_date" in df.columns and df["call_date"].notna().any():
    mn = df["call_date"].min().strftime("%b %d")
    mx = df["call_date"].max().strftime("%b %d, %Y")
    date_str = f"{mn} – {mx}"

st.title("📊 Product Rank Dash")
st.caption(f"{date_str}  ·  {len(df):,} calls in view")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_model, tab_agent, tab_sale_mix, tab_agent_level, tab_chat = st.tabs(["Model Outputs", "Agent Behavior & Performance", "Sale Mixes", "Agent Level", "AI Analyst"])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — MODEL OUTPUTS
# ════════════════════════════════════════════════════════════════════════════════
with tab_model:

    # ── Section 1: Recommendation Mix ────────────────────────────────────────
    st.subheader("Recommendation Mix Over Time")

    if "top_recommended_plan_type" in df_nodatefilter.columns and "call_date" in df_nodatefilter.columns:
        plan_types_all = sorted(df_nodatefilter["top_recommended_plan_type"].dropna().unique().tolist())
        if plan_types_all:
            _wdf_mix = df_nodatefilter.dropna(subset=["call_date", "top_recommended_plan_type"])
            if not _wdf_mix.empty:
                _asof_mix = min(report_through_date(), pd.to_datetime(_wdf_mix["call_date"].max()).date())
                _ws_mix = monday_of_week_containing(_asof_mix)
                st.caption(
                    "Mon–Sun weeks · **WTD** = share of calls from **Monday of this week through yesterday** "
                    "(same logic as Arcadia Overview: on Mondays, yesterday is Sunday, so WTD is the full Mon–Sun week just ended) "
                    "vs **P4WA**: the same mix on **all calls in the four prior full Mon–Sun weeks** (pooled). "
                    "Ignores date filter · sidebar filters apply. "
                    f"WTD: {_ws_mix:%b %d}–{_asof_mix:%b %d}."
                )

            def _wk_mix(fn):
                return wtd_vs_four_week_pooled(df_nodatefilter, fn)

            trend_cols = st.columns(len(plan_types_all))
            for i, pt in enumerate(plan_types_all):
                cur, pool = _wk_mix(lambda d, p=pt: mix_share_pct(d, p))
                trend_cols[i].metric(
                    label=f"{pt} — Mix (WTD)",
                    value=fmt_metric_val_pct(cur),
                    delta=wk_pct_delta_vs_avg(cur, pool),
                    help=(
                        "Share of calls with this plan as top recommendation — week-to-date vs pooled four prior Mon–Sun weeks (P4WA). "
                        "Ignores date filter."
                    ),
                )


        period_col = period_labels(df["call_date"], _chart_granularity)
        rec_ts = (
            df.dropna(subset=["call_date", "top_recommended_plan_type"])
            .assign(period=period_col)
            .groupby(["period", "top_recommended_plan_type"])
            .size()
            .reset_index(name="n")
            .sort_values("period")
        )
        rec_ts["period_display"] = period_display(rec_ts["period"], _chart_granularity)
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
        apply_chart_theme(fig_mix,
            **PAIR_CHART_LAYOUT,
            yaxis_ticksuffix="%",
            legend=dict(**PAIR_LEGEND_BELOW),
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
        st.dataframe(
            rec_pivot,
            use_container_width=True,
            hide_index=True,
            height=dataframe_display_height(len(rec_pivot)),
        )
        table_export_row(rec_pivot, "recommendation_mix_pivot.csv", key_suffix="model_rec_mix")
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
        # Build a dataframe with diamond product and gold product per call
        prod_df = df.dropna(subset=["call_date"]).copy()
        prod_df["diamond_product"] = prod_df[prod_col].apply(lambda x: _extract_ranked_slot_product(x, 0))
        prod_df["gold_product"] = prod_df[prod_col].apply(lambda x: _extract_ranked_slot_product(x, 1))

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
            pm_df["period"] = period_labels(pm_df["call_date"], _chart_granularity)
            # Total calls per period (from full df, not pm_df, for proper denominator)
            period_totals = (
                prod_df.assign(period=period_labels(prod_df["call_date"], _chart_granularity))
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
            pm_ts["period_display"] = period_display(pm_ts["period"], _chart_granularity)

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
            apply_chart_theme(fig_pm,
                **PAIR_CHART_LAYOUT,
                yaxis_ticksuffix="%",
                legend=dict(**PAIR_LEGEND_BELOW),
                yaxis_title=f"% of calls with product in {pm_slot} slot",
            )
            st.plotly_chart(fig_pm, use_container_width=True)
            pm_pivot = (
                pm_ts.pivot_table(index=slot_product_col, columns="period_display", values="pct", aggfunc="mean")
                .fillna(0)
                .round(1)
                .reset_index()
                .rename(columns={slot_product_col: "Product"})
            )
            pm_disp = pm_pivot.copy()
            for _col in pm_disp.columns[1:]:
                pm_disp[_col] = pm_disp[_col].astype(str) + "%"
            st.caption(f"Product mix ({pm_slot} slot) — share of calls (%) per period (same scope as the chart above)")
            st.dataframe(
                pm_disp,
                use_container_width=True,
                hide_index=True,
                height=dataframe_display_height(len(pm_disp)),
            )
            table_export_row(pm_disp, "product_level_rec_mix_pivot.csv", key_suffix="model_pm_mix")
            if not pm_products:
                st.caption(f"Showing top 10 products by volume in the {pm_slot} slot. Use the filter above to select specific products.")
        else:
            st.info("No data available for the selected slot / product combination.")
    else:
        st.info("Product recommendation column not found. Expected one of: recommended_in_order, pitches_canonical_in_order, pitches_in_order.")

    st.divider()
    st.subheader("Custom period comparison — recommendation mix")
    st.caption(
        "Compare **share of calls (%)** for each category between a **Pre** and **Post** window. "
        "Pickers span the **full** `call_date` range in the extract (sidebar **date** filter does not apply). "
        "Counts still use other sidebar filters only. Defaults: **Post** = latest full Mon–Sun week vs raw max; "
        "**Pre** = four Mon–Sun weeks before that. **% change vs pre** is relative to the pre-period share."
    )
    df_mcmp = df_nodatefilter.dropna(subset=["call_date"]).copy()
    if "call_date" not in df_raw.columns or df_raw["call_date"].isna().all():
        st.caption("Raw `call_date` is missing for period bounds.")
    elif df_mcmp.empty:
        st.caption("No calls match the current sidebar filters (excluding date) for this comparison.")
    else:
        mod_min = pd.to_datetime(df_raw["call_date"].min()).date()
        mod_max = pd.to_datetime(df_raw["call_date"].max()).date()
        _mm_sig = (mod_min, mod_max, len(df_mcmp))
        if st.session_state.get("model_mix_cmp_sig") != _mm_sig:
            st.session_state.pop("model_mix_cmp_pre_range", None)
            st.session_state.pop("model_mix_cmp_post_range", None)
            st.session_state["model_mix_cmp_sig"] = _mm_sig

        _mo_pre_default, _mo_post_default = streamlit_safe_period_defaults(mod_max, mod_min)

        prod_col_m = next(
            (c for c in ("recommended_in_order", "pitches_canonical_in_order", "pitches_in_order") if c in df_mcmp.columns),
            None,
        )
        mix_mode_opts = ["Plan type (#1 recommendation)"]
        if prod_col_m is not None:
            mix_mode_opts.append("Product (ranked pitch slot)")
        mix_cmp_mode = st.radio(
            "Mix to compare",
            mix_mode_opts,
            horizontal=True,
            key="model_mix_cmp_mode",
        )
        slot_idx_m = 0
        if mix_cmp_mode.startswith("Product"):
            slot_cmp = st.selectbox(
                "Pitch slot",
                ["Diamond", "Gold"],
                index=0,
                key="model_mix_cmp_slot",
            )
            slot_idx_m = 0 if slot_cmp == "Diamond" else 1

        mo_c1, mo_c2 = st.columns(2)
        with mo_c1:
            mo_pre = st.date_input(
                "Pre period",
                value=_mo_pre_default,
                min_value=mod_min,
                max_value=mod_max,
                key="model_mix_cmp_pre_range",
            )
        with mo_c2:
            mo_post = st.date_input(
                "Post period",
                value=_mo_post_default,
                min_value=mod_min,
                max_value=mod_max,
                key="model_mix_cmp_post_range",
            )

        def _mo_slice(d0: date, d1: date):
            lo, hi = sorted((d0, d1))
            m = (df_mcmp["call_date"].dt.date >= lo) & (df_mcmp["call_date"].dt.date <= hi)
            return df_mcmp.loc[m]

        def _mo_share_plan(sub: pd.DataFrame) -> pd.Series:
            if len(sub) == 0 or "top_recommended_plan_type" not in sub.columns:
                return pd.Series(dtype=float)
            tot = len(sub)
            vc = sub["top_recommended_plan_type"].value_counts(dropna=True)
            return (vc / tot * 100).sort_values(ascending=False)

        def _mo_share_product(sub: pd.DataFrame, col: str, idx: int) -> pd.Series:
            if len(sub) == 0:
                return pd.Series(dtype=float)
            tot = len(sub)
            sprod = sub[col].apply(lambda x, i=idx: _extract_ranked_slot_product(x, i))
            vc = sprod.value_counts(dropna=True)
            return (vc / tot * 100).sort_values(ascending=False)

        def _mo_color_pct_chg(val):
            try:
                x = float(val)
            except (TypeError, ValueError):
                return ""
            if pd.isna(x):
                return ""
            return theme.period_comparison_delta_style(x, neutral_abs=10.0)

        if len(mo_pre) == 2 and len(mo_post) == 2:
            pre_m = _mo_slice(mo_pre[0], mo_pre[1])
            post_m = _mo_slice(mo_post[0], mo_post[1])
            pre_lab = f"{sorted(mo_pre)[0].strftime('%-m/%-d')}-{sorted(mo_pre)[1].strftime('%-m/%-d')}"
            post_lab = f"{sorted(mo_post)[0].strftime('%-m/%-d')}-{sorted(mo_post)[1].strftime('%-m/%-d')}"

            s_pre = None
            s_post = None
            idx_m = None
            axis_lbl = ""
            export_fn = ""
            export_key = ""

            if mix_cmp_mode.startswith("Plan"):
                if "top_recommended_plan_type" not in df_mcmp.columns:
                    st.info("Column top_recommended_plan_type is missing for plan-type comparison.")
                else:
                    s_pre = _mo_share_plan(pre_m)
                    s_post = _mo_share_plan(post_m)
                    idx_m = s_pre.index.union(s_post.index)
                    axis_lbl = "Plan type"
                    export_fn = "model_outputs_plan_mix_period_compare.csv"
                    export_key = "model_mix_cmp_plan"
            else:
                if prod_col_m is None:
                    st.info("Product list column not found for product mix comparison.")
                else:
                    s_pre = _mo_share_product(pre_m, prod_col_m, slot_idx_m)
                    s_post = _mo_share_product(post_m, prod_col_m, slot_idx_m)
                    idx_m = s_pre.index.union(s_post.index)
                    axis_lbl = "Product"
                    export_fn = "model_outputs_product_mix_period_compare.csv"
                    export_key = "model_mix_cmp_prod"

            if s_pre is not None and s_post is not None and idx_m is not None:
                t_m = pd.DataFrame(
                    {
                        f"Share % ({pre_lab})": s_pre.reindex(idx_m).fillna(0).round(1),
                        f"Share % ({post_lab})": s_post.reindex(idx_m).fillna(0).round(1),
                    }
                )
                _m_c0, _m_c1 = t_m.columns[0], t_m.columns[1]
                _pre_v = t_m[_m_c0].astype(float)
                _post_v = t_m[_m_c1].astype(float)
                t_m["% change vs pre"] = (
                    (_post_v / _pre_v.replace(0, float("nan")) - 1.0).mul(100).round(1)
                )
                t_m = t_m.sort_values(_m_c1, ascending=False).rename_axis(axis_lbl).reset_index()

                sty_m = t_m.style.map(_mo_color_pct_chg, subset=["% change vs pre"])
                sty_m = sty_m.set_properties(**{"text-align": "right"}, subset=[_m_c0, _m_c1, "% change vs pre"])
                sty_m = sty_m.set_properties(**{"text-align": "left"}, subset=[axis_lbl])
                st.dataframe(
                    sty_m,
                    use_container_width=True,
                    hide_index=True,
                    height=dataframe_display_height(min(len(t_m), 40)),
                )
                table_export_row(t_m, export_fn, key_suffix=export_key)
        else:
            st.caption("Select full pre and post date ranges to populate the comparison table.")

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
        "call_date",
        "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled",
        "expected_points_gap_1_2", "top_recommended_plan_type",
        "classification_bucket", "adhered_call", "order_count",
        "gcv_on_first_pitch", "gcv",
    }

    if prob_cols_needed.issubset(df.columns):

        GAP_LABELS = ["Q1\nLowest", "Q2", "Q3", "Q4", "Q5\nHighest"]

        _wdf_prob = df_nodatefilter
        _wk_prob = lambda fn: wtd_vs_four_week_pooled(_wdf_prob, fn)

        if "call_date" in _wdf_prob.columns and not _wdf_prob.dropna(subset=["call_date"]).empty:
            _asof_p = min(report_through_date(), pd.to_datetime(_wdf_prob["call_date"].max()).date())
            _ws_p = monday_of_week_containing(_asof_p)
            st.caption(
                f"WTD ({_ws_p:%b %d}–{_asof_p:%b %d}) vs **P4WA** (four prior full Mon–Sun weeks, pooled) · "
                "ignores date filter · sidebar filters apply."
            )

        ra_cols = st.columns(4)
        for i, (pt, col) in enumerate([("Fixed", "raw_prob_fixed"), ("Tiered", "raw_prob_tiered"),
                                        ("Bundled", "raw_prob_bundled")]):
            if col in _wdf_prob.columns:
                cur_p, pool_p = _wk_prob(lambda d, c=col: d[c].mean() * 100 if c in d.columns else float("nan"))
                ra_cols[i].metric(
                    f"Avg P(convert) — {pt}",
                    fmt_metric_val_pct(cur_p),
                    delta=wk_pct_delta_vs_avg(cur_p, pool_p),
                    help="Mean raw conversion probability — WTD vs P4WA (pooled four prior weeks).",
                )
        cur_gap, pool_gap = _wk_prob(
            lambda d: d["expected_points_gap_1_2"].mean() if "expected_points_gap_1_2" in d.columns else float("nan")
        )
        _gap_val = fmt_metric_val_float(cur_gap, 2)
        ra_cols[3].metric(
            "Avg Confidence Gap",
            f"{_gap_val} pts" if _gap_val != "—" else "—",
            delta=wk_pct_delta_vs_avg(cur_gap, pool_gap),
            help="Mean expected-points gap #1 vs #2 — WTD vs P4WA.",
        )

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
            apply_chart_theme(fig_violin,
                **PAIR_CHART_LAYOUT,
                yaxis_title="Raw Conversion Probability",
                yaxis_tickformat=".0%",
                showlegend=False,
            )
            st.plotly_chart(fig_violin, use_container_width=True)

        with rb2:
            gap_vals = df["expected_points_gap_1_2"].dropna()
            p25, p75 = gap_vals.quantile(0.25), gap_vals.quantile(0.75)
            pct_low  = (gap_vals < p25).mean() * 100
            pct_high = (gap_vals > p75).mean() * 100
            fig_hist = go.Figure()
            _hist_stroke, _ = chart_hist_stroke_and_title()
            fig_hist.add_trace(
                go.Histogram(
                    x=gap_vals,
                    nbinsx=40,
                    marker_color=PLOT_COLORWAY[0],
                    marker_line_width=1,
                    marker_line_color=_hist_stroke,
                    opacity=0.8,
                )
            )
            fig_hist.add_vline(x=float(p25), line_dash="dash", line_color=chart_hline_reference(),
                               annotation_text=f"25th ({p25:.2f})", annotation_position="top right",
                               annotation_font_color=chart_muted())
            fig_hist.add_vline(x=float(p75), line_dash="dash", line_color=chart_hline_reference(),
                               annotation_text=f"75th ({p75:.2f})", annotation_position="top left",
                               annotation_font_color=chart_muted())
            apply_chart_theme(fig_hist,
                **PAIR_CHART_LAYOUT,
                xaxis_title="Expected Points Gap (#1 vs #2)",
                yaxis_title="Calls",
                showlegend=False,
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        st.caption(
            f"**{pct_low:.0f}%** of calls are low-confidence (gap < {p25:.2f} pts) · "
            f"**{pct_high:.0f}%** are high-confidence (gap > {p75:.2f} pts)"
        )

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

        metric_choice = st.radio(
            "Outcome metric",
            ["1st Pitch CR", "Overall CR", "GCV / Call"],
            horizontal=True,
            key="conf_gap_metric",
        )
        col_map2 = {"1st Pitch CR": "gcv_on_first_pitch", "Overall CR": "order_count", "GCV / Call": "gcv"}
        df_gap2 = df_gap[df_gap["classification_bucket"].isin(["Adherence", "Slide"])].copy()
        gap_out = None
        is_dollar = metric_choice == "GCV / Call"
        if len(df_gap2) > 0:
            gap_out = (
                df_gap2.groupby(["gap_bucket", "classification_bucket"], observed=True)
                .agg(val=(col_map2[metric_choice],
                      lambda x: x.mean() if metric_choice == "GCV / Call" else (x > 0).mean() * 100),
                     calls=("gcv", "count"))
                .reset_index()
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
                textfont=bar_outside_textfont(),
                marker_color=PLOT_COLORWAY[0],
                marker_line_width=0,
                customdata=adh_by_gap[["calls", "gap_med"]],
                hovertemplate="Calls: %{customdata[0]:,}<br>Median gap: %{customdata[1]:.2f}<extra></extra>",
            ))
            apply_chart_theme(fig_adh_gap,
                **PAIR_CHART_LAYOUT,
                xaxis_title="Confidence Gap Quintile",
                yaxis_title="Adherence Rate",
                yaxis_ticksuffix="%",
                showlegend=False,
            )
            st.plotly_chart(fig_adh_gap, use_container_width=True)

        with rd2:
            if gap_out is not None and len(gap_out) > 0:
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
                apply_chart_theme(fig_out,
                    **PAIR_CHART_LAYOUT,
                    xaxis_title="Confidence Gap Quintile",
                    yaxis_tickprefix="$" if is_dollar else "",
                    yaxis_ticksuffix="" if is_dollar else "%",
                    legend=dict(**PAIR_LEGEND_BELOW),
                )
                st.plotly_chart(fig_out, use_container_width=True)
            else:
                st.info("No Adherence / Slide calls in view for this outcome chart.")

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

    needed = {"call_date", "classification_bucket", "gcv_on_first_pitch", "order_count", "gcv"}

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

        _wdf_ts = df_nodatefilter
        _wk_ts = lambda fn: wtd_vs_four_week_pooled(_wdf_ts, fn)

        def _fp_cr_bucket(d, bucket):
            sub = d[d["classification_bucket"] == bucket]
            if sub.empty:
                return float("nan")
            return (sub["gcv_on_first_pitch"] > 0).mean() * 100

        def _gcv_call_bucket(d, bucket):
            sub = d[d["classification_bucket"] == bucket]
            if sub.empty or "gcv" not in sub.columns:
                return float("nan")
            return sub["gcv"].mean()

        cur_t_f, p_t_f = _wk_ts(lambda d: _fp_cr_bucket(d, "Adherence"))
        cur_s_f, p_s_f = _wk_ts(lambda d: _fp_cr_bucket(d, "Slide"))
        cur_t_g, p_t_g = _wk_ts(lambda d: _gcv_call_bucket(d, "Adherence"))
        cur_s_g, p_s_g = _wk_ts(lambda d: _gcv_call_bucket(d, "Slide"))

        if "call_date" in _wdf_ts.columns and not _wdf_ts.dropna(subset=["call_date"]).empty:
            _asof_ts = min(report_through_date(), pd.to_datetime(_wdf_ts["call_date"].max()).date())
            _ws_ts = monday_of_week_containing(_asof_ts)
            st.caption(
                f"**WTD** ({_ws_ts:%b %d}–{_asof_ts:%b %d}) vs **P4WA** on KPI row (pooled four prior Mon–Sun weeks) · "
                "ignores date filter. Bar charts use the sidebar date range."
            )

        ca1, ca2, ca3, ca4 = st.columns(4)
        ca1.metric(
            "Top Rec — 1st Pitch CR",
            fmt_metric_val_pct(cur_t_f),
            delta=wk_pct_delta_vs_avg(cur_t_f, p_t_f),
            help=(
                f"WTD vs P4WA. In current date range: {top_fp_cr:.1f}% vs Slide {slide_fp_cr:.1f}% "
                f"({top_fp_cr - slide_fp_cr:+.1f}pp)."
            ),
        )
        ca2.metric(
            "Slide — 1st Pitch CR",
            fmt_metric_val_pct(cur_s_f),
            delta=wk_pct_delta_vs_avg(cur_s_f, p_s_f),
            help=f"In current date range: {slide_fp_cr:.1f}%.",
        )
        ca3.metric(
            "Top Rec — GCV / Call",
            fmt_metric_val_dollar(cur_t_g),
            delta=wk_pct_delta_vs_avg(cur_t_g, p_t_g),
            help=(
                f"WTD vs P4WA. In current date range: ${top_gcv:,.0f} vs Slide ${slide_gcv:,.0f} "
                f"(${top_gcv - slide_gcv:+,.0f})."
            ),
        )
        ca4.metric(
            "Slide — GCV / Call",
            fmt_metric_val_dollar(cur_s_g),
            delta=wk_pct_delta_vs_avg(cur_s_g, p_s_g),
            help=f"In current date range: ${slide_gcv:,.0f}.",
        )

        cb1, cb2 = st.columns(2)

        with cb1:
            st.markdown("**Conversion Rate**")
            fig_cr = go.Figure()
            fig_cr.add_trace(go.Bar(
                name="Top Rec", x=["1st Pitch CR", "Overall CR"],
                y=[top_fp_cr, top_cr],
                text=[f"{top_fp_cr:.1f}%", f"{top_cr:.1f}%"], textposition="outside",
                textfont=bar_outside_textfont(),
                marker_color=PLOT_COLORWAY[0], marker_line_width=0,
            ))
            fig_cr.add_trace(go.Bar(
                name="Slide", x=["1st Pitch CR", "Overall CR"],
                y=[slide_fp_cr, slide_cr],
                text=[f"{slide_fp_cr:.1f}%", f"{slide_cr:.1f}%"], textposition="outside",
                textfont=bar_outside_textfont(),
                marker_color=PLOT_COLORWAY[1], marker_line_width=0,
            ))
            apply_chart_theme(fig_cr,
                **PAIR_CHART_LAYOUT,
                barmode="group", yaxis_ticksuffix="%",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig_cr, use_container_width=True)

        with cb2:
            st.markdown("**GCV**")
            fig_gcv = go.Figure()
            fig_gcv.add_trace(go.Bar(
                name="Top Rec", x=["GCV / Call", "GCV / 1st Pitch"],
                y=[top_gcv, top_gcv_fp],
                text=[f"${top_gcv:,.0f}", f"${top_gcv_fp:,.0f}"], textposition="outside",
                textfont=bar_outside_textfont(),
                marker_color=PLOT_COLORWAY[0], marker_line_width=0,
            ))
            fig_gcv.add_trace(go.Bar(
                name="Slide", x=["GCV / Call", "GCV / 1st Pitch"],
                y=[slide_gcv, slide_gcv_fp],
                text=[f"${slide_gcv:,.0f}", f"${slide_gcv_fp:,.0f}"], textposition="outside",
                textfont=bar_outside_textfont(),
                marker_color=PLOT_COLORWAY[1], marker_line_width=0,
            ))
            apply_chart_theme(fig_gcv,
                **PAIR_CHART_LAYOUT,
                barmode="group", yaxis_tickprefix="$",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig_gcv, use_container_width=True)

        st.caption(
            "GCV / 1st Pitch = total first-pitch GCV ÷ all calls in group (expected value per call)."
        )

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
            st.dataframe(
                plan_cmp,
                use_container_width=True,
                hide_index=True,
                height=dataframe_display_height(len(plan_cmp)),
            )
            table_export_row(plan_cmp, "conversion_by_plan_type_top_rec_vs_slide.csv", key_suffix="model_plan_cmp")
    else:
        st.info("One or more required columns are missing for this section.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — AGENT BEHAVIOR & PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
with tab_agent:

    agent_adherence_type = st.radio(
        "Adherence type",
        ["First Pitch", "Sale"],
        index=0,
        horizontal=True,
        key="agent_adherence_type",
    )

    df_agent, colspec, agent_eff_mode = prepare_agent_behavior_dataframe(df, agent_adherence_type)
    df_nodate_agent, _, _ = prepare_agent_behavior_dataframe(df_nodatefilter, agent_adherence_type)

    if agent_adherence_type == "Sale" and agent_eff_mode != "Sale":
        st.warning(
            "Sale-based adherence needs `sale_type`, `order_count`, and top/slide/all-plans pitch-view flags "
            "on the call-level file. Showing first-pitch adherence instead."
        )

    adh_c, sl_c, ap_c, cls_c = colspec["adh"], colspec["slide"], colspec["ap"], colspec["cls"]

    if "call_date" in df_nodate_agent.columns and "agent_tier_display" in df_nodate_agent.columns:
        tmp_fp = df_nodate_agent.dropna(subset=["call_date"]).copy()
        tmp_fp["week"] = tmp_fp["call_date"].dt.to_period("W")
        fp_weeks = sorted(tmp_fp["week"].unique())
        fp_m1, fp_m2 = (fp_weeks[-1] if len(fp_weeks) >= 1 else None,
                        fp_weeks[-2] if len(fp_weeks) >= 2 else None)

        def fp_rate(week_period, tier):
            if week_period is None:
                return None
            sub = tmp_fp[tmp_fp["week"] == week_period]
            if agent_eff_mode == "Sale" and "order_count" in sub.columns:
                sub = sub[sub["order_count"].fillna(0) > 0]
            if len(sub) == 0:
                return None
            return (sub["agent_tier_display"] == tier).mean() * 100

        tier_lbl = "Sale tier" if agent_eff_mode == "Sale" else "FP"
        fp_cols = st.columns(4)
        for i, tier in enumerate(["Diamond", "Gold", "Silver", "Bronze"]):
            this_v = fp_rate(fp_m1, tier)
            prev_v = fp_rate(fp_m2, tier)
            if this_v is not None:
                delta_str = (f"{this_v - prev_v:+.1f}pp vs prior week"
                             if prev_v is not None else None)
                _help = (
                    "Among **converting calls** in the week, share whose sold-pitch tier was this slot "
                    "(`sale_type` in the pipeline) · last ISO week · ignores date filter"
                    if agent_eff_mode == "Sale"
                    else "Share of calls where this tier was pitched first · last full ISO week · ignores date filter"
                )
                fp_cols[i].metric(
                    label=f"{tier} {tier_lbl} rate",
                    value=f"{this_v:.1f}%",
                    delta=delta_str,
                    help=_help,
                )

    ah1, ah2 = st.columns(2)
    with ah1:
        st.subheader("Adherence Over Time")
    with ah2:
        st.subheader(
            "Sale tier mix over time (among sales)"
            if agent_eff_mode == "Sale"
            else "First pitch mix over time"
        )

    col1, col2 = st.columns(2)

    with col1:
        if "call_date" in df_agent.columns and adh_c in df_agent.columns:
            _adh_ts_base = df_agent.dropna(subset=["call_date"])
            if agent_eff_mode == "Sale" and "order_count" in _adh_ts_base.columns:
                _adh_ts_base = _adh_ts_base[_adh_ts_base["order_count"].fillna(0) > 0]
            ts = (
                _adh_ts_base
                .assign(period=period_labels(_adh_ts_base["call_date"], _chart_granularity))
                .groupby("period")
                .agg(
                    adherence=(adh_c, "mean"),
                    slide=(sl_c, "mean"),
                    all_plans=(ap_c, "mean"),
                )
                .reset_index()
                .sort_values("period")
            )
            ts["period_display"] = period_display(ts["period"], _chart_granularity)
            ts[["adherence", "slide", "all_plans"]] *= 100

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["adherence"], name="Adherence",
                                     mode="lines+markers", line=dict(width=2), marker=dict(size=5)))
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["slide"], name="Slide",
                                     mode="lines+markers", line=dict(dash="dot", width=2), marker=dict(size=5)))
            fig.add_trace(go.Scatter(x=ts["period_display"], y=ts["all_plans"], name="All Plans",
                                     mode="lines+markers", line=dict(dash="dash", width=2), marker=dict(size=5)))
            apply_chart_theme(fig,
                **PAIR_CHART_LAYOUT,
                yaxis_ticksuffix="%",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig, use_container_width=True)
            if agent_eff_mode == "Sale":
                st.caption("Adherence, slide, and all-plans rates are **among converting calls only** (each line is % of sales).")
        else:
            st.info("call_date or adherence columns missing.")

    with col2:
        if "call_date" in df_agent.columns and "agent_tier_display" in df_agent.columns:
            tier_order = SALE_TIER_ORDER
            _mix_base = df_agent.dropna(subset=["call_date"])
            if agent_eff_mode == "Sale" and "order_count" in _mix_base.columns:
                _mix_base = _mix_base[_mix_base["order_count"].fillna(0) > 0]
            _mix_base = _mix_base.dropna(subset=["agent_tier_display"])
            pitch_ts = (
                _mix_base
                .assign(period=period_labels(_mix_base["call_date"], _chart_granularity))
                .groupby(["period", "agent_tier_display"])
                .size()
                .reset_index(name="n")
                .sort_values("period")
            )
            pitch_ts["period_display"] = period_display(pitch_ts["period"], _chart_granularity)
            totals = pitch_ts.groupby("period")["n"].transform("sum")
            pitch_ts["pct"] = pitch_ts["n"] / totals * 100

            fig2 = go.Figure()
            for pt in tier_order:
                sub = pitch_ts[pitch_ts["agent_tier_display"] == pt]
                if sub.empty:
                    continue
                fig2.add_trace(go.Scatter(x=sub["period_display"], y=sub["pct"], name=pt,
                                          mode="lines+markers", line=dict(width=2), marker=dict(size=5)))
            apply_chart_theme(fig2,
                **PAIR_CHART_LAYOUT,
                yaxis_ticksuffix="%",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig2, use_container_width=True)
            if agent_eff_mode == "Sale":
                st.caption(
                    "Each period sums to **100%** over converting calls with a classified **Diamond / Gold / "
                    "Silver / Bronze** `sale_type` (orders without that classification are excluded)."
                )
        else:
            st.info("call_date or tier display column missing.")

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
            "Sale tier" if agent_eff_mode == "Sale" else "First pitch type",
            fp_type_opts,
            key="pot_fp_filter",
        )

    if "call_date" in df_agent.columns:
        pot_df = df_agent.dropna(subset=["call_date"]).copy()

        if pot_fp_filter != "All" and "agent_tier_display" in pot_df.columns:
            pot_df = pot_df[pot_df["agent_tier_display"] == pot_fp_filter]

        pot_df["period"] = period_labels(pot_df["call_date"], _chart_granularity)

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
            pot_ts["period_display"] = period_display(pot_ts["period"], _chart_granularity)

            is_dollar = pot_metric in ("GCV / 1st Pitch", "GCV / Call", "RPO")
            fig_pot = go.Figure()
            fig_pot.add_trace(go.Scatter(
                x=pot_ts["period_display"],
                y=pot_ts["value"],
                mode="lines+markers",
                name=pot_metric,
                line=dict(color=PLOT_COLORWAY[0], width=2),
                marker=dict(size=5, color=PLOT_COLORWAY[0]),
                fill="tozeroy",
                fillcolor=area_fill_primary(),
            ))
            apply_chart_theme(fig_pot,
                **PAIR_CHART_LAYOUT,
                yaxis_tickprefix="$" if is_dollar else "",
                yaxis_ticksuffix="" if is_dollar else "%",
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
        "Select two date ranges to compare. Defaults: **Post** = latest full Mon–Sun week in the data; "
        "**Pre** = the four Mon–Sun weeks before that. Delta cells are green (improvement) or red (decline)."
    )

    _pop_core = {
        "call_date", "top_recommended_plan_type",
        "gcv_on_first_pitch", "order_count", "gcv",
    }
    _pop_fp = {"classification_bucket", "adhered_call", "slide_call", "all_plans_call"}
    _pop_sale = {
        "sale_type", "order_count", "has_top_rec_pitch_view",
        "has_slide_recs_pitch_view", "has_all_plans_pitch_view",
    }
    _pop_needed = _pop_core | (_pop_sale if agent_eff_mode == "Sale" else _pop_fp)

    if _pop_needed.issubset(df_raw.columns):

        raw_min = pd.to_datetime(df_raw["call_date"].min()).date()
        raw_max = pd.to_datetime(df_raw["call_date"].max()).date()

        (_pre_def_start, _pre_def_end), (_post_def_start, _post_def_end) = streamlit_safe_period_defaults(
            raw_max, raw_min
        )

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

            pre_df  = slice_period(df_nodate_agent, pre_range[0],  pre_range[1])
            post_df = slice_period(df_nodate_agent, post_range[0], post_range[1])

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
                if metric == "rpo":
                    oc = source["order_count"].fillna(0) > 0
                    sub = source[oc]
                    return sub["gcv"].mean() if len(sub) else float("nan")
                return float("nan")

            pre_label = f"{pre_range[0].strftime('%-m/%-d')}-{pre_range[1].strftime('%-m/%-d')}"
            post_label = f"{post_range[0].strftime('%-m/%-d')}-{post_range[1].strftime('%-m/%-d')}"

            def fmt_val(v, fmt):
                if pd.isna(v):
                    return "—"
                if fmt == "pct":
                    return f"{v:.1f}%"
                if fmt == "dollar":
                    return f"${v:,.1f}"
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
                return theme.period_comparison_delta_style(num, neutral_abs=3.0)

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
                    .size()
                    .rename("rec_total")
                )

                rows = []
                for rec_type, behavior, mask_col in [
                    ("top_recommended_plan_type", "Adhered",   adh_c),
                    ("top_recommended_plan_type", "Slide",     sl_c),
                    ("top_recommended_plan_type", "All Plans", ap_c),
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

                METRICS = [
                    ("mix",      "Mix",                    "pct",    False),
                    ("fp_cr",    "First Pitch CR",          "pct",    False),
                    ("ov_cr",    "Overall CR",              "pct",    False),
                    ("gcv_fp",   "GCV / First Pitch",  "dollar", True),
                    ("gcv_call", "GCV / Call",              "dollar", True),
                ]

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

                st.dataframe(
                    styler,
                    use_container_width=True,
                    hide_index=True,
                    column_order=col_order,
                    height=dataframe_display_height(len(display_df)),
                )
                table_export_row(display_df, "agent_period_rec_type_behavior.csv", key_suffix="agent_pop_rec")

                st.subheader("Overall Comparison")

                def compute_overall_metrics(source):
                    if len(source) == 0:
                        return pd.DataFrame()
                    total_calls = len(source)
                    rows = []
                    for behavior, mask_col in [
                        ("Adhered",   adh_c),
                        ("Slide",     sl_c),
                        ("All Plans", ap_c),
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

                    st.dataframe(
                        ov_styler,
                        use_container_width=True,
                        hide_index=True,
                        column_order=ov_col_order,
                        height=dataframe_display_height(len(ov_df)),
                    )
                    table_export_row(ov_df, "agent_period_overall_behavior.csv", key_suffix="agent_pop_ov")

            else:
                st.info("Not enough data in selected date ranges to compute metrics.")

            kpi_specs = [
                ("fp_cr", "First pitch conversion rate", "pct"),
                ("ov_cr", "Overall conversion rate", "pct"),
                ("gcv_fp", "GCV/First Pitch", "dollar"),
                ("gcv_call", "GCV/Call", "dollar"),
                ("rpo", "RPO", "dollar"),
            ]
            kpi_pre_col = f"Pre ({pre_label})"
            kpi_post_col = f"Post ({post_label})"
            kpi_delta_col = "% change"
            _kpi_rows = []
            for _mkey, _mtitle, _mfmt in kpi_specs:
                _pv = overall_metric(pre_df, _mkey)
                _qv = overall_metric(post_df, _mkey)
                _pre_cell = fmt_val(_pv, _mfmt)
                _post_cell = fmt_val(_qv, _mfmt)
                _kpi_rows.append({
                    "Metric": _mtitle,
                    kpi_pre_col: _pre_cell,
                    kpi_post_col: _post_cell,
                    kpi_delta_col: fmt_delta(_pv, _qv, _mfmt),
                })
            kpi_summary_df = pd.DataFrame(_kpi_rows)
            kpi_styler = (
                kpi_summary_df.style.map(color_delta_cell, subset=[kpi_delta_col])
                .set_properties(**{"text-align": "left"}, subset=["Metric"])
                .set_properties(**{"text-align": "right"}, subset=[kpi_pre_col, kpi_post_col, kpi_delta_col])
            )
            st.markdown("**Key Metrics**")
            st.dataframe(
                kpi_styler,
                use_container_width=True,
                hide_index=True,
                height=dataframe_display_height(len(kpi_summary_df)),
            )
            table_export_row(kpi_summary_df, "agent_period_key_metrics.csv", key_suffix="agent_kpi")
    else:
        missing = sorted(_pop_needed - set(df_raw.columns))
        st.info(f"Columns missing for comparison table: {', '.join(missing)}")

    st.divider()

    # ── Confusion Matrix ──────────────────────────────────────────────────────
    if agent_eff_mode == "Sale":
        st.subheader("Confusion Matrix")
        st.caption(
            "Converting calls only. Rows: plan type from the **sold** pitch (tier `sale_type` and plan category "
            "of the converting pitch, using the same rec-slot mapping as the first-pitch matrix). "
            "Columns: top recommended plan type."
        )
        cm_needed = {
            "sale_type", "top_recommended_plan_type", "recommended_plan_types_in_order",
            "order_count", "gcv", 
        }
        _cm_use_sale = True
    else:
        st.subheader("Confusion matrix — first pitch vs. recommended plan type")
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
        _cm_use_sale = False

    if cm_needed.issubset(df_agent.columns):
        import re as _re

        def norm_plan_type(x):
            if not isinstance(x, str):
                return None
            x = x.strip()
            if _re.search(r"\bFixed\b",   x, _re.IGNORECASE): return "Fixed"
            if _re.search(r"\bTiered\b",  x, _re.IGNORECASE): return "Tiered"
            if _re.search(r"\bBundled\b", x, _re.IGNORECASE): return "Bundled"
            return None

        def safe_parse_list(v):
            if isinstance(v, list):
                return v
            if not isinstance(v, str) or v.strip() in ("", "None", "nan", "null", "[]"):
                return []
            import re
            return re.findall(r"\b(Fixed|Tiered|Bundled)\b", v)

        cm_df = df_agent.dropna(subset=["top_recommended_plan_type"]).copy()
        cm_df = cm_df[~cm_df["recommended_plan_types_in_order"].astype(str).isin(
            ["", "None", "nan", "null", "[]"]
        )]
        cm_df["_rec_types"] = cm_df["recommended_plan_types_in_order"].apply(safe_parse_list)

        if _cm_use_sale:
            cm_df = cm_df[cm_df["order_count"].fillna(0) > 0]
            cm_df = cm_df.dropna(subset=["sale_type"])
        else:
            cm_df = cm_df.dropna(subset=["first_pitch_type"])

        def get_row_col_fp(row):
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

        def get_row_col_sale(row):
            st_val    = row["sale_type"]
            top_ptype = norm_plan_type(row.get("top_recommended_plan_type"))
            rec_types = [norm_plan_type(t) for t in row["_rec_types"]]

            if st_val == "Diamond":
                row_label = rec_types[0] if rec_types else None
                col_label = top_ptype
            elif st_val == "Gold":
                slide_types = rec_types[1:] if len(rec_types) > 1 else []
                row_label = slide_types[0] if slide_types else None
                col_label = top_ptype
            else:
                row_label = "Other"
                col_label = top_ptype
            return pd.Series({"row_label": row_label, "col_label": col_label})

        if _cm_use_sale:
            cm_df[["row_label", "col_label"]] = cm_df.apply(get_row_col_sale, axis=1)
        else:
            cm_df[["row_label", "col_label"]] = cm_df.apply(get_row_col_fp, axis=1)
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

        _ax_lines = plotly_axis_lines()
        _lc = _ax_lines["linecolor"]
        fig_cm = go.Figure(go.Heatmap(
            z=z_counts,
            x=COL_LABELS,
            y=ROW_LABELS,
            text=text_cells,
            texttemplate="%{text}",
            colorscale=heatmap_colorscale(),
            colorbar=heatmap_colorbar_dict(),
            hoverongaps=False,
        ))
        _y_cm_title = (
            "Sold pitch (canonical rec match → plan type)"
            if _cm_use_sale
            else "First pitch (canonical rec match → plan type)"
        )
        apply_chart_theme(fig_cm,
            xaxis=dict(
                title="Recommended plan type",
                side="bottom",
                gridcolor="rgba(0,0,0,0)",
                linecolor=_lc,
            ),
            yaxis=dict(
                title=_y_cm_title,
                autorange="reversed",
                gridcolor="rgba(0,0,0,0)",
                linecolor=_lc,
            ),
            height=460,
            margin=dict(l=100, r=40, t=20, b=80),
        )
        st.plotly_chart(fig_cm, use_container_width=True)
        if _cm_use_sale:
            d_ct = int((cm_df["sale_type"] == "Diamond").sum())
            g_ct = int((cm_df["sale_type"] == "Gold").sum())
        else:
            d_ct = int((cm_df["first_pitch_type"] == "Diamond").sum())
            g_ct = int((cm_df["first_pitch_type"] == "Gold").sum())
        st.caption(
            f"Calls in matrix: {total_calls:,}  ·  "
            f"Diamond: {d_ct:,}  ·  "
            f"Gold: {g_ct:,}  ·  "
            f"Other (Silver/Bronze/Other): {int(cm_df['row_label'].eq('Other').sum()):,}"
        )
    else:
        missing = sorted(cm_needed - set(df_agent.columns))
        st.info(f"Columns missing for confusion matrix: {', '.join(missing)}")




# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — SALE MIXES
# ════════════════════════════════════════════════════════════════════════════════
with tab_sale_mix:
    # ── Sale Mixes (sold partner / plan among converting calls) ───────────────
    st.subheader("Sale Mixes")
    st.caption(
        "Among **converting calls** only (`order_count` > 0). Uses `sold_partner_name` and `sold_plan_name` "
        "from the pipeline. Charts and pivot tables respect the sidebar **and** date filter. "
        "Partner and plan **mix** charts use the selections below (default top 5 each); enable **Other** to bucket the rest."
    )

    _sm_cols = {"call_date", "order_count", "sold_partner_name", "sold_plan_name"}
    if not _sm_cols.issubset(df.columns):
        st.info("Sale mixes need `call_date`, `order_count`, `sold_partner_name`, and `sold_plan_name` on the call-level file.")
    else:

        def _sm_display_str(ser: pd.Series, unknown: str = "(Unknown)") -> pd.Series:
            out = ser.astype("string")
            out = out.str.strip()
            out = out.mask(out.isna() | (out == "") | out.str.lower().isin(["none", "nan", "null"]), unknown)
            return out.str.slice(0, 100)

        def _sm_bucket_selected_other(
            ser: pd.Series,
            selected: list,
            *,
            include_other: bool,
            other: str = "Other",
        ) -> pd.Series:
            """Keep values in ``selected`` as-is; optionally map the rest to ``Other``, else NA."""
            sel = frozenset(s for s in selected if s is not None and str(s) != "")
            if not sel:
                return ser
            if include_other:
                return ser.where(ser.isin(sel), other)
            return ser.where(ser.isin(sel), pd.NA)

        sm_sales = df[df["order_count"].fillna(0) > 0].dropna(subset=["call_date"]).copy()
        sm_sales["partner"] = _sm_display_str(sm_sales["sold_partner_name"])
        sm_sales["plan"] = _sm_display_str(sm_sales["sold_plan_name"])

        if sm_sales.empty:
            st.info("No converting calls in the current filters and date range.")
        else:
            sm_sales["period"] = period_labels(sm_sales["call_date"], _chart_granularity)
            sm_sales["period_display"] = period_display(sm_sales["period"], _chart_granularity)

            st.markdown("### Providers (sold partner)")
            _pvc = sm_sales["partner"].value_counts()
            _partner_sig = (len(sm_sales), tuple(_pvc.head(40).items()))
            if st.session_state.get("sale_mix_partner_sig") != _partner_sig:
                st.session_state["sale_mix_sel_partners"] = list(_pvc.head(5).index)
                st.session_state["sale_mix_partner_sig"] = _partner_sig
            st.multiselect(
                "Partners in mix",
                options=list(_pvc.index),
                key="sale_mix_sel_partners",
                help="Default: top 5 providers by converting sales. Search to add or remove. "
                "Unselected providers roll into **Other** when that option is enabled.",
            )
            include_other_partners = st.checkbox(
                "Include **Other** (all providers not selected above)",
                value=True,
                key="sale_mix_partner_include_other",
            )
            _raw_p = st.session_state.get("sale_mix_sel_partners")
            _sel_p = list(_raw_p) if _raw_p else list(_pvc.head(5).index)
            sm_sales["partner_grp"] = _sm_bucket_selected_other(
                sm_sales["partner"], _sel_p, include_other=include_other_partners
            )
            sm_p = (
                sm_sales.dropna(subset=["partner_grp"])
                if not include_other_partners
                else sm_sales
            )

            st.markdown("#### Partner mix over time (% of sales)")
            pc = (
                sm_p.groupby(["period", "period_display", "partner_grp"], observed=True)
                .size()
                .reset_index(name="n")
            )
            pt = pc.groupby("period")["n"].transform("sum")
            pc["pct"] = pc["n"] / pt * 100
            partners_sorted = sorted(pc["partner_grp"].unique().tolist(), key=lambda x: (x == "Other", x))
            fig_sm_p = go.Figure()
            for ptn in partners_sorted:
                sub = pc[pc["partner_grp"] == ptn].sort_values("period")
                if sub.empty:
                    continue
                fig_sm_p.add_trace(go.Scatter(
                    x=sub["period_display"],
                    y=sub["pct"],
                    name=ptn,
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
            apply_chart_theme(
                fig_sm_p,
                **PAIR_CHART_LAYOUT,
                yaxis_ticksuffix="%",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig_sm_p, use_container_width=True)

            piv_p = (
                pc.pivot_table(index="partner_grp", columns="period_display", values="pct", aggfunc="sum")
                .fillna(0)
                .round(1)
            )
            piv_p = piv_p.reset_index().rename(columns={"partner_grp": "Partner"})
            st.dataframe(
                piv_p,
                use_container_width=True,
                hide_index=True,
                height=dataframe_display_height(len(piv_p)),
            )
            table_export_row(piv_p, "sale_mix_partner_by_period.csv", key_suffix="sale_mix_piv_p")

            st.markdown("### Sold plans")
            _plc = sm_sales["plan"].value_counts()
            _plan_sig = (len(sm_sales), tuple(_plc.head(60).items()))
            if st.session_state.get("sale_mix_plan_sig") != _plan_sig:
                st.session_state["sale_mix_sel_plans"] = list(_plc.head(5).index)
                st.session_state["sale_mix_plan_sig"] = _plan_sig
            st.multiselect(
                "Plans in mix",
                options=list(_plc.index),
                key="sale_mix_sel_plans",
                help="Default: top 5 sold plans by volume. Unselected plans roll into **Other** when enabled.",
            )
            include_other_plans = st.checkbox(
                "Include **Other** (all plans not selected above)",
                value=True,
                key="sale_mix_plan_include_other",
            )
            _raw_pl = st.session_state.get("sale_mix_sel_plans")
            _sel_pl = list(_raw_pl) if _raw_pl else list(_plc.head(5).index)
            sm_sales["plan_grp"] = _sm_bucket_selected_other(
                sm_sales["plan"], _sel_pl, include_other=include_other_plans
            )
            sm_pl = (
                sm_sales.dropna(subset=["plan_grp"])
                if not include_other_plans
                else sm_sales
            )

            st.markdown("#### Sold plan mix over time (% of sales)")
            plc = (
                sm_pl.groupby(["period", "period_display", "plan_grp"], observed=True)
                .size()
                .reset_index(name="n")
            )
            plt2 = plc.groupby("period")["n"].transform("sum")
            plc["pct"] = plc["n"] / plt2 * 100
            plans_sorted = sorted(plc["plan_grp"].unique().tolist(), key=lambda x: (x == "Other", x))
            fig_sm_pl = go.Figure()
            for pln in plans_sorted:
                sub = plc[plc["plan_grp"] == pln].sort_values("period")
                if sub.empty:
                    continue
                fig_sm_pl.add_trace(go.Scatter(
                    x=sub["period_display"],
                    y=sub["pct"],
                    name=pln[:40] + ("…" if len(pln) > 40 else ""),
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=4),
                ))
            apply_chart_theme(
                fig_sm_pl,
                **PAIR_CHART_LAYOUT,
                yaxis_ticksuffix="%",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig_sm_pl, use_container_width=True)

            piv_pl = (
                plc.pivot_table(index="plan_grp", columns="period_display", values="pct", aggfunc="sum")
                .fillna(0)
                .round(1)
            )
            piv_pl = piv_pl.reset_index().rename(columns={"plan_grp": "Sold plan"})
            st.dataframe(
                piv_pl,
                use_container_width=True,
                hide_index=True,
                height=dataframe_display_height(min(len(piv_pl), 40)),
            )
            table_export_row(piv_pl, "sale_mix_plan_by_period.csv", key_suffix="sale_mix_piv_pl")

            st.divider()
            st.subheader("Period-over-period comparison")
            st.caption(
                "Choose **Partners** or **Plans** for one comparison table. "
                "Pickers span the **full** `call_date` range in the extract (sidebar **date** filter does not apply); "
                "shares still use converting calls with other sidebar filters only. "
                "Defaults: **Post** = latest full Mon–Sun week vs raw max; **Pre** = four Mon–Sun weeks before that. "
                "**% change vs pre** is relative to the pre-period share."
            )

            sm_sales_cmp = (
                df_nodatefilter[df_nodatefilter["order_count"].fillna(0) > 0]
                .dropna(subset=["call_date"])
                .copy()
            )
            sm_sales_cmp["partner"] = _sm_display_str(sm_sales_cmp["sold_partner_name"])
            sm_sales_cmp["plan"] = _sm_display_str(sm_sales_cmp["sold_plan_name"])
            sm_sales_cmp["partner_grp"] = _sm_bucket_selected_other(
                sm_sales_cmp["partner"], _sel_p, include_other=include_other_partners
            )
            sm_sales_cmp["plan_grp"] = _sm_bucket_selected_other(
                sm_sales_cmp["plan"], _sel_pl, include_other=include_other_plans
            )

            sm_data_min = pd.to_datetime(df_raw["call_date"].min()).date()
            sm_data_max = pd.to_datetime(df_raw["call_date"].max()).date()
            _sm_cmp_sig = (sm_data_min, sm_data_max, len(sm_sales_cmp))
            if st.session_state.get("sale_mix_cmp_date_sig") != _sm_cmp_sig:
                st.session_state.pop("sale_mix_cmp_pre_range", None)
                st.session_state.pop("sale_mix_cmp_post_range", None)
                st.session_state["sale_mix_cmp_date_sig"] = _sm_cmp_sig

            _sm_pre_default, _sm_post_default = streamlit_safe_period_defaults(
                sm_data_max, sm_data_min
            )

            sale_cmp_dim = st.radio(
                "Compare",
                ["Partners", "Plans"],
                horizontal=True,
                key="sale_mix_cmp_dim",
            )

            sm_po1, sm_po2 = st.columns(2)
            with sm_po1:
                sm_pre_range = st.date_input(
                    "Pre period",
                    value=_sm_pre_default,
                    min_value=sm_data_min,
                    max_value=sm_data_max,
                    key="sale_mix_cmp_pre_range",
                )
            with sm_po2:
                sm_post_range = st.date_input(
                    "Post period",
                    value=_sm_post_default,
                    min_value=sm_data_min,
                    max_value=sm_data_max,
                    key="sale_mix_cmp_post_range",
                )

            def _sm_slice_sm_sales(d0: date, d1: date):
                lo, hi = sorted((d0, d1))
                m = (sm_sales_cmp["call_date"].dt.date >= lo) & (sm_sales_cmp["call_date"].dt.date <= hi)
                return sm_sales_cmp.loc[m]

            def _sm_share_mix(sub: pd.DataFrame, grp_col: str) -> pd.Series:
                if len(sub) == 0:
                    return pd.Series(dtype=float)
                return sub[grp_col].value_counts(normalize=True).mul(100).sort_values(ascending=False)

            def _sm_color_pct_chg(val):
                try:
                    x = float(val)
                except (TypeError, ValueError):
                    return ""
                if pd.isna(x):
                    return ""
                return theme.period_comparison_delta_style(x, neutral_abs=10.0)

            if len(sm_pre_range) == 2 and len(sm_post_range) == 2:
                pre_s = _sm_slice_sm_sales(sm_pre_range[0], sm_pre_range[1])
                post_s = _sm_slice_sm_sales(sm_post_range[0], sm_post_range[1])
                pre_lo, pre_hi = sorted(sm_pre_range)
                post_lo, post_hi = sorted(sm_post_range)
                pre_lab = f"{pre_lo.strftime('%-m/%-d')}-{pre_hi.strftime('%-m/%-d')}"
                post_lab = f"{post_lo.strftime('%-m/%-d')}-{post_hi.strftime('%-m/%-d')}"

                if sale_cmp_dim == "Partners":
                    pre_ss = pre_s.dropna(subset=["partner_grp"]) if not include_other_partners else pre_s
                    post_ss = post_s.dropna(subset=["partner_grp"]) if not include_other_partners else post_s
                    grp_col = "partner_grp"
                    axis_lbl = "Partner"
                    export_fn = "sale_mix_partner_period_compare.csv"
                    export_key = "sale_mix_popp"
                else:
                    pre_ss = pre_s.dropna(subset=["plan_grp"]) if not include_other_plans else pre_s
                    post_ss = post_s.dropna(subset=["plan_grp"]) if not include_other_plans else post_s
                    grp_col = "plan_grp"
                    axis_lbl = "Sold plan"
                    export_fn = "sale_mix_plan_period_compare.csv"
                    export_key = "sale_mix_popl"

                sh_pre = _sm_share_mix(pre_ss, grp_col)
                sh_post = _sm_share_mix(post_ss, grp_col)
                idx_x = sh_pre.index.union(sh_post.index)
                t_cmp = pd.DataFrame(
                    {
                        f"Share % ({pre_lab})": sh_pre.reindex(idx_x).fillna(0).round(1),
                        f"Share % ({post_lab})": sh_post.reindex(idx_x).fillna(0).round(1),
                    }
                )
                _cp0, _cp1 = t_cmp.columns[0], t_cmp.columns[1]
                _pv0 = t_cmp[_cp0].astype(float)
                _pv1 = t_cmp[_cp1].astype(float)
                t_cmp["% change vs pre"] = ((_pv1 / _pv0.replace(0, float("nan")) - 1.0).mul(100).round(1))
                t_cmp = t_cmp.sort_values(_cp1, ascending=False).rename_axis(axis_lbl).reset_index()

                sty_cmp = t_cmp.style.map(_sm_color_pct_chg, subset=["% change vs pre"])
                sty_cmp = sty_cmp.set_properties(**{"text-align": "right"}, subset=[_cp0, _cp1, "% change vs pre"])
                sty_cmp = sty_cmp.set_properties(**{"text-align": "left"}, subset=[axis_lbl])
                st.dataframe(
                    sty_cmp,
                    use_container_width=True,
                    hide_index=True,
                    height=dataframe_display_height(min(len(t_cmp), 40)),
                )
                table_export_row(t_cmp, export_fn, key_suffix=export_key)
            else:
                st.caption("Select full pre and post date ranges to populate the comparison table.")



# ════════════════════════════════════════════════════════════════════════════════
# TAB 4 — AGENT LEVEL
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

        dht1, dht2 = st.columns(2)
        dht1.markdown("**Diamond % Distribution**")
        dht2.markdown("**GCV / Call Distribution**")

        dc1, dc2 = st.columns(2)

        with dc1:
            fig_d = go.Figure(go.Histogram(
                x=agent_df["Diamond %"], nbinsx=20,
                marker_color=PLOT_COLORWAY[0], opacity=0.8,
                marker_line_color=histogram_marker_line(), marker_line_width=1,
            ))
            apply_chart_theme(fig_d,
                **PAIR_CHART_LAYOUT,
                xaxis_title="Diamond First-Pitch Rate (%)",
                yaxis_title="Agents",
            )
            st.plotly_chart(fig_d, use_container_width=True)

        with dc2:
            fig_g = go.Figure(go.Histogram(
                x=agent_df["GCV / Call"].dropna(), nbinsx=20,
                marker_color=PLOT_COLORWAY[1], opacity=0.8,
                marker_line_color=histogram_marker_line(), marker_line_width=1,
            ))
            apply_chart_theme(fig_g,
                **PAIR_CHART_LAYOUT,
                xaxis_title="GCV / Call ($)",
                yaxis_title="Agents",
            )
            st.plotly_chart(fig_g, use_container_width=True)

        fmt_df = agent_df.copy()
        for col in ["Diamond %", "Gold %", "Silver %", "Bronze %", "1st Pitch CR", "Overall CR"]:
            fmt_df[col] = fmt_df[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
        for col in ["GCV / Call", "GCV / 1st Pitch"]:
            fmt_df[col] = fmt_df[col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
        fmt_df["Points / Call"] = fmt_df["Points / Call"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        fmt_df["Calls"] = fmt_df["Calls"].apply(lambda x: f"{x:,}")

        st.dataframe(
            fmt_df,
            use_container_width=True,
            hide_index=True,
            height=dataframe_display_height(len(fmt_df)),
        )
        table_export_row(fmt_df, "agent_level_performance.csv", key_suffix="agent_level")

    else:
        missing = agent_needed - set(df.columns)
        st.info(f"Columns missing for agent table: {', '.join(sorted(missing))}")


_AI_PRIOR_TOOL_OMITTED = (
    "[prior result omitted — use only the is_final result above]"
)

_AI_RUNCODE_ERROR_SUFFIX = (
    "\n\nFix the specific error above. Do not repeat the same code. If the same approach "
    "has failed twice, try a completely different method."
)

AI_ANALYST_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "execute_python",
            "description": (
                "Execute Python against `df`. Assign final output to `result`. "
                "Never call print(). pandas=pd, numpy=np, plotly=go/px available. "
                "Date helpers for WTD/MTD/P4WA: analysis_as_of, analysis_wtd_start, analysis_mtd_start, "
                "analysis_ytd_start, analysis_p4wa_start, analysis_p4wa_end (datetime.date; analysis_data_max may be None)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "Python code assigning to `result`."},
                    "rationale": {"type": "string", "description": "One sentence: what and why."},
                    "is_final": {
                        "type": "boolean",
                        "description": (
                            "Set true ONLY on the last tool call before your final answer. "
                            "When true, your code must produce the single authoritative result "
                            "your narrative will be based on. You may NOT reference numbers from "
                            "earlier steps in your final answer — only from this result."
                        ),
                    },
                },
                "required": ["code", "rationale", "is_final"],
            },
        },
    }
]


def _user_message_is_text_only(m: dict) -> bool:
    if m.get("role") != "user":
        return False
    c = m.get("content")
    if isinstance(c, str):
        return True
    if isinstance(c, list) and c:
        return all(isinstance(b, dict) and b.get("type") == "text" for b in c)
    return False


def strip_prior_tool_results_keep_final(
    messages: list[dict],
    keep_tool_call_id: str,
    *,
    placeholder: str = _AI_PRIOR_TOOL_OMITTED,
) -> None:
    """Blank prior tool outputs except the is_final tool_call_id (OpenAI tool role messages)."""
    for m in messages:
        if m.get("role") == "tool" and m.get("tool_call_id") != keep_tool_call_id:
            m["content"] = placeholder


def truncate_ai_agent_messages(messages: list[dict]) -> list[dict]:
    """Keep first user message; treat each (assistant w/ tool_calls + matching tool msgs) as one atomic span."""
    if not messages:
        return []
    first_user_i = None
    for i, m in enumerate(messages):
        if m.get("role") == "user" and _user_message_is_text_only(m):
            first_user_i = i
            break
    if first_user_i is None:
        for i, m in enumerate(messages):
            if m.get("role") == "user":
                first_user_i = i
                break
    if first_user_i is None:
        return list(messages)

    spans: list[tuple[int, int]] = []
    i = first_user_i + 1
    while i < len(messages):
        m = messages[i]
        if m.get("role") == "assistant" and m.get("tool_calls"):
            ntc = len(m["tool_calls"])
            j = i + 1
            got = 0
            while j < len(messages) and got < ntc:
                if messages[j].get("role") != "tool":
                    break
                got += 1
                j += 1
            if got == ntc:
                spans.append((i, j - 1))
                i = j
                continue
        i += 1

    if len(spans) <= 4:
        return list(messages)

    start_keep = spans[-4][0]
    if start_keep <= first_user_i:
        return list(messages)
    return [messages[first_user_i]] + messages[start_keep:]


def build_ai_example_questions(_df_raw: pd.DataFrame) -> list[str]:
    """Short example prompts for the AI Analyst empty state."""
    return [
        "Plot Durban's weekly adherence rate since 4/18.",
        "What is the overall CR across each center?",
        "Which agents have the highest 1st pitch CR?",
        "Show GCV/Call by marketing bucket.",
    ]


# ════════════════════════════════════════════════════════════════════════════════
# TAB 5 — AI ANALYST
# ════════════════════════════════════════════════════════════════════════════════
with tab_chat:


    def run_code(code: str, dataframe: pd.DataFrame):
        import plotly.graph_objects as _go
        import plotly.express as _px
        _time_ns = _ai_analyst_time_bundle(dataframe)["namespace"]
        local_ns = {
            "df":              dataframe.copy(),
            "df_nodatefilter": df_nodatefilter.copy(),
            "df_filtered":     df.copy(),
            "df_raw":          dataframe.copy(),
            "pd":              pd,
            "np":              __import__("numpy"),
            "go":              _go,
            "px":              _px,
            **_time_ns,
        }
        try:
            exec(code, {}, local_ns)  # noqa: S102
            result = local_ns.get("result", "⚠️ No `result` variable assigned.")
            return result, None
        except Exception:
            return None, _tb.format_exc()

    def format_for_model(result) -> str:
        import plotly.graph_objects as _go
        if result is None:
            return "None"
        if isinstance(result, _go.Figure):
            traces = []
            for trace in result.data:
                x = list(trace.x) if hasattr(trace, "x") and trace.x is not None else []
                y = list(trace.y) if hasattr(trace, "y") and trace.y is not None else []
                traces.append(
                    f"  Trace '{trace.name}': "
                    f"x={x[:5]}{'...' if len(x) > 5 else ''}, "
                    f"y={y[:5]}{'...' if len(y) > 5 else ''}"
                )
            return "Plotly Figure with traces:\n" + "\n".join(traces)
        if isinstance(result, dict):
            parts = []
            for k, v in result.items():
                if isinstance(v, _go.Figure):
                    parts.append(f"{k}: [Plotly Figure — see trace summary above]")
                elif isinstance(v, pd.DataFrame):
                    parts.append(f"{k} (DataFrame {v.shape}):\n{v.to_string(max_rows=50)}")
                elif isinstance(v, pd.Series):
                    parts.append(f"{k} (Series {len(v)}):\n{v.to_string(max_rows=50)}")
                else:
                    parts.append(f"{k}: {v}")
            return "\n\n".join(parts)
        if isinstance(result, pd.DataFrame):
            return (
                f"DataFrame: {result.shape[0]} rows × {result.shape[1]} cols\n"
                f"{result.to_string(max_rows=50, max_cols=20)}"
            )
        if isinstance(result, pd.Series):
            return f"Series ({len(result)} items):\n{result.to_string(max_rows=50)}"
        return str(result)

    def _ai_export_slug(text: str, *, max_len: int = 40) -> str:
        return "".join(c if c.isalnum() else "_" for c in str(text))[:max_len]

    def render_step_body(step: dict, *, export_key_suffix: str = "step"):
        kind = step.get("kind")
        if kind == "user":
            st.markdown(step.get("content", ""))
        elif kind == "thinking":
            st.markdown(f"**Planning - {step.get('summary', '')}**")
            st.markdown(step.get("content", ""))
        elif kind == "code":
            st.markdown(f"**Step {step.get('n')} - {step.get('rationale', '')}**")
            st.code(step.get("code", ""), language="python")
        elif kind == "result":
            import plotly.graph_objects as _go
            r = step.get("content", step.get("result"))
            step_label = step.get("step_num", step.get("n"))
            st.markdown(f"**Result {step_label}**")
            if isinstance(r, dict):
                if "figure" in r and isinstance(r["figure"], _go.Figure):
                    st.plotly_chart(r["figure"], use_container_width=True)
                if "summary" in r:
                    if isinstance(r["summary"], pd.DataFrame):
                        sdf = r["summary"]
                        st.dataframe(
                            sdf,
                            use_container_width=True,
                            hide_index=True,
                            height=dataframe_display_height(len(sdf)),
                        )
                        table_export_row(
                            sdf,
                            f"ai_analyst_{export_key_suffix}_summary.csv",
                            key_suffix=f"{export_key_suffix}_summary",
                        )
                    elif isinstance(r["summary"], pd.Series):
                        sdf = r["summary"].reset_index()
                        st.dataframe(
                            sdf,
                            use_container_width=True,
                            hide_index=True,
                            height=dataframe_display_height(len(sdf)),
                        )
                        table_export_row(
                            sdf,
                            f"ai_analyst_{export_key_suffix}_summary_series.csv",
                            key_suffix=f"{export_key_suffix}_summary_series",
                        )
                    else:
                        st.write(r["summary"])
                remaining = {k: v for k, v in r.items() if k not in ("figure", "summary")}
                for k, v in remaining.items():
                    if isinstance(v, pd.DataFrame):
                        st.markdown(f"**{k}**")
                        slug = _ai_export_slug(k)
                        st.dataframe(
                            v,
                            use_container_width=True,
                            hide_index=True,
                            height=dataframe_display_height(len(v)),
                        )
                        table_export_row(
                            v,
                            f"ai_analyst_{export_key_suffix}_{slug}.csv",
                            key_suffix=f"{export_key_suffix}_{slug}",
                        )
                    elif isinstance(v, pd.Series):
                        st.markdown(f"**{k}**")
                        slug = _ai_export_slug(k)
                        ser_df = v.reset_index()
                        st.dataframe(
                            ser_df,
                            use_container_width=True,
                            hide_index=True,
                            height=dataframe_display_height(len(ser_df)),
                        )
                        table_export_row(
                            ser_df,
                            f"ai_analyst_{export_key_suffix}_{slug}_series.csv",
                            key_suffix=f"{export_key_suffix}_{slug}_ser",
                        )
                    else:
                        st.write(f"**{k}:**", v)
            elif isinstance(r, _go.Figure):
                st.plotly_chart(r, use_container_width=True)
            elif isinstance(r, pd.DataFrame):
                st.dataframe(
                    r,
                    use_container_width=True,
                    hide_index=True,
                    height=dataframe_display_height(len(r)),
                )
                table_export_row(
                    r,
                    f"ai_analyst_{export_key_suffix}.csv",
                    key_suffix=export_key_suffix,
                )
            elif isinstance(r, pd.Series):
                ser_df = r.reset_index()
                st.dataframe(
                    ser_df,
                    use_container_width=True,
                    hide_index=True,
                    height=dataframe_display_height(len(ser_df)),
                )
                table_export_row(
                    ser_df,
                    f"ai_analyst_{export_key_suffix}_series.csv",
                    key_suffix=f"{export_key_suffix}_series",
                )
            else:
                st.write(r)
        elif kind == "error":
            st.markdown(f"**Error on step {step.get('n')} - retrying...**")
            st.code(step.get("error", ""), language="text")
        elif kind == "answer":
            st.markdown(step.get("content", ""))

    def render_step(step: dict, *, export_key_suffix: str = "step"):
        kind = step.get("kind")
        if kind == "user":
            import html as _html

            st.markdown(
                f"""
                <div class="ai-user-row">
                    <div class="ai-user-bubble">{_html.escape(step.get("content", ""))}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif kind == "answer":
            render_step_body(step, export_key_suffix=export_key_suffix)
        else:
            with st.expander(f"Analysis step - {kind}", expanded=False):
                render_step_body(step, export_key_suffix=export_key_suffix)

    def render_agent_transcript(steps: list[dict]):
        i = 0
        while i < len(steps):
            step = steps[i]
            if step.get("kind") == "user":
                render_step(step)
                i += 1

            intermediate_steps = []
            final_answer = None
            while i < len(steps) and steps[i].get("kind") != "user":
                if steps[i].get("kind") == "answer":
                    final_answer = steps[i]
                    i += 1
                    break
                intermediate_steps.append((steps[i], i))
                i += 1

            if intermediate_steps:
                with st.expander(f"Analysis steps ({len(intermediate_steps)})", expanded=False):
                    for j, (intermediate, step_i) in enumerate(intermediate_steps):
                        render_step_body(intermediate, export_key_suffix=f"mid_{step_i}_{j}")
                        if j < len(intermediate_steps) - 1:
                            st.divider()

            if final_answer is not None:
                render_step(final_answer, export_key_suffix=f"fin_{steps.index(final_answer)}")

            if step.get("kind") != "user" and final_answer is None and not intermediate_steps:
                render_step(step, export_key_suffix=f"orphan_{steps.index(step)}")
                i += 1

    _schema_display = build_schema_context(df_raw)

    if "agent_steps" not in st.session_state:
        st.session_state.agent_steps = []
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []
    if "ai_analyst_pending_example" not in st.session_state:
        st.session_state.ai_analyst_pending_example = None
    if "ai_analyst_pending_user_input" not in st.session_state:
        st.session_state.ai_analyst_pending_user_input = None
    if "ai_analyst_limit_warning" not in st.session_state:
        st.session_state.ai_analyst_limit_warning = False

    st.markdown(
        """
        <style>
        .ai-empty-state {
            min-height: 42vh;
            display: flex;
            flex-direction: column;
            justify-content: center;
            text-align: center;
        }
        .ai-empty-state h1 {
            opacity: 0.45;
            font-size: 3.25rem;
            font-weight: 600;
            margin-bottom: 2rem;
        }
        div[data-testid="stButton"] button,
        div[data-testid="stButton"] button[kind],
        div[data-testid="stButton"] button[data-testid*="baseButton"] {
            border-radius: 999px !important;
            padding: 0.55rem 1rem !important;
            white-space: normal !important;
            background-color: rgb(39, 39, 42) !important;
            background: rgb(39, 39, 42) !important;
            border: 1px solid rgb(113, 113, 122) !important;
            box-shadow: none !important;
            color: rgb(212, 212, 216) !important;
            transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease !important;
        }
        div[data-testid="stButton"] button p,
        div[data-testid="stButton"] button span {
            color: rgb(212, 212, 216) !important;
        }
        div[data-testid="stButton"] button:hover:enabled,
        div[data-testid="stButton"] button[kind]:hover:enabled,
        div[data-testid="stButton"] button[data-testid*="baseButton"]:hover:enabled {
            background-color: rgb(63, 63, 70) !important;
            background: rgb(63, 63, 70) !important;
            border-color: rgb(161, 161, 170) !important;
            color: rgb(244, 244, 245) !important;
        }
        div[data-testid="stButton"] button:hover:enabled p,
        div[data-testid="stButton"] button:hover:enabled span {
            color: rgb(244, 244, 245) !important;
        }
        div[data-testid="stButton"] button:disabled,
        div[data-testid="stButton"] button[kind]:disabled,
        div[data-testid="stButton"] button[data-testid*="baseButton"]:disabled {
            background-color: rgba(39, 39, 42, 0.58) !important;
            background: rgba(39, 39, 42, 0.58) !important;
            border-color: rgba(113, 113, 122, 0.45) !important;
            color: rgba(212, 212, 216, 0.55) !important;
        }
        div[data-testid="stButton"] button:disabled p,
        div[data-testid="stButton"] button:disabled span {
            color: rgba(212, 212, 216, 0.55) !important;
        }
        .ai-user-row {
            width: 100%;
            display: flex;
            justify-content: flex-end;
            margin: 0.75rem 0 1rem;
        }
        .ai-user-bubble {
            max-width: 75%;
            border-radius: 1rem;
            padding: 0.8rem 1rem;
            background: rgba(49, 130, 206, 0.14);
            border: 1px solid rgba(49, 130, 206, 0.22);
            line-height: 1.45;
            overflow-wrap: anywhere;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    def queue_example(question: str):
        st.session_state.ai_analyst_pending_example = question

    has_chat = len(st.session_state.agent_steps) > 0
    pending_example = st.session_state.ai_analyst_pending_example
    pending_user_input = st.session_state.ai_analyst_pending_user_input
    pending_request = pending_example or pending_user_input

    if not has_chat and not pending_request:
        st.markdown('<div class="ai-empty-state"><h1>Ask a question</h1></div>', unsafe_allow_html=True)
        example_questions = build_ai_example_questions(df_raw)
        ex_cols = st.columns(2)
        for i, question in enumerate(example_questions):
            ex_cols[i % 2].button(
                question,
                key=f"ai_example_{i}",
                use_container_width=True,
                on_click=queue_example,
                args=(question,),
            )

    render_agent_transcript(st.session_state.agent_steps)

    if st.session_state.ai_analyst_limit_warning:
        st.warning("The AI Analyst reached the maximum number of tool steps. Ask a narrower follow-up or clear the chat and try again.")

    if pending_request:
        user_input = pending_request
        st.session_state.ai_analyst_pending_example = None
        st.session_state.ai_analyst_pending_user_input = None
        st.session_state.ai_analyst_limit_warning = False
        _time_bundle = _ai_analyst_time_bundle(df_raw)
        full_system = (
            AI_ANALYST_SYSTEM_PROMPT
            + "\n\n"
            + _time_bundle["markdown"]
            + f"\n\nCURRENT DATASET SCHEMA:\n{_schema_display}"
        )
        client = _OpenAI()

        user_step = {"kind": "user", "content": user_input}
        st.session_state.agent_steps.append(user_step)
        st.session_state.agent_messages.append({"role": "user", "content": user_input})
        render_step(user_step, export_key_suffix=f"live_{len(st.session_state.agent_steps) - 1}")

        MAX_STEPS = 16
        step_num = 0
        pending_final_strip = False
        final_tool_call_id: str | None = None
        code_error_by_hash: dict[str, int] = {}

        with st.status("Agent is running...", expanded=False) as run_status:
            while step_num < MAX_STEPS:
                if pending_final_strip and final_tool_call_id:
                    strip_prior_tool_results_keep_final(
                        st.session_state.agent_messages,
                        final_tool_call_id,
                    )
                    pending_final_strip = False
                    final_tool_call_id = None

                msgs_for_api = truncate_ai_agent_messages(st.session_state.agent_messages)
                try:
                    response = client.chat.completions.create(
                        model="gpt-4o",
                        tools=AI_ANALYST_TOOLS,
                        tool_choice="auto",
                        messages=[{"role": "system", "content": full_system}] + msgs_for_api,
                    )
                except Exception:
                    err = _tb.format_exc()
                    err_step = {"kind": "error", "n": step_num + 1, "error": err}
                    st.session_state.agent_steps.append(err_step)
                    break

                msg = response.choices[0].message
                tool_calls = msg.tool_calls or []
                msg_content = msg.content or ""

                if response.choices[0].finish_reason == "stop" and not tool_calls:
                    answer_step = {"kind": "answer", "content": msg_content}
                    st.session_state.agent_steps.append(answer_step)
                    st.session_state.agent_messages.append({"role": "assistant", "content": msg_content})
                    break

                if tool_calls:
                    if msg_content.strip():
                        thinking_step = {
                            "kind": "thinking",
                            "summary": msg_content.strip()[:80]
                            + ("..." if len(msg_content.strip()) > 80 else ""),
                            "content": msg_content,
                        }
                        st.session_state.agent_steps.append(thinking_step)

                    st.session_state.agent_messages.append(
                        {
                            "role": "assistant",
                            "content": msg_content,
                            "tool_calls": [
                                {
                                    "id": tc.id,
                                    "type": "function",
                                    "function": {
                                        "name": tc.function.name,
                                        "arguments": tc.function.arguments,
                                    },
                                }
                                for tc in tool_calls
                            ],
                        },
                    )

                    for tool_call in tool_calls:
                        fname = getattr(tool_call.function, "name", "") or ""
                        if fname != "execute_python":
                            st.session_state.agent_messages.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": f"Unsupported tool: {fname}",
                                },
                            )
                            continue

                        try:
                            args = _json.loads(tool_call.function.arguments)
                        except Exception:
                            args = {"code": "", "rationale": "Could not parse tool arguments."}

                        code = args.get("code", "") or ""
                        rationale = args.get("rationale", "") or ""
                        is_final = bool(args.get("is_final", False))
                        step_num += 1
                        run_status.update(
                            label=f"Agent is running... step {step_num}",
                            state="running",
                            expanded=False,
                        )

                        code_step = {
                            "kind": "code",
                            "n": step_num,
                            "code": code,
                            "rationale": rationale,
                        }
                        st.session_state.agent_steps.append(code_step)

                        h = hashlib.sha256(code.encode("utf-8")).hexdigest()
                        if code_error_by_hash.get(h, 0) > 2:
                            skip_msg = (
                                "Tool execution was skipped: identical code has failed more than twice. "
                                "Move on with a different approach or question — do not retry this code."
                                f" (tool_call_id={tool_call.id})"
                            )
                            st.session_state.agent_messages.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": skip_msg,
                                },
                            )
                            fail_step = {
                                "kind": "error",
                                "n": step_num,
                                "error": skip_msg,
                            }
                            st.session_state.agent_steps.append(fail_step)
                            continue

                        result, error = run_code(code, df_raw)
                        if error:
                            code_error_by_hash[h] = code_error_by_hash.get(h, 0) + 1
                            error_step = {"kind": "error", "n": step_num, "error": error}
                            st.session_state.agent_steps.append(error_step)
                            err_body = (
                                f"ERROR:\n{error}{_AI_RUNCODE_ERROR_SUFFIX}"
                                f"\n\n(tool_call_id={tool_call.id}; failures for this exact code: {code_error_by_hash[h]})"
                            )
                            st.session_state.agent_messages.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": err_body,
                                },
                            )
                        else:
                            code_error_by_hash.pop(h, None)
                            result_step = {"kind": "result", "step_num": step_num, "content": result}
                            st.session_state.agent_steps.append(result_step)
                            step_count = sum(
                                1
                                for s in st.session_state.agent_steps
                                if s["kind"] == "code"
                            )
                            tool_content = format_for_model(result)

                            if is_final:
                                tool_content += (
                                    "\n\n--- FINAL RESULT LOCK ---"
                                    "\nYour narrative MUST be derived exclusively from the result above."
                                    "\nDo NOT reference any numbers, rankings, or names from previous steps."
                                    "\nEvery specific value you mention must appear verbatim in this result."
                                    "\nIf this result is a chart, do not list specific data points in prose unless "
                                    "your code explicitly extracted those values into the result dict."
                                    "\nIf you notice any inconsistency between this result and what you expected, "
                                    "call execute_python again rather than papering over it in prose."
                                )
                                pending_final_strip = True
                                final_tool_call_id = tool_call.id
                            elif step_count > 1:
                                tool_content += (
                                    f"\n\n--- CONSISTENCY REMINDER (step {step_num}) ---"
                                    f"\nYou have now run {step_num} code steps. When you are ready to give your final answer, "
                                    f"your last code step (with is_final=true) must produce a single unified result containing "
                                    f"everything your answer will reference. Do not split the final answer across multiple steps "
                                    f"and then merge them mentally — produce one self-contained final result."
                                )

                            st.session_state.agent_messages.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": tool_content,
                                },
                            )
                else:
                    if msg_content.strip():
                        answer_step = {"kind": "answer", "content": msg_content}
                        st.session_state.agent_steps.append(answer_step)
                    st.session_state.agent_messages.append({"role": "assistant", "content": msg_content})
                    break

            run_status.update(label="Agent finished.", state="complete", expanded=False)

        if step_num >= MAX_STEPS:
            st.session_state.ai_analyst_limit_warning = True
        st.rerun()

    typed_user_input = st.chat_input("What do you want to know?", key="ai_analyst_input")
    if typed_user_input:
        st.session_state.ai_analyst_pending_user_input = typed_user_input
        st.rerun()

    if st.button("Clear", key="ai_analyst_clear", disabled=not has_chat):
        st.session_state.agent_steps = []
        st.session_state.agent_messages = []
        st.session_state.ai_analyst_pending_example = None
        st.session_state.ai_analyst_pending_user_input = None
        st.session_state.ai_analyst_limit_warning = False
        st.rerun()