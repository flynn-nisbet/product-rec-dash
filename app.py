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
    sel_center   = st.multiselect("Center", options=centers_opts, default=center_defaults, key="filter_center")

    # Agent filter with search — st.multiselect has built-in search when there are many options
    sel_agent    = st.multiselect(
        "Agent",
        options=agent_opts,
        default=[],
        key="filter_agent",
        placeholder="Search agents…",
    )

    rec_type_opts = sorted(df_raw["top_recommended_plan_type"].dropna().unique().tolist()) if "top_recommended_plan_type" in df_raw.columns else []
    happy_path_tf = st.selectbox(
        "Happy Path Only",
        options=["True", "False"],
        index=0,
        key="filter_happy_path_only",
        help="True: happy_path = 1 (Arcadia target, no failed qualification, no Payless pitch, no Low rec). False: all calls.",
    )
    happy_only = happy_path_tf == "True"

    with st.expander("Advanced filters", expanded=False):
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
        sel_rec_type = st.multiselect("Rec Product Type", options=rec_type_opts, default=[], key="filter_rec_type")

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
        "center_location", "top_recommended_plan_type",
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
    """Renders a compact copy action below a table."""
    tsv = display_df.to_csv(index=False, sep="\t")
    uid = hashlib.md5((download_filename + "\0" + key_suffix).encode(), usedforsecurity=False).hexdigest()[:12]
    copy_bg = "#f8fafc" if theme.is_light_theme() else "#181c25"
    copy_text = "#475569" if theme.is_light_theme() else "#8b95aa"
    copy_border = "#cbd5e1" if theme.is_light_theme() else "#2e3649"
    copy_col, _spacer = st.columns([0.65, 6.35])
    with copy_col:
        tsv_literal = _json.dumps(tsv)
        lbl_literal = _json.dumps(copy_label)
        components.html(
            f"""<div style="font-family:DM Sans,sans-serif;padding:0;margin:0;">
<button type="button" id="cpbtn_{uid}"
  style="background:{copy_bg};color:{copy_text};border:1px solid {copy_border};border-radius:999px;box-sizing:border-box;
  width:100%;min-height:2.25rem;height:2.25rem;padding:0 0.85rem;font-size:0.78rem;font-weight:500;
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


def render_table_expander(
    label: str,
    display_df: pd.DataFrame,
    export_filename: str,
    *,
    key_suffix: str,
    height_rows: int | None = None,
) -> None:
    """Keep supporting data available without making it compete with the chart."""
    with st.expander(label, expanded=False):
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=dataframe_display_height(height_rows if height_rows is not None else len(display_df)),
        )
        table_export_row(display_df, export_filename, key_suffix=key_suffix)


def mix_share_pct(slice_df: pd.DataFrame, plan_type: str) -> float:
    if slice_df.empty or "top_recommended_plan_type" not in slice_df.columns:
        return float("nan")
    return (slice_df["top_recommended_plan_type"] == plan_type).mean() * 100


PERFORMANCE_METRICS = ["1st Pitch CR", "Overall CR", "GCV / 1st Pitch", "GCV / Call", "RPO"]


def calc_performance_metric(source: pd.DataFrame, metric: str) -> float:
    if source.empty:
        return float("nan")
    if metric == "Calls":
        return float(len(source))
    if metric == "1st Pitch CR":
        if "gcv_on_first_pitch" not in source.columns:
            return float("nan")
        return (source["gcv_on_first_pitch"] > 0).mean() * 100
    if metric == "Overall CR":
        if "order_count" not in source.columns:
            return float("nan")
        return (source["order_count"].fillna(0) > 0).mean() * 100
    if metric == "GCV / 1st Pitch":
        if "gcv_on_first_pitch" not in source.columns:
            return float("nan")
        return source["gcv_on_first_pitch"].mean()
    if metric == "GCV / Call":
        if "gcv" not in source.columns:
            return float("nan")
        return source["gcv"].mean()
    if metric == "RPO":
        if "order_count" not in source.columns or "gcv" not in source.columns:
            return float("nan")
        orders = source[source["order_count"].fillna(0) > 0]
        return orders["gcv"].mean() if len(orders) else float("nan")
    return float("nan")


def metric_axis_kwargs(metric: str) -> dict:
    if metric.endswith("%") or metric in ("1st Pitch CR", "Overall CR", "Share of Calls", "Share of Sales", "Tier Mix"):
        return {"yaxis_ticksuffix": "%"}
    if metric in ("GCV / 1st Pitch", "GCV / Call", "RPO", "GCV / Sale"):
        return {"yaxis_tickprefix": "$"}
    return {}


def format_chart_value(value, metric: str) -> str:
    try:
        if value is None or pd.isna(value):
            return "—"
    except TypeError:
        return "—"
    if metric.endswith("%") or metric in ("1st Pitch CR", "Overall CR", "Share of Calls", "Share of Sales", "Tier Mix"):
        return f"{float(value):.1f}%"
    if metric in ("GCV / 1st Pitch", "GCV / Call", "RPO", "GCV / Sale"):
        return f"${float(value):,.0f}"
    return f"{float(value):,.0f}"


def prepare_agent_behavior_dataframe(d: pd.DataFrame, adherence_mode: str):
    """Build agent-tab frame with ``agent_tier_display`` from first-pitch or sale tier.

    Returns ``(frame, spec, effective_mode)``. If ``Sale`` is requested but required columns
    are missing, falls back to first-pitch columns and ``effective_mode == "First Pitch"``.
    """
    out = d.copy()
    base_spec = {}
    sale_needed = {"sale_type", "order_count"}
    if adherence_mode != "Sale" or not sale_needed.issubset(out.columns):
        if "first_pitch_type" in out.columns:
            out["agent_tier_display"] = out["first_pitch_type"]
        else:
            out["agent_tier_display"] = pd.NA
        eff = "First Pitch" if adherence_mode == "Sale" else adherence_mode
        return out, base_spec, eff

    ord_pos = out["order_count"].fillna(0) > 0
    stype = out["sale_type"]

    # Tier mix / tier filters: only defined on converting calls with a canonical tier
    # (non-sales, missing sale_type, or non-D/G/S/B values stay NA — excluded from mix charts).
    out["agent_tier_display"] = pd.NA
    tier_known = ord_pos & stype.notna() & stype.isin(SALE_TIER_ORDER)
    out.loc[tier_known, "agent_tier_display"] = stype.loc[tier_known].astype(str)

    return out, base_spec, "Sale"


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
    st.subheader(
        "Recommendation Mix",
        help=(
            "Trend model recommendation mix by plan type or product slot. KPI cards compare WTD plan-type "
            "mix against pooled P4WA and ignore the sidebar date filter."
        ),
    )

    prod_col_candidates = ["recommended_in_order", "recommended_matched_in_order"]
    prod_col = next((c for c in prod_col_candidates if c in df.columns), None)
    rec_view_options = ["Plan type"] + (["Product"] if prod_col is not None else [])

    rm_c1, rm_c2, rm_c3 = st.columns([1.1, 1.1, 1])
    with rm_c1:
        rec_mix_view = st.selectbox(
            "View",
            rec_view_options,
            key="rec_mix_view",
            help="Plan type uses the #1 model recommendation. Product uses a ranked recommendation slot.",
        )
    with rm_c2:
        rec_mix_metric = st.selectbox(
            "Metric",
            ["Share of Calls", "Call Count"],
            key="rec_mix_metric",
            help="Share uses all calls in the period as the denominator.",
        )
    with rm_c3:
        rec_mix_top_n = st.slider(
            "Top categories",
            min_value=3,
            max_value=20,
            value=10,
            key="rec_mix_top_n",
            help="Limits the chart when many categories are available.",
        )

    if rec_mix_view == "Plan type" and "top_recommended_plan_type" in df_nodatefilter.columns and "call_date" in df_nodatefilter.columns:
        plan_types_all = sorted(df_nodatefilter["top_recommended_plan_type"].dropna().unique().tolist())
        if plan_types_all:
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
        rec_ts["value"] = rec_ts["pct"] if rec_mix_metric == "Share of Calls" else rec_ts["n"]

        plan_types = rec_ts["top_recommended_plan_type"].value_counts().head(rec_mix_top_n).index.tolist()

        fig_mix = go.Figure()
        for pt in plan_types:
            sub = rec_ts[rec_ts["top_recommended_plan_type"] == pt]
            fig_mix.add_trace(go.Scatter(
                x=sub["period_display"], y=sub["value"],
                name=pt, mode="lines+markers",
                line=dict(width=2),
                marker=dict(size=5),
            ))
        apply_chart_theme(
            fig_mix,
            **PAIR_CHART_LAYOUT,
            yaxis_title=rec_mix_metric,
            legend=dict(**PAIR_LEGEND_BELOW),
            **metric_axis_kwargs(rec_mix_metric),
        )
        st.plotly_chart(fig_mix, use_container_width=True)

        rec_ts_tbl = rec_ts.copy()
        rec_pivot = (
            rec_ts_tbl[rec_ts_tbl["top_recommended_plan_type"].isin(plan_types)]
            .pivot(index="top_recommended_plan_type", columns="period_display", values="value")
            .reset_index()
            .rename(columns={"top_recommended_plan_type": "Plan Type"})
        )
        for col in rec_pivot.columns[1:]:
            rec_pivot[col] = rec_pivot[col].apply(lambda v: format_chart_value(v, rec_mix_metric))
        render_table_expander(
            "Data table",
            rec_pivot,
            "recommendation_mix_pivot.csv",
            key_suffix="model_rec_mix",
        )
    elif rec_mix_view == "Plan type":
        st.info("call_date or top_recommended_plan_type column missing.")

    if rec_mix_view == "Product":
        st.markdown(
            "**Product-Level Mix**",
            help=(
                "Share or count of calls where a specific product appears in the Diamond or Gold recommendation slot."
            ),
        )

    # Determine which columns hold the ranked product recommendations.
    # Current CSVs expose matched recommendation names as recommended_matched_in_order.
    prod_col_candidates = ["recommended_in_order", "recommended_matched_in_order"]
    prod_col = next((c for c in prod_col_candidates if c in df.columns), None)

    if rec_mix_view == "Product" and prod_col is not None and "call_date" in df.columns:
        # Build a dataframe with diamond product and gold product per call
        prod_df = df.dropna(subset=["call_date"]).copy()
        prod_df["diamond_product"] = prod_df[prod_col].apply(lambda x: _extract_ranked_slot_product(x, 0))
        prod_df["gold_product"] = prod_df[prod_col].apply(lambda x: _extract_ranked_slot_product(x, 1))

        all_diamond_products = sorted(prod_df["diamond_product"].dropna().unique().tolist())
        all_gold_products    = sorted(prod_df["gold_product"].dropna().unique().tolist())

        pml_c1, pml_c2 = st.columns(2)
        with pml_c1:
            pm_slot = st.selectbox(
                "Recommendation Slot",
                options=["Diamond", "Gold"],
                key="pm_slot",
                help="Recommendation slot to plot: Diamond is slot 1; Gold is slot 2.",
            )
        with pml_c2:
            slot_product_opts = all_diamond_products if pm_slot == "Diamond" else all_gold_products
            pm_products = st.multiselect(
                "Products (leave blank for all)",
                options=slot_product_opts,
                default=[],
                key="pm_products",
                help="Leave blank to show the top 10 products by volume for the selected slot.",
            )

        slot_product_col = "diamond_product" if pm_slot == "Diamond" else "gold_product"
        pm_df = prod_df.dropna(subset=[slot_product_col]).copy()

        if pm_products:
            pm_df = pm_df[pm_df[slot_product_col].isin(pm_products)]
            products_to_plot = pm_products
        else:
            # Show top 10 by frequency to avoid chart overload
            top_products = (
                pm_df[slot_product_col].value_counts().head(rec_mix_top_n).index.tolist()
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
            pm_ts["value"] = pm_ts["pct"] if rec_mix_metric == "Share of Calls" else pm_ts["n"]
            pm_ts["period_display"] = period_display(pm_ts["period"], _chart_granularity)

            fig_pm = go.Figure()
            for prod in products_to_plot:
                sub = pm_ts[pm_ts[slot_product_col] == prod]
                if sub.empty:
                    continue
                fig_pm.add_trace(go.Scatter(
                    x=sub["period_display"],
                    y=sub["value"],
                    name=prod,
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
            apply_chart_theme(
                fig_pm,
                **PAIR_CHART_LAYOUT,
                legend=dict(**PAIR_LEGEND_BELOW),
                yaxis_title=rec_mix_metric,
                **metric_axis_kwargs(rec_mix_metric),
            )
            st.plotly_chart(fig_pm, use_container_width=True)
            pm_pivot = (
                pm_ts.pivot_table(index=slot_product_col, columns="period_display", values="value", aggfunc="sum")
                .fillna(0)
                .round(1)
                .reset_index()
                .rename(columns={slot_product_col: "Product"})
            )
            pm_disp = pm_pivot.copy()
            for _col in pm_disp.columns[1:]:
                pm_disp[_col] = pm_disp[_col].apply(lambda v: format_chart_value(v, rec_mix_metric))
            render_table_expander(
                "Data table",
                pm_disp,
                "product_level_rec_mix_pivot.csv",
                key_suffix="model_pm_mix",
            )
        else:
            st.info("No data available for the selected slot / product combination.")
    elif rec_mix_view == "Product":
        st.info("Product recommendation column not found. Expected one of: recommended_in_order, recommended_matched_in_order.")

    st.divider()
    st.subheader(
        "Recommendation Mix Comparison",
        help=(
            "Compare category share between Pre and Post windows. Date pickers span the full raw call_date range; "
            "other sidebar filters still apply. % change is relative to the pre-period share."
        ),
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
            (c for c in ("recommended_in_order", "recommended_matched_in_order") if c in df_mcmp.columns),
            None,
        )
        mix_mode_opts = ["Plan type (#1 recommendation)"]
        if prod_col_m is not None:
            mix_mode_opts.append("Product (ranked recommendation slot)")
        mix_cmp_mode = st.radio(
            "Mix to compare",
            mix_mode_opts,
            horizontal=True,
            key="model_mix_cmp_mode",
            help="Compare mix by top plan type or, when available, by product in a ranked recommendation slot.",
        )
        slot_idx_m = 0
        if mix_cmp_mode.startswith("Product"):
            slot_cmp = st.selectbox(
                "Recommendation slot",
                ["Diamond", "Gold"],
                index=0,
                key="model_mix_cmp_slot",
                help="Diamond is recommendation slot 1; Gold is recommendation slot 2.",
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
                help="Baseline window for the comparison.",
            )
        with mo_c2:
            mo_post = st.date_input(
                "Post period",
                value=_mo_post_default,
                min_value=mod_min,
                max_value=mod_max,
                key="model_mix_cmp_post_range",
                help="Current or test window for the comparison.",
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
    st.subheader(
        "Model Confidence & Raw Conversion Probabilities",
        help=(
            "Raw conversion probabilities feed expected-points scores and ranked recommendations. "
            "Use the view selector to inspect one diagnostic at a time."
        ),
    )

    prob_cols_needed = {
        "call_date",
        "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled",
        "expected_points_gap_1_2", "first_pitch_type", "order_count",
        "gcv_on_first_pitch", "gcv",
    }

    if prob_cols_needed.issubset(df.columns):
        _wdf_prob = df_nodatefilter
        _wk_prob = lambda fn: wtd_vs_four_week_pooled(_wdf_prob, fn)

        ra_cols = st.columns(4)
        for i, (pt, col) in enumerate([("Fixed", "raw_prob_fixed"), ("Tiered", "raw_prob_tiered"),
                                        ("Bundled", "raw_prob_bundled")]):
            if col in _wdf_prob.columns:
                cur_p, pool_p = _wk_prob(lambda d, c=col: d[c].mean() * 100 if c in d.columns else float("nan"))
                ra_cols[i].metric(
                    f"Avg P(convert) - {pt}",
                    fmt_metric_val_pct(cur_p),
                    delta=wk_pct_delta_vs_avg(cur_p, pool_p),
                    help="Mean raw conversion probability - WTD vs P4WA.",
                )
        cur_gap, pool_gap = _wk_prob(
            lambda d: d["expected_points_gap_1_2"].mean() if "expected_points_gap_1_2" in d.columns else float("nan")
        )
        _gap_val = fmt_metric_val_float(cur_gap, 2)
        ra_cols[3].metric(
            "Avg Confidence Gap",
            f"{_gap_val} pts" if _gap_val != "—" else "—",
            delta=wk_pct_delta_vs_avg(cur_gap, pool_gap),
            help="Mean expected-points gap #1 vs #2 - WTD vs P4WA.",
        )

        conf_c1, conf_c2 = st.columns([1.4, 1])
        with conf_c1:
            conf_view = st.selectbox(
                "View",
                ["Raw Probabilities", "Confidence Gap Distribution", "Tier Mix by Gap", "Outcome by Gap"],
                key="confidence_view",
                help="Choose one confidence diagnostic to show.",
            )
        with conf_c2:
            conf_metric_choice = st.selectbox(
                "Outcome metric",
                ["1st Pitch CR", "Overall CR", "GCV / Call"],
                key="conf_gap_metric",
                help="Used when the selected view is Outcome by Gap.",
            )

        if conf_view == "Raw Probabilities":
            fig_violin = go.Figure()
            for pt, col in [("Fixed", "raw_prob_fixed"), ("Tiered", "raw_prob_tiered"), ("Bundled", "raw_prob_bundled")]:
                fig_violin.add_trace(go.Violin(
                    y=df[col].dropna(),
                    name=pt,
                    box_visible=True,
                    meanline_visible=True,
                    points=False,
                ))
            apply_chart_theme(
                fig_violin,
                **PAIR_CHART_LAYOUT,
                yaxis_title="Raw Conversion Probability",
                yaxis_tickformat=".0%",
                legend=dict(**PAIR_LEGEND_BELOW),
            )
            st.plotly_chart(fig_violin, use_container_width=True)

        elif conf_view == "Confidence Gap Distribution":
            gap_vals = df["expected_points_gap_1_2"].dropna()
            if gap_vals.empty:
                st.info("No confidence gap values in view.")
            else:
                p25, p75 = gap_vals.quantile(0.25), gap_vals.quantile(0.75)
                pct_low  = (gap_vals < p25).mean() * 100
                pct_high = (gap_vals > p75).mean() * 100
                fig_hist = go.Figure()
                _hist_stroke, _ = chart_hist_stroke_and_title()
                fig_hist.add_trace(go.Histogram(
                    x=gap_vals,
                    nbinsx=40,
                    marker_color=PLOT_COLORWAY[0],
                    marker_line_width=1,
                    marker_line_color=_hist_stroke,
                    opacity=0.8,
                ))
                fig_hist.add_vline(x=float(p25), line_dash="dash", line_color=chart_hline_reference(),
                                   annotation_text=f"25th ({p25:.2f})", annotation_position="top right",
                                   annotation_font_color=chart_muted())
                fig_hist.add_vline(x=float(p75), line_dash="dash", line_color=chart_hline_reference(),
                                   annotation_text=f"75th ({p75:.2f})", annotation_position="top left",
                                   annotation_font_color=chart_muted())
                apply_chart_theme(
                    fig_hist,
                    **PAIR_CHART_LAYOUT,
                    xaxis_title="Expected Points Gap (#1 vs #2)",
                    yaxis_title="Calls",
                    showlegend=False,
                )
                st.plotly_chart(fig_hist, use_container_width=True)
                st.caption(
                    f"**{pct_low:.0f}%** low-confidence (gap < {p25:.2f} pts) · "
                    f"**{pct_high:.0f}%** high-confidence (gap > {p75:.2f} pts)"
                )

        else:
            df_gap = df.dropna(subset=["expected_points_gap_1_2", "first_pitch_type"]).copy()
            unique_gaps = df_gap["expected_points_gap_1_2"].nunique(dropna=True)
            if df_gap.empty or unique_gaps < 2:
                st.info("Not enough confidence gap variation for quintile analysis.")
            else:
                q = min(5, unique_gaps)
                bucket_codes = pd.qcut(df_gap["expected_points_gap_1_2"], q=q, labels=False, duplicates="drop")
                df_gap["gap_bucket"] = bucket_codes.apply(lambda x: f"Q{int(x) + 1}" if pd.notna(x) else pd.NA)
                df_gap = df_gap.dropna(subset=["gap_bucket"])

                if conf_view == "Tier Mix by Gap":
                    mix = (
                        df_gap[df_gap["first_pitch_type"].isin(SALE_TIER_ORDER)]
                        .groupby(["gap_bucket", "first_pitch_type"], observed=True)
                        .size()
                        .reset_index(name="n")
                    )
                    totals = mix.groupby("gap_bucket")["n"].transform("sum")
                    mix["value"] = mix["n"] / totals.replace(0, pd.NA) * 100
                    fig_mix_gap = go.Figure()
                    for tier in SALE_TIER_ORDER:
                        sub = mix[mix["first_pitch_type"] == tier]
                        if sub.empty:
                            continue
                        fig_mix_gap.add_trace(go.Bar(
                            x=sub["gap_bucket"],
                            y=sub["value"],
                            name=tier,
                            text=sub["value"].round(1).astype(str) + "%",
                            textposition="outside",
                            textfont=bar_outside_textfont(),
                        ))
                    apply_chart_theme(
                        fig_mix_gap,
                        **PAIR_CHART_LAYOUT,
                        barmode="group",
                        xaxis_title="Confidence Gap Quintile",
                        yaxis_title="Tier Mix",
                        yaxis_ticksuffix="%",
                        legend=dict(**PAIR_LEGEND_BELOW),
                    )
                    st.plotly_chart(fig_mix_gap, use_container_width=True)

                else:
                    col_map2 = {"1st Pitch CR": "gcv_on_first_pitch", "Overall CR": "order_count", "GCV / Call": "gcv"}
                    is_dollar = conf_metric_choice == "GCV / Call"
                    gap_out = (
                        df_gap[df_gap["first_pitch_type"].isin(SALE_TIER_ORDER)]
                        .groupby(["gap_bucket", "first_pitch_type"], observed=True)
                        .agg(val=(col_map2[conf_metric_choice],
                                  lambda x: x.mean() if conf_metric_choice == "GCV / Call" else (x > 0).mean() * 100),
                             calls=("gcv", "count"))
                        .reset_index()
                    )
                    if gap_out.empty:
                        st.info("No tiered pitch calls in view for this outcome chart.")
                    else:
                        fig_out = go.Figure()
                        for tier in SALE_TIER_ORDER:
                            sub = gap_out[gap_out["first_pitch_type"] == tier]
                            if sub.empty:
                                continue
                            fig_out.add_trace(go.Scatter(
                                x=sub["gap_bucket"],
                                y=sub["val"],
                                name=tier,
                                mode="lines+markers",
                                line=dict(width=2),
                                marker=dict(size=5),
                            ))
                        apply_chart_theme(
                            fig_out,
                            **PAIR_CHART_LAYOUT,
                            xaxis_title="Confidence Gap Quintile",
                            yaxis_title=conf_metric_choice,
                            yaxis_tickprefix="$" if is_dollar else "",
                            yaxis_ticksuffix="" if is_dollar else "%",
                            legend=dict(**PAIR_LEGEND_BELOW),
                        )
                        st.plotly_chart(fig_out, use_container_width=True)

    else:
        missing = prob_cols_needed - set(df.columns)
        st.info(f"Columns missing for this section: {', '.join(sorted(missing))}")

    st.divider()

    # ── Section 3: Tier Outcome Snapshot ─────────────────────────────────────
    st.subheader(
        "Pitch Tier Outcomes",
        help="Compare conversion and value outcomes by the Diamond, Gold, Silver, and Bronze first-pitch tiers.",
    )

    needed = {"call_date", "first_pitch_type", "gcv_on_first_pitch", "order_count", "gcv"}

    if needed.issubset(df.columns):
        tier_metric = st.selectbox(
            "Metric",
            PERFORMANCE_METRICS,
            key="model_tier_outcome_metric",
            help="Outcome metric to compare across first-pitched tiers.",
        )

        _wdf_ts = df_nodatefilter
        _wk_ts = lambda fn: wtd_vs_four_week_pooled(_wdf_ts, fn)

        def _tier_metric(d, tier, metric):
            sub = d[d["first_pitch_type"] == tier]
            if sub.empty:
                return float("nan")
            return calc_performance_metric(sub, metric)

        ca_cols = st.columns(4)
        for i, tier in enumerate(SALE_TIER_ORDER):
            cur_v, pool_v = _wk_ts(lambda d, t=tier: _tier_metric(d, t, tier_metric))
            ca_cols[i].metric(
                tier,
                format_chart_value(cur_v, tier_metric),
                delta=wk_pct_delta_vs_avg(cur_v, pool_v),
                help=f"WTD {tier_metric} for {tier} first-pitched calls vs pooled P4WA.",
            )

        tier_out = pd.DataFrame(
            [
                {
                    "Tier": tier,
                    "Value": calc_performance_metric(df[df["first_pitch_type"] == tier], tier_metric),
                    "Calls": len(df[df["first_pitch_type"] == tier]),
                }
                for tier in SALE_TIER_ORDER
            ]
        )

        fig_tier = go.Figure(go.Bar(
            x=tier_out["Tier"],
            y=tier_out["Value"],
            text=[format_chart_value(v, tier_metric) for v in tier_out["Value"]],
            textposition="outside",
            textfont=bar_outside_textfont(),
            marker_color=PLOT_COLORWAY[:len(tier_out)],
            marker_line_width=0,
            customdata=tier_out[["Calls"]],
            hovertemplate="Calls: %{customdata[0]:,}<extra></extra>",
        ))
        apply_chart_theme(
            fig_tier,
            **PAIR_CHART_LAYOUT,
            yaxis_title=tier_metric,
            showlegend=False,
            **metric_axis_kwargs(tier_metric),
        )
        st.plotly_chart(fig_tier, use_container_width=True)

        if "top_recommended_plan_type" in df.columns:
            plan_cmp = (
                df[df["first_pitch_type"].isin(SALE_TIER_ORDER)]
                .groupby(["top_recommended_plan_type", "first_pitch_type"])
                .agg(
                    calls=("first_pitch_type", "count"),
                    fp_cr=("gcv_on_first_pitch", lambda x: (x > 0).mean()),
                    overall_cr=("order_count", lambda x: (x > 0).mean()),
                    gcv_call=("gcv", "mean"),
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
                "first_pitch_type": "Tier",
                "calls": "Calls",
                "fp_cr": "1st Pitch CR",
                "overall_cr": "Overall CR",
                "gcv_call": "GCV / Call",
                "gcv_fp_ev": "GCV / 1st Pitch",
            })
            render_table_expander(
                "Conversion by plan type",
                plan_cmp,
                "conversion_by_plan_type_and_tier.csv",
                key_suffix="model_plan_tier_cmp",
                height_rows=min(len(plan_cmp), 40),
            )
    else:
        st.info("One or more required columns are missing for this section.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — AGENT BEHAVIOR & PERFORMANCE
# ════════════════════════════════════════════════════════════════════════════════
with tab_agent:

    agent_adherence_type = st.radio(
        "Tier source",
        ["First Pitch", "Sale"],
        index=0,
        horizontal=True,
        key="agent_adherence_type",
        help="First Pitch uses the initial pitched tier. Sale uses the sold product tier among converting calls.",
    )

    df_agent, _, agent_eff_mode = prepare_agent_behavior_dataframe(df, agent_adherence_type)
    df_nodate_agent, _, _ = prepare_agent_behavior_dataframe(df_nodatefilter, agent_adherence_type)

    if agent_adherence_type == "Sale" and agent_eff_mode != "Sale":
        st.warning(
            "Sale tier needs `sale_type` and `order_count` on the call-level file. Showing first-pitch tiers instead."
        )

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

    st.subheader(
        "Tier Trend",
        help="Trend tier mix or any conversion/value metric by tier, center, quartile, or overall.",
    )

    tr_c1, tr_c2 = st.columns(2)
    with tr_c1:
        agent_trend_metric = st.selectbox(
            "Metric",
            ["Tier Mix", "Calls"] + PERFORMANCE_METRICS,
            key="agent_trend_metric",
            help="Tier Mix shows each tier's share of calls per period. Other metrics are calculated within each group.",
        )
    with tr_c2:
        group_options = ["Tier", "Overall", "Center", "Agent Quartile"]
        agent_group_choice = st.selectbox(
            "Group By",
            group_options,
            key="agent_trend_group",
            help="Controls the line categories for performance metrics. Tier Mix always groups by tier.",
        )

    if "call_date" in df_agent.columns:
        trend_df = df_agent.dropna(subset=["call_date"]).copy()
        if agent_eff_mode == "Sale" and "order_count" in trend_df.columns:
            trend_df = trend_df[trend_df["order_count"].fillna(0) > 0]
        trend_df["period"] = period_labels(trend_df["call_date"], _chart_granularity)

        group_map = {
            "Tier": "agent_tier_display",
            "Overall": None,
            "Center": "center_location",
            "Agent Quartile": "performance_quartile",
        }
        group_col = "agent_tier_display" if agent_trend_metric == "Tier Mix" else group_map[agent_group_choice]

        if agent_trend_metric == "Tier Mix":
            mix_df = trend_df.dropna(subset=["agent_tier_display"])
            trend_ts = (
                mix_df.groupby(["period", "agent_tier_display"])
                .size()
                .reset_index(name="n")
                .sort_values("period")
            )
            totals = trend_ts.groupby("period")["n"].transform("sum")
            trend_ts["value"] = trend_ts["n"] / totals.replace(0, pd.NA) * 100
            trend_ts["period_display"] = period_display(trend_ts["period"], _chart_granularity)
            line_values = SALE_TIER_ORDER
            y_metric = "Tier Mix"
        elif group_col and group_col in trend_df.columns:
            trend_ts = (
                trend_df.dropna(subset=[group_col])
                .groupby(["period", group_col])
                .apply(lambda g: calc_performance_metric(g, agent_trend_metric))
                .reset_index(name="value")
                .sort_values("period")
            )
            trend_ts["period_display"] = period_display(trend_ts["period"], _chart_granularity)
            line_values = SALE_TIER_ORDER if group_col == "agent_tier_display" else sorted(trend_ts[group_col].dropna().unique().tolist())
            y_metric = agent_trend_metric
        else:
            trend_ts = (
                trend_df.groupby("period")
                .apply(lambda g: calc_performance_metric(g, agent_trend_metric))
                .reset_index(name="value")
                .sort_values("period")
            )
            trend_ts["period_display"] = period_display(trend_ts["period"], _chart_granularity)
            trend_ts["_overall"] = "Overall"
            group_col = "_overall"
            line_values = ["Overall"]
            y_metric = agent_trend_metric

        if not trend_ts.empty:
            fig_agent_trend = go.Figure()
            for line_val in line_values:
                sub = trend_ts[trend_ts[group_col] == line_val]
                if sub.empty:
                    continue
                fig_agent_trend.add_trace(go.Scatter(
                    x=sub["period_display"],
                    y=sub["value"],
                    name=str(line_val),
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
            apply_chart_theme(
                fig_agent_trend,
                **PAIR_CHART_LAYOUT,
                yaxis_title=y_metric,
                legend=dict(**PAIR_LEGEND_BELOW),
                **metric_axis_kwargs(y_metric),
            )
            st.plotly_chart(fig_agent_trend, use_container_width=True)
        else:
            st.info("No data available for the selected trend.")
    else:
        st.info("call_date column missing.")

    st.divider()

    # ── Period-over-period comparison table ───────────────────────────────────
    st.subheader(
        "Period-over-Period Comparison",
        help=(
            "Compare Pre and Post date ranges. Defaults use latest full Mon-Sun week for Post and four prior "
            "Mon-Sun weeks for Pre. Delta cells are green for improvement and red for decline."
        ),
    )

    _pop_core = {
        "call_date", "top_recommended_plan_type",
        "gcv_on_first_pitch", "order_count", "gcv",
    }
    _pop_fp = {"first_pitch_type"}
    _pop_sale = {"sale_type", "order_count"}
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
                help="Baseline comparison window.",
            )
        with tc2:
            post_range = st.date_input(
                "Post period",
                value=(_post_def_start, _post_def_end),
                min_value=raw_min,
                max_value=raw_max,
                key="cmp_post_range",
                help="Current or test comparison window.",
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

                rows = []
                grp = (
                    source.dropna(subset=["top_recommended_plan_type"])
                    .groupby("top_recommended_plan_type")
                )
                for rtype, g in grp:
                    n_rec = len(g)
                    for tier in SALE_TIER_ORDER:
                        sub = g[g["agent_tier_display"] == tier] if "agent_tier_display" in g.columns else g.iloc[0:0]
                        n_sub    = len(sub)
                        mix      = n_sub / n_rec * 100 if n_rec > 0 else float("nan")
                        fp_cr    = (sub["gcv_on_first_pitch"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        ov_cr    = (sub["order_count"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        # FIX: GCV / 1st Pitch EV = mean over all calls in subset (zeros included)
                        gcv_fp   = sub["gcv_on_first_pitch"].mean() if n_sub > 0 else float("nan")
                        gcv_call = sub["gcv"].mean() if n_sub > 0 else float("nan")
                        rows.append({
                            "rec_type": rtype,
                            "tier":     tier,
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
                    on=["rec_type", "tier"],
                    suffixes=("_pre", "_post"),
                )

                METRICS = [
                    ("mix",      "Mix",                    "pct",    False),
                    ("fp_cr",    "First Pitch CR",          "pct",    False),
                    ("ov_cr",    "Overall CR",              "pct",    False),
                    ("gcv_fp",   "GCV / First Pitch",  "dollar", True),
                    ("gcv_call", "GCV / Call",              "dollar", True),
                ]

                TIER_ORDER = SALE_TIER_ORDER
                rec_types = sorted(merged["rec_type"].unique())

                display_rows = []
                row_idx = 0
                for rt in rec_types:
                    for tier in TIER_ORDER:
                        match = merged[(merged["rec_type"] == rt) & (merged["tier"] == tier)]
                        if match.empty:
                            continue
                        r = match.iloc[0]
                        row = {
                            "Rec Type":  rt if tier == TIER_ORDER[0] else "",
                            "Tier":      tier,
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

                col_order = ["Rec Type", "Tier"]
                for _, label, _, _ in METRICS:
                    col_order += [f"{label} {pre_label}", f"{label} {post_label}", f"{label} Delta"]

                styler = styler.set_properties(**{"text-align": "right"}, subset=col_order[2:])
                styler = styler.set_properties(**{"text-align": "left"},  subset=["Rec Type", "Tier"])

                st.dataframe(
                    styler,
                    use_container_width=True,
                    hide_index=True,
                    column_order=col_order,
                    height=dataframe_display_height(len(display_df)),
                )
                table_export_row(display_df, "agent_period_rec_type_tier.csv", key_suffix="agent_pop_rec")

                st.subheader(
                    "Overall Comparison",
                    help="Same period comparison collapsed across recommendation plan types.",
                )

                def compute_overall_metrics(source):
                    if len(source) == 0:
                        return pd.DataFrame()
                    total_calls = len(source)
                    rows = []
                    for tier in SALE_TIER_ORDER:
                        sub = source[source["agent_tier_display"] == tier] if "agent_tier_display" in source.columns else source.iloc[0:0]
                        n_sub = len(sub)
                        mix      = n_sub / total_calls * 100 if total_calls > 0 else float("nan")
                        fp_cr    = (sub["gcv_on_first_pitch"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        ov_cr    = (sub["order_count"] > 0).mean() * 100 if n_sub > 0 else float("nan")
                        # FIX: GCV / 1st Pitch EV = mean over all calls (zeros included)
                        gcv_fp   = sub["gcv_on_first_pitch"].mean() if n_sub > 0 else float("nan")
                        gcv_call = sub["gcv"].mean() if n_sub > 0 else float("nan")
                        rows.append({
                            "tier":     tier,
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
                    merged_ov = pre_overall.merge(post_overall, on="tier", suffixes=("_pre", "_post"))

                    ov_rows = []
                    for _, r in merged_ov.iterrows():
                        row = {"Tier": r["tier"]}
                        for col, label, fmt, hib in METRICS:
                            pre_v  = r[f"{col}_pre"]
                            post_v = r[f"{col}_post"]
                            row[f"{label} {pre_label}"]  = fmt_val(pre_v,  fmt)
                            row[f"{label} {post_label}"] = fmt_val(post_v, fmt)
                            row[f"{label} Delta"]        = fmt_delta(pre_v, post_v, fmt)
                        ov_rows.append(row)

                    ov_df = pd.DataFrame(ov_rows)
                    ov_styler = ov_df.style.map(color_delta_cell, subset=delta_cols)

                    ov_col_order = ["Tier"]
                    for _, label, _, _ in METRICS:
                        ov_col_order += [f"{label} {pre_label}", f"{label} {post_label}", f"{label} Delta"]

                    ov_styler = ov_styler.set_properties(**{"text-align": "right"}, subset=ov_col_order[1:])
                    ov_styler = ov_styler.set_properties(**{"text-align": "left"},  subset=["Tier"])

                    st.dataframe(
                        ov_styler,
                        use_container_width=True,
                        hide_index=True,
                        column_order=ov_col_order,
                        height=dataframe_display_height(len(ov_df)),
                    )
                    table_export_row(ov_df, "agent_period_overall_tier.csv", key_suffix="agent_pop_ov")

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
            st.markdown("**Key Metrics**", help="Overall KPI comparison for the selected Pre and Post windows.")
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
        st.subheader(
            "Confusion Matrix",
            help=(
                "Converting calls only. Rows show plan type from the sold pitch; columns show top recommended plan type."
            ),
        )
        cm_needed = {
            "sale_type", "top_recommended_plan_type", "recommended_plan_types_in_order",
            "order_count", "gcv", 
        }
        _cm_use_sale = True
    else:
        st.subheader(
            "Confusion Matrix",
            help=(
                "Rows show the rec-slot plan type pitched first; columns show top recommended plan type. "
                "Other means first pitch was outside all rec slots."
            ),
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
    st.subheader(
        "Sale Mixes",
        help=(
            "Among converting calls only. Choose the sale dimension and metric from one chart surface. "
            "The chart respects sidebar and date filters; comparison uses sidebar filters plus its own date windows."
        ),
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
            sel = frozenset(s for s in selected if s is not None and str(s) != "")
            if not sel:
                return ser
            if include_other:
                return ser.where(ser.isin(sel), other)
            return ser.where(ser.isin(sel), pd.NA)

        def _sm_metric_by_group(source: pd.DataFrame, group_col: str, metric: str) -> pd.Series:
            if source.empty:
                return pd.Series(dtype=float)
            if metric == "Share of Sales":
                return source[group_col].value_counts(normalize=True, dropna=True).mul(100)
            if metric == "Sales Count":
                return source[group_col].value_counts(dropna=True).astype(float)
            grouped = source.dropna(subset=[group_col]).groupby(group_col)
            if metric == "GCV / Sale":
                return grouped["gcv"].mean() if "gcv" in source.columns else pd.Series(dtype=float)
            if metric == "RPO":
                if "gcv" not in source.columns or "order_count" not in source.columns:
                    return pd.Series(dtype=float)
                sums = grouped[["gcv", "order_count"]].sum(numeric_only=True)
                return sums["gcv"] / sums["order_count"].replace(0, pd.NA)
            return pd.Series(dtype=float)

        sm_sales = df[df["order_count"].fillna(0) > 0].dropna(subset=["call_date"]).copy()
        sm_sales["partner"] = _sm_display_str(sm_sales["sold_partner_name"])
        sm_sales["plan"] = _sm_display_str(sm_sales["sold_plan_name"])

        if sm_sales.empty:
            st.info("No converting calls in the current filters and date range.")
        else:
            sm_sales["period"] = period_labels(sm_sales["call_date"], _chart_granularity)
            sm_sales["period_display"] = period_display(sm_sales["period"], _chart_granularity)

            sm_c1, sm_c2, sm_c3, sm_c4 = st.columns([1, 1, 1, 1])
            with sm_c1:
                sale_mix_dimension = st.selectbox(
                    "Dimension",
                    ["Provider", "Sold Plan"],
                    key="sale_mix_dimension",
                    help="Choose whether the chart groups converting sales by partner or by sold plan.",
                )
            with sm_c2:
                sale_mix_metric = st.selectbox(
                    "Metric",
                    ["Share of Sales", "Sales Count", "GCV / Sale", "RPO"],
                    key="sale_mix_metric",
                    help="Metric to trend for the selected sale dimension.",
                )
            with sm_c3:
                sale_mix_top_n = st.slider(
                    "Top categories",
                    3,
                    20,
                    5,
                    key="sale_mix_top_n",
                    help="Default category selection when the selection list resets.",
                )
            with sm_c4:
                include_other = st.checkbox(
                    "Include Other",
                    value=True,
                    key="sale_mix_include_other",
                    help="Bucket unselected categories into Other.",
                )

            dim_key = "partner" if sale_mix_dimension == "Provider" else "plan"
            axis_lbl = "Partner" if sale_mix_dimension == "Provider" else "Sold plan"
            vc = sm_sales[dim_key].value_counts()
            sel_key = f"sale_mix_selected_{dim_key}"
            sig_key = f"sale_mix_sig_{dim_key}"
            dim_sig = (len(sm_sales), tuple(vc.head(80).items()), sale_mix_top_n)
            if st.session_state.get(sig_key) != dim_sig:
                st.session_state[sel_key] = list(vc.head(sale_mix_top_n).index)
                st.session_state[sig_key] = dim_sig

            selected_categories = st.multiselect(
                f"{axis_lbl}s in mix",
                options=list(vc.index),
                key=sel_key,
                help="Search to add or remove categories. Unselected categories roll into Other when enabled.",
            )
            selected_categories = list(selected_categories) if selected_categories else list(vc.head(sale_mix_top_n).index)

            group_col = "sale_mix_group"
            sm_sales[group_col] = _sm_bucket_selected_other(
                sm_sales[dim_key], selected_categories, include_other=include_other
            )
            sm_plot = sm_sales if include_other else sm_sales.dropna(subset=[group_col])

            if sale_mix_metric in ("Share of Sales", "Sales Count"):
                ts = (
                    sm_plot.groupby(["period", "period_display", group_col], observed=True)
                    .size()
                    .reset_index(name="n")
                )
                if sale_mix_metric == "Share of Sales":
                    totals = ts.groupby("period")["n"].transform("sum")
                    ts["value"] = ts["n"] / totals.replace(0, pd.NA) * 100
                else:
                    ts["value"] = ts["n"]
            else:
                ts = (
                    sm_plot.groupby(["period", "period_display", group_col], observed=True)
                    .agg(gcv_sum=("gcv", "sum"), gcv_mean=("gcv", "mean"), order_sum=("order_count", "sum"), n=("order_count", "size"))
                    .reset_index()
                )
                if sale_mix_metric == "GCV / Sale":
                    ts["value"] = ts["gcv_mean"]
                else:
                    ts["value"] = ts["gcv_sum"] / ts["order_sum"].replace(0, pd.NA)

            cats_sorted = sorted(ts[group_col].dropna().unique().tolist(), key=lambda x: (x == "Other", str(x)))
            fig_sale = go.Figure()
            for cat in cats_sorted:
                sub = ts[ts[group_col] == cat].sort_values("period")
                if sub.empty:
                    continue
                name = str(cat)
                fig_sale.add_trace(go.Scatter(
                    x=sub["period_display"],
                    y=sub["value"],
                    name=name[:44] + ("..." if len(name) > 44 else ""),
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
            apply_chart_theme(
                fig_sale,
                **PAIR_CHART_LAYOUT,
                yaxis_title=sale_mix_metric,
                legend=dict(**PAIR_LEGEND_BELOW),
                **metric_axis_kwargs(sale_mix_metric),
            )
            st.plotly_chart(fig_sale, use_container_width=True)

            piv = (
                ts.pivot_table(index=group_col, columns="period_display", values="value", aggfunc="sum")
                .fillna(0)
                .round(1)
                .reset_index()
                .rename(columns={group_col: axis_lbl})
            )
            piv_disp = piv.copy()
            for col in piv_disp.columns[1:]:
                piv_disp[col] = piv_disp[col].apply(lambda v: format_chart_value(v, sale_mix_metric))
            render_table_expander(
                "Data table",
                piv_disp,
                f"sale_mix_{dim_key}_by_period.csv",
                key_suffix=f"sale_mix_{dim_key}_period",
                height_rows=min(len(piv_disp), 40),
            )

            st.markdown("**Compare periods**")
            sm_sales_cmp = (
                df_nodatefilter[df_nodatefilter["order_count"].fillna(0) > 0]
                .dropna(subset=["call_date"])
                .copy()
            )
            sm_sales_cmp["partner"] = _sm_display_str(sm_sales_cmp["sold_partner_name"])
            sm_sales_cmp["plan"] = _sm_display_str(sm_sales_cmp["sold_plan_name"])
            sm_sales_cmp[group_col] = _sm_bucket_selected_other(
                sm_sales_cmp[dim_key], selected_categories, include_other=include_other
            )
            sm_sales_cmp = sm_sales_cmp if include_other else sm_sales_cmp.dropna(subset=[group_col])

            sm_data_min = pd.to_datetime(df_raw["call_date"].min()).date()
            sm_data_max = pd.to_datetime(df_raw["call_date"].max()).date()
            _sm_cmp_sig = (sm_data_min, sm_data_max, len(sm_sales_cmp), sale_mix_dimension, sale_mix_metric)
            if st.session_state.get("sale_mix_cmp_date_sig") != _sm_cmp_sig:
                st.session_state.pop("sale_mix_cmp_pre_range", None)
                st.session_state.pop("sale_mix_cmp_post_range", None)
                st.session_state["sale_mix_cmp_date_sig"] = _sm_cmp_sig

            _sm_pre_default, _sm_post_default = streamlit_safe_period_defaults(sm_data_max, sm_data_min)
            sm_po1, sm_po2 = st.columns(2)
            with sm_po1:
                sm_pre_range = st.date_input(
                    "Pre period",
                    value=_sm_pre_default,
                    min_value=sm_data_min,
                    max_value=sm_data_max,
                    key="sale_mix_cmp_pre_range",
                    help="Baseline comparison window.",
                )
            with sm_po2:
                sm_post_range = st.date_input(
                    "Post period",
                    value=_sm_post_default,
                    min_value=sm_data_min,
                    max_value=sm_data_max,
                    key="sale_mix_cmp_post_range",
                    help="Current or test comparison window.",
                )

            def _sm_slice_sm_sales(d0: date, d1: date):
                lo, hi = sorted((d0, d1))
                m = (sm_sales_cmp["call_date"].dt.date >= lo) & (sm_sales_cmp["call_date"].dt.date <= hi)
                return sm_sales_cmp.loc[m]

            def _sm_color_pct_chg(val):
                try:
                    x = float(str(val).replace("%", "").replace("+", ""))
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

                pre_vals = _sm_metric_by_group(pre_s, group_col, sale_mix_metric)
                post_vals = _sm_metric_by_group(post_s, group_col, sale_mix_metric)
                idx_x = pre_vals.index.union(post_vals.index)
                t_cmp = pd.DataFrame(
                    {
                        f"{sale_mix_metric} ({pre_lab})": pre_vals.reindex(idx_x).fillna(0).round(1),
                        f"{sale_mix_metric} ({post_lab})": post_vals.reindex(idx_x).fillna(0).round(1),
                    }
                )
                _cp0, _cp1 = t_cmp.columns[0], t_cmp.columns[1]
                _pv0 = t_cmp[_cp0].astype(float)
                _pv1 = t_cmp[_cp1].astype(float)
                t_cmp["% change vs pre"] = ((_pv1 / _pv0.replace(0, float("nan")) - 1.0).mul(100).round(1))
                t_cmp = t_cmp.sort_values(_cp1, ascending=False).rename_axis(axis_lbl).reset_index()

                t_disp = t_cmp.copy()
                t_disp[_cp0] = t_disp[_cp0].apply(lambda v: format_chart_value(v, sale_mix_metric))
                t_disp[_cp1] = t_disp[_cp1].apply(lambda v: format_chart_value(v, sale_mix_metric))
                sty_cmp = t_disp.style.map(_sm_color_pct_chg, subset=["% change vs pre"])
                sty_cmp = sty_cmp.set_properties(**{"text-align": "right"}, subset=[_cp0, _cp1, "% change vs pre"])
                sty_cmp = sty_cmp.set_properties(**{"text-align": "left"}, subset=[axis_lbl])
                st.dataframe(
                    sty_cmp,
                    use_container_width=True,
                    hide_index=True,
                    height=dataframe_display_height(min(len(t_disp), 40)),
                )
                table_export_row(t_disp, f"sale_mix_{dim_key}_period_compare.csv", key_suffix=f"sale_mix_{dim_key}_cmp")
            else:
                st.caption("Select full pre and post date ranges to populate the comparison table.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 4 — AGENT LEVEL
# ════════════════════════════════════════════════════════════════════════════════
with tab_agent_level:
    st.subheader(
        "Agent-Level Performance",
        help=(
            "One row per agent. Tier rates show share of calls by first-pitched tier. Conversion and GCV metrics "
            "are per-call; GCV / 1st Pitch is an expected value over all calls. Sidebar filters apply."
        ),
    )

    agent_needed = {
        "agent_name", "first_pitch_type",
        "gcv_on_first_pitch", "order_count", "gcv", "points",
    }

    if agent_needed.issubset(df.columns):

        al_c1, al_c2, al_c3 = st.columns([2, 2, 1])
        with al_c1:
            agent_search = st.text_input(
                "Search Agent Name",
                key="agent_search",
                placeholder="Type to filter…",
                help="Filter the agent table and charts by name.",
            )
        with al_c2:
            sort_col = st.selectbox(
                "Sort by",
                ["Calls", "Diamond %", "Gold %", "Silver %", "Bronze %",
                 "1st Pitch CR", "Overall CR", "GCV / Call", "GCV / 1st Pitch", "Points / Call"],
                key="agent_sort_col",
                help="Metric used to order the agent table.",
            )
        with al_c3:
            sort_asc = st.radio(
                "Order",
                ["Desc", "Asc"],
                horizontal=True,
                key="agent_sort_order",
                help="Sort direction for the selected metric.",
            ) == "Asc"

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
            center = g["center_location"].mode().iloc[0] if "center_location" in g.columns and not g["center_location"].mode().empty else "—"
            quartile = g["performance_quartile"].mode().iloc[0] if "performance_quartile" in g.columns and not g["performance_quartile"].mode().empty else "—"

            return pd.Series({
                "Center":              center,
                "Agent Quartile":      quartile,
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

        dist_c1, dist_c2 = st.columns(2)
        with dist_c1:
            dist_metric = st.selectbox(
                "Distribution metric",
                ["Diamond %", "Gold %", "Silver %", "Bronze %",
                 "1st Pitch CR", "Overall CR", "GCV / Call", "GCV / 1st Pitch", "Points / Call"],
                key="agent_distribution_metric",
                index=0,
                help="Metric to visualize across agents.",
            )
        with dist_c2:
            dist_group = st.selectbox(
                "Group By",
                ["None", "Center", "Agent Quartile"],
                key="agent_distribution_group",
                help="Use grouping to compare distributions across centers or quartiles.",
            )

        fig_dist = go.Figure()
        _dist_stroke = histogram_marker_line()
        if dist_group == "None":
            fig_dist.add_trace(go.Histogram(
                x=agent_df[dist_metric].dropna(),
                nbinsx=20,
                marker_color=PLOT_COLORWAY[0],
                opacity=0.85,
                marker_line_color=_dist_stroke,
                marker_line_width=1,
            ))
            x_title = dist_metric
            y_title = "Agents"
            showlegend = False
        else:
            group_col = "Center" if dist_group == "Center" else "Agent Quartile"
            for i, group_val in enumerate(sorted(agent_df[group_col].dropna().unique().tolist())):
                sub = agent_df[agent_df[group_col] == group_val]
                fig_dist.add_trace(go.Box(
                    x=[str(group_val)] * len(sub),
                    y=sub[dist_metric],
                    name=str(group_val),
                    marker_color=PLOT_COLORWAY[i % len(PLOT_COLORWAY)],
                    boxmean=True,
                ))
            x_title = dist_group
            y_title = dist_metric
            showlegend = False
        apply_chart_theme(
            fig_dist,
            **PAIR_CHART_LAYOUT,
            xaxis_title=x_title,
            yaxis_title=y_title,
            showlegend=showlegend,
            **metric_axis_kwargs(dist_metric),
        )
        st.plotly_chart(fig_dist, use_container_width=True)

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
                "For visuals, return a Plotly Figure directly or under `result['figure']`; "
                "do not save, link, or embed PNG/image files. "
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

    def _result_has_plotly_figure(result) -> bool:
        import plotly.graph_objects as _go

        if isinstance(result, _go.Figure):
            return True
        if isinstance(result, dict):
            return any(isinstance(v, _go.Figure) for v in result.values())
        return False

    def _step_has_plotly_figure(step: dict) -> bool:
        if step.get("kind") != "result":
            return False
        return _result_has_plotly_figure(step.get("content", step.get("result")))

    def _clean_ai_answer_markdown(text: str) -> str:
        import re as _re

        return _re.sub(r"!\[[^\]]*\]\([^)]*\)", "", str(text)).strip()

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
                figure_keys = set()
                figure_items = [(k, v) for k, v in r.items() if isinstance(v, _go.Figure)]
                for k, fig in figure_items:
                    figure_keys.add(k)
                    if k != "figure" or len(figure_items) > 1:
                        st.markdown(f"**{k}**")
                    st.plotly_chart(fig, use_container_width=True)
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
                remaining = {k: v for k, v in r.items() if k not in figure_keys and k != "summary"}
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
            st.markdown(_clean_ai_answer_markdown(step.get("content", "")))

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

            final_visual_result = None
            if final_answer is not None:
                final_result_positions = [
                    j
                    for j, (intermediate, _) in enumerate(intermediate_steps)
                    if intermediate.get("kind") == "result"
                ]
                for j in reversed(final_result_positions):
                    intermediate = intermediate_steps[j][0]
                    if intermediate.get("is_final") and _step_has_plotly_figure(intermediate):
                        final_visual_result = intermediate_steps.pop(j)
                        break
                if final_visual_result is None and final_result_positions:
                    j = final_result_positions[-1]
                    if _step_has_plotly_figure(intermediate_steps[j][0]):
                        final_visual_result = intermediate_steps.pop(j)

            if intermediate_steps:
                with st.expander(f"Analysis steps ({len(intermediate_steps)})", expanded=False):
                    for j, (intermediate, step_i) in enumerate(intermediate_steps):
                        render_step_body(intermediate, export_key_suffix=f"mid_{step_i}_{j}")
                        if j < len(intermediate_steps) - 1:
                            st.divider()

            if final_answer is not None:
                render_step(final_answer, export_key_suffix=f"fin_{steps.index(final_answer)}")

            if final_visual_result is not None:
                visual_step, visual_step_i = final_visual_result
                render_step_body(visual_step, export_key_suffix=f"visual_{visual_step_i}")

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
        .st-key-ai_chat_shell {
            max-width: 920px;
            margin: 0 auto;
            min-height: 58vh;
            padding: 0.5rem 0 1.5rem;
        }
        .ai-empty-state {
            min-height: 54vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            text-align: center;
            max-width: 720px;
            margin: 0 auto;
            padding: 2rem 1rem;
        }
        .ai-empty-state h1 {
            color: var(--text-primary) !important;
            font-size: clamp(2.25rem, 4vw, 3.4rem);
            font-weight: 700;
            margin: 0 0 0.75rem;
            letter-spacing: 0 !important;
        }
        .ai-empty-state p {
            max-width: 560px;
            margin: 0;
            color: var(--text-secondary) !important;
            font-size: 0.98rem;
            line-height: 1.6;
            font-weight: 400 !important;
            letter-spacing: 0 !important;
        }
        .ai-user-row {
            width: 100%;
            display: flex;
            justify-content: flex-end;
            margin: 0.75rem 0 1.25rem;
        }
        .ai-user-bubble {
            max-width: min(74%, 720px);
            border-radius: 1.15rem;
            padding: 0.8rem 1rem;
            background: color-mix(in srgb, var(--accent) 14%, transparent);
            border: 1px solid color-mix(in srgb, var(--accent) 24%, transparent);
            line-height: 1.5;
            overflow-wrap: anywhere;
        }
        .st-key-ai_chat_shell [data-testid="stMarkdownContainer"] p,
        .st-key-ai_chat_shell [data-testid="stMarkdownContainer"] li {
            line-height: 1.65;
        }
        .st-key-ai_chat_shell [data-testid="stExpander"] {
            border-color: var(--border) !important;
            background: transparent !important;
        }
        .st-key-ai_analyst_clear {
            max-width: 920px;
            margin: 0.25rem auto 0;
            display: flex;
            justify-content: flex-end;
        }
        .st-key-ai_analyst_clear button {
            min-height: auto !important;
            border-radius: 999px !important;
            padding: 0.4rem 0.8rem !important;
            background: transparent !important;
            border: 1px solid var(--border) !important;
            color: var(--text-muted) !important;
            box-shadow: none !important;
            font-size: 0.78rem !important;
            font-weight: 500 !important;
            letter-spacing: 0 !important;
            text-transform: none !important;
        }
        .st-key-ai_analyst_clear button:hover:enabled {
            background: var(--bg-hover) !important;
            border-color: var(--border-bright) !important;
            color: var(--text-secondary) !important;
            transform: none !important;
            box-shadow: none !important;
        }
        div[data-testid="stChatInput"] {
            max-width: 920px;
            margin: 0.75rem auto 0;
        }
        .ai-analyst-footer-note {
            max-width: 760px;
            margin: 0.7rem auto 0;
            padding: 0 1rem 0.9rem;
            color: var(--text-muted) !important;
            font-size: 0.78rem;
            line-height: 1.35;
            text-align: center;
            opacity: 0.72;
        }
        @media (max-width: 760px) {
            .st-key-ai_chat_shell {
                min-height: 52vh;
            }
            .ai-analyst-footer-note {
                font-size: 0.72rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    has_chat = len(st.session_state.agent_steps) > 0
    pending_user_input = st.session_state.ai_analyst_pending_user_input
    pending_request = pending_user_input

    with st.container(key="ai_chat_shell"):
        if not has_chat and not pending_request:
            st.markdown(
                """
                <div class="ai-empty-state">
                    <h1>AI Analyst</h1>
                    <p>Ask about model outputs, agent behavior, sales quality, or any field in the sidebar schema.</p>
                </div>
                """,
                unsafe_allow_html=True,
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
                            result_step = {
                                "kind": "result",
                                "step_num": step_num,
                                "content": result,
                                "is_final": is_final,
                            }
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
                                    "\nDo not include Markdown image links or PNG placeholders; the app renders "
                                    "Plotly figures directly from the result object."
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

    if has_chat:
        if st.button("Clear chat", key="ai_analyst_clear"):
            st.session_state.agent_steps = []
            st.session_state.agent_messages = []
            st.session_state.ai_analyst_pending_example = None
            st.session_state.ai_analyst_pending_user_input = None
            st.session_state.ai_analyst_limit_warning = False
            st.rerun()

    typed_user_input = st.chat_input("What do you want to know?", key="ai_analyst_input")
    if typed_user_input:
        st.session_state.ai_analyst_pending_user_input = typed_user_input
        st.rerun()

    st.markdown(
        """
        <div class="ai-analyst-footer-note">
            Tip: Use field names from the sidebar schema when possible, and describe the analysis you want in detail to reduce misinterpretation.
        </div>
        """,
        unsafe_allow_html=True,
    )
