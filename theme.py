"""App-wide CSS and sidebar light/dark theme toggle for Product Rank Dash."""

from __future__ import annotations

import functools
import json
import math

import streamlit as st

# Persisted choice: first option is default (Dark) on first visit.
THEME_RADIO_KEY = "product_rec_app_theme"
# Boolean widget state; kept in sync with THEME_RADIO_KEY for is_light_theme / URL restore.
THEME_TOGGLE_KEY = "product_rec_theme_light_toggle"
# Survives full-page reload when syncing Streamlit host theme (session_state resets on reload).
THEME_QUERY_KEY = "product_rec_theme"
_THEME_URL_SIG_KEY = "_product_rec_theme_url_sig"


def restore_theme_from_query_params() -> None:
    """After host-theme sync, the browser opens ``?product_rec_theme=light|dark`` so we can restore
    the sidebar theme control — a hard reload clears widget state back to default Dark.

    We only apply when the query value **changes** so a lingering ``?product_rec_theme=light`` does not
    override the user after they switch back to Dark."""
    qp = st.query_params
    if THEME_QUERY_KEY not in qp:
        st.session_state.pop(_THEME_URL_SIG_KEY, None)
        return
    raw = qp.get(THEME_QUERY_KEY)
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    s = str(raw).strip().lower()
    if s not in ("light", "l", "dark", "d"):
        return
    sig = f"{THEME_QUERY_KEY}={s}"
    if st.session_state.get(_THEME_URL_SIG_KEY) == sig:
        return
    if s in ("light", "l"):
        st.session_state[THEME_RADIO_KEY] = "Light"
        st.session_state[THEME_TOGGLE_KEY] = True
    else:
        st.session_state[THEME_RADIO_KEY] = "Dark"
        st.session_state[THEME_TOGGLE_KEY] = False
    st.session_state[_THEME_URL_SIG_KEY] = sig


def init_browser_query_state() -> None:
    """Call at app startup: restore theme from URL (after host reload)."""
    restore_theme_from_query_params()


def is_light_theme() -> bool:
    """True when the user selected Light (toggle on or legacy THEME_RADIO_KEY)."""
    if st.session_state.get(THEME_TOGGLE_KEY) is not None:
        return bool(st.session_state.get(THEME_TOGGLE_KEY))
    return st.session_state.get(THEME_RADIO_KEY, "Dark") == "Light"


def period_comparison_delta_style(pct_num: float, neutral_abs: float = 3.0) -> str:
    """CSS for period-over-period % change cells (positive = improvement)."""
    if pct_num is None:
        return ""
    try:
        p = float(pct_num)
    except (TypeError, ValueError):
        return ""
    if math.isnan(p) or math.isinf(p):
        return ""
    light = is_light_theme()
    if abs(p) < neutral_abs:
        if light:
            return "background-color: #fef9c3; color: #854d0e"
        return "background-color: #2a2a1a; color: #c8a000"
    if p > 0:
        if light:
            return "background-color: #dcfce7; color: #166534"
        return "background-color: #0f2a1a; color: #22c55e"
    if light:
        return "background-color: #ffe4e6; color: #be123c"
    return "background-color: #2a1018; color: #f43f5e"


def render_app_theme_toggle() -> str:
    """Sidebar: Light mode toggle. Syncs THEME_RADIO_KEY for charts/CSS. Returns 'Light' or 'Dark'."""
    if THEME_TOGGLE_KEY not in st.session_state:
        st.session_state[THEME_TOGGLE_KEY] = st.session_state.get(THEME_RADIO_KEY, "Dark") == "Light"
    light = st.toggle(
        "Light mode",
        key=THEME_TOGGLE_KEY,
        help="On: light theme. Off: dark theme for page and charts.",
    )
    st.session_state[THEME_RADIO_KEY] = "Light" if light else "Dark"
    return st.session_state[THEME_RADIO_KEY]


def _root_variables(light: bool) -> str:
    if light:
        return """
:root {
    --bg-base:       #ffffff;
    --bg-card:       #f8fafc;
    --bg-card-alt:   #f1f5f9;
    --bg-hover:      #e2e8f0;
    --border:        #e2e8f0;
    --border-bright: #cbd5e1;
    --accent:        #3d8ef8;
    --accent-dim:    #2563c4;
    --accent-glow:   rgba(61, 142, 248, 0.18);
    --teal:          #22d3c8;
    --amber:         #f5a623;
    --rose:          #f43f5e;
    --green:         #22c55e;
    --text-primary:  #0f172a;
    --text-secondary:#475569;
    --text-muted:    #64748b;
    --radius:        8px;
    --radius-lg:     12px;
    --header-bg:     rgba(255, 255, 255, 0.97);
    --header-border: #e2e8f0;
    --scrollbar-track: #f1f5f9;

    /* Streamlit widget / shell tokens (many components read these) */
    --st-color-text: #0f172a;
    --st-color-body-text: #0f172a;
    --st-color-heading: #0f172a;
    --st-color-background: #ffffff;
    --st-color-secondary-background: #f8fafc;
    --st-color-surface: #ffffff;
    --st-color-border: #e2e8f0;
    --st-color-input-background: #ffffff;
    --st-color-widget-background: #ffffff;
    --st-color-widget-border: #cbd5e1;
    --st-color-text-input: #0f172a;
    --st-color-menu-background: #ffffff;
    --st-color-menu-text: #0f172a;

    /* Glide data grid (st.dataframe) — see glide-data-grid theme / --gdg-* */
    --gdg-bg-cell: #ffffff;
    --gdg-bg-cell-medium: #f8fafc;
    --gdg-bg-header: #f1f5f9;
    --gdg-bg-header-hovered: #e2e8f0;
    --gdg-bg-header-has: #dbeafe;
    --gdg-text-dark: #0f172a;
    --gdg-text-medium: #334155;
    --gdg-text-light: #64748b;
    --gdg-text-header: #0f172a;
    --gdg-border-color: #e2e8f0;
    --gdg-horizontal-border-color: #e2e8f0;
    --gdg-accent-color: #3d8ef8;
    --gdg-accent-fg: #ffffff;
    --gdg-accent-light: rgba(61, 142, 248, 0.22);
    --gdg-font-family: "DM Sans", sans-serif;
    --gdg-base-font-style: 13px "DM Sans", sans-serif;
    --gdg-header-font-style: 600 12px "DM Sans", sans-serif;
}
"""
    return """
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
    --header-bg:     rgba(13, 15, 20, 0.92);
    --header-border: #252b3a;
    --scrollbar-track: #0d0f14;

    --st-color-text: #e8ecf4;
    --st-color-body-text: #e8ecf4;
    --st-color-heading: #e8ecf4;
    --st-color-background: #0d0f14;
    --st-color-secondary-background: #13161d;
    --st-color-surface: #181c25;
    --st-color-border: #252b3a;
    --st-color-input-background: #181c25;
    --st-color-widget-background: #181c25;
    --st-color-widget-border: #2e3649;
    --st-color-text-input: #e8ecf4;
    --st-color-menu-background: #181c25;
    --st-color-menu-text: #e8ecf4;

    --gdg-bg-cell: #13161d;
    --gdg-bg-cell-medium: #181c25;
    --gdg-bg-header: #181c25;
    --gdg-bg-header-hovered: #1e2330;
    --gdg-bg-header-has: rgba(61, 142, 248, 0.18);
    --gdg-text-dark: #e8ecf4;
    --gdg-text-medium: #8b95aa;
    --gdg-text-light: #64748b;
    --gdg-text-header: #8b95aa;
    --gdg-border-color: #252b3a;
    --gdg-horizontal-border-color: #252b3a;
    --gdg-accent-color: #3d8ef8;
    --gdg-accent-fg: #ffffff;
    --gdg-accent-light: rgba(61, 142, 248, 0.22);
    --gdg-font-family: "DM Sans", sans-serif;
    --gdg-base-font-style: 13px "DM Sans", sans-serif;
    --gdg-header-font-style: 600 12px "DM Sans", sans-serif;
}
"""


def _shared_stylesheet_uncached(light: bool) -> str:
    st_app_bg_img = (
        ""
        if light
        else """
    background-image:
        radial-gradient(ellipse 80% 40% at 50% -10%, rgba(61,142,248,0.06) 0%, transparent 58%),
        radial-gradient(ellipse 40% 30% at 90% 80%, rgba(34,211,200,0.03) 0%, transparent 50%);
"""
    )
    metric_hover_shadow = (
        "0 0 0 1px var(--border-bright), 0 4px 16px rgba(15, 23, 42, 0.08) !important;"
        if light
        else "0 0 0 1px var(--border-bright), 0 4px 20px rgba(0,0,0,0.4) !important;"
    )
    return f"""
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=DM+Mono:wght@300;400;500&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,300&display=swap');

{_root_variables(light)}

html, body, [class*="css"], .stApp, .stMarkdown, p, span, div, label {{
    font-family: 'DM Sans', sans-serif !important;
    color: var(--text-primary) !important;
}}

.stApp {{
    background-color: var(--bg-base) !important;
    color: var(--text-primary) !important;
{st_app_bg_img}
}}

[data-testid="stHeader"] {{
    background-color: var(--header-bg) !important;
    border-bottom: 1px solid var(--header-border) !important;
}}
[data-testid="stToolbar"] {{
    background-color: var(--bg-base) !important;
}}

.main .block-container {{
    padding: 2.75rem 2.5rem 4rem !important;
    max-width: 1600px !important;
}}

[data-testid="stSidebar"] {{
    background-color: var(--bg-card) !important;
    border-right: 1px solid var(--border) !important;
}}
[data-testid="stSidebar"] .stTitle > * {{
    font-family: 'Syne', sans-serif !important;
    font-size: 1.1rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: var(--accent) !important;
}}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stDateInput label,
[data-testid="stSidebar"] .stToggle label {{
    font-size: 0.7rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
}}

h1, h2, h3, h4 {{
    font-family: 'Syne', sans-serif !important;
    color: var(--text-primary) !important;
}}
h1 {{ font-size: 1.8rem !important; font-weight: 800 !important; letter-spacing: -0.01em !important; }}
h2 {{ font-size: 1.25rem !important; font-weight: 700 !important; letter-spacing: 0.01em !important; }}
h3 {{ font-size: 1rem !important; font-weight: 600 !important; }}

[data-testid="stHeading"] h1 {{
    color: var(--text-primary) !important;
    font-size: 2rem !important;
    font-weight: 800 !important;
    letter-spacing: -0.02em !important;
    padding-bottom: 0.1em;
    margin-top: 0.75rem !important;
}}

.stCaptionContainer, [data-testid="stCaptionContainer"], small, caption {{
    color: var(--text-secondary) !important;
    font-size: 0.78rem !important;
    line-height: 1.5 !important;
}}

[data-testid="stMetric"] {{
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-lg) !important;
    padding: 1rem 1.25rem !important;
    transition: border-color 0.2s, box-shadow 0.2s !important;
    position: relative;
    overflow: hidden;
}}
[data-testid="stMetric"]::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--teal));
    opacity: 0;
    transition: opacity 0.2s;
}}
[data-testid="stMetric"]:hover {{ border-color: var(--border-bright) !important; box-shadow: {metric_hover_shadow} }}
[data-testid="stMetric"]:hover::before {{ opacity: 1; }}
[data-testid="stMetricLabel"] {{ font-size: 0.68rem !important; font-weight: 500 !important; letter-spacing: 0.1em !important; text-transform: uppercase !important; color: var(--text-secondary) !important; font-family: 'DM Sans', sans-serif !important; }}
[data-testid="stMetricValue"] {{ font-family: 'DM Mono', monospace !important; font-size: 1.5rem !important; font-weight: 500 !important; color: var(--text-primary) !important; line-height: 1.2 !important; }}
[data-testid="stMetricDelta"] {{ font-family: 'DM Mono', monospace !important; font-size: 0.75rem !important; }}
[data-testid="stMetricDelta"] svg {{ display: none !important; }}

[data-testid="stTabs"] [role="tablist"] {{ border-bottom: 1px solid var(--border) !important; gap: 0 !important; background: transparent !important; }}
[data-testid="stTabs"] [role="tab"] {{ font-family: 'Syne', sans-serif !important; font-size: 0.78rem !important; font-weight: 600 !important; letter-spacing: 0.07em !important; text-transform: uppercase !important; color: var(--text-muted) !important; padding: 0.6rem 1.25rem !important; border: none !important; border-bottom: 2px solid transparent !important; background: transparent !important; transition: color 0.15s, border-color 0.15s !important; }}
[data-testid="stTabs"] [role="tab"]:hover {{ color: var(--text-secondary) !important; border-bottom-color: var(--border-bright) !important; }}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {{ color: var(--accent) !important; border-bottom-color: var(--accent) !important; background: transparent !important; }}

hr {{ border: none !important; border-top: 1px solid var(--border) !important; margin: 2rem 0 !important; }}

.stSelectbox > div > div,
.stMultiSelect > div > div,
.stTextInput > div > div > input,
.stDateInput > div > div > input {{
    background-color: var(--bg-card-alt) !important;
    border: 1px solid var(--border-bright) !important;
    border-radius: var(--radius) !important;
    color: var(--text-primary) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.85rem !important;
    transition: border-color 0.15s !important;
}}
.stSelectbox > div > div:focus-within,
.stMultiSelect > div > div:focus-within {{ border-color: var(--accent) !important; box-shadow: 0 0 0 2px var(--accent-glow) !important; outline: none !important; }}

[data-baseweb="menu"] {{ background-color: var(--bg-card-alt) !important; border: 1px solid var(--border-bright) !important; border-radius: var(--radius) !important; }}
[data-baseweb="menu"] li {{ font-family: 'DM Sans', sans-serif !important; font-size: 0.85rem !important; color: var(--text-primary) !important; }}
[data-baseweb="menu"] li:hover {{ background-color: var(--bg-hover) !important; }}
[data-baseweb="tag"] {{ background-color: var(--accent-dim) !important; border: none !important; border-radius: 4px !important; font-size: 0.75rem !important; }}

.stRadio > div {{ gap: 0.5rem !important; background: transparent !important; border: none !important; padding: 0 !important; display: inline-flex !important; }}
.stRadio label {{ font-size: 0.75rem !important; font-weight: 500 !important; letter-spacing: 0.05em !important; text-transform: uppercase !important; padding: 0.3rem 0.85rem !important; border-radius: 6px !important; cursor: pointer !important; color: var(--text-secondary) !important; background: transparent !important; transition: color 0.15s !important; }}

[data-testid="stDataFrame"], .stDataFrame {{ border: 1px solid var(--border) !important; border-radius: var(--radius-lg) !important; overflow: hidden !important; }}
[data-testid="stDataFrame"] thead th {{ background: var(--bg-card-alt) !important; font-family: 'DM Sans', sans-serif !important; font-size: 0.68rem !important; font-weight: 600 !important; letter-spacing: 0.1em !important; text-transform: uppercase !important; color: var(--text-secondary) !important; border-bottom: 1px solid var(--border-bright) !important; padding: 0.6rem 0.8rem !important; }}
[data-testid="stDataFrame"] thead th:not(:first-child),
[data-testid="stDataFrame"] tbody td:not(:first-child) {{ text-align: right !important; }}
[data-testid="stDataFrame"] thead th:first-child,
[data-testid="stDataFrame"] tbody td:first-child {{ text-align: left !important; }}
[data-testid="stDataFrame"] tbody td {{ font-family: 'DM Mono', monospace !important; font-size: 0.82rem !important; color: var(--text-primary) !important; border-bottom: 1px solid var(--border) !important; padding: 0.5rem 0.8rem !important; background: var(--bg-card) !important; }}
[data-testid="stDataFrame"] tbody tr:hover td {{ background: var(--bg-hover) !important; }}

.stButton > button {{ background: var(--accent) !important; color: white !important; border: none !important; border-radius: var(--radius) !important; font-family: 'DM Sans', sans-serif !important; font-size: 0.8rem !important; font-weight: 600 !important; letter-spacing: 0.05em !important; padding: 0.5rem 1.25rem !important; transition: all 0.15s !important; }}
.stButton > button:hover {{ background: var(--accent-dim) !important; box-shadow: 0 4px 12px rgba(61,142,248,0.3) !important; transform: translateY(-1px) !important; }}

[data-testid="stHeading"] h2, .stMarkdown h2 {{ color: var(--text-primary) !important; font-size: 1.1rem !important; font-weight: 700 !important; letter-spacing: 0.02em !important; padding-top: 0.25rem !important; padding-bottom: 0.5rem !important; border-bottom: 1px solid var(--border) !important; margin-bottom: 1rem !important; }}

[data-testid="stInfo"] {{ background: rgba(61,142,248,0.08) !important; border: 1px solid rgba(61,142,248,0.25) !important; border-radius: var(--radius) !important; color: var(--accent) !important; font-size: 0.85rem !important; }}
[data-testid="stWarning"] {{ background: rgba(245,166,35,0.08) !important; border: 1px solid rgba(245,166,35,0.25) !important; border-radius: var(--radius) !important; color: var(--amber) !important; }}

[data-testid="stCaptionContainer"] p {{ color: var(--text-muted) !important; font-size: 0.78rem !important; font-family: 'DM Mono', monospace !important; letter-spacing: 0.05em !important; }}
.stMarkdown strong {{ color: var(--text-primary) !important; font-weight: 600 !important; }}

::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: var(--scrollbar-track); }}
::-webkit-scrollbar-thumb {{ background: var(--border-bright); border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: var(--text-muted); }}
"""


_shared_stylesheet = functools.lru_cache(maxsize=2)(_shared_stylesheet_uncached)


@functools.lru_cache(maxsize=1)
def _light_streamlit_widget_overrides() -> str:
    """Base Web + Glide; menus portal under ``body`` — scope globals in light mode only."""
    return """
/* ── In-app selects / inputs (sidebar + main) ─────────────────────────────── */
[data-testid="stAppViewContainer"] [data-baseweb="select"] > div,
[data-testid="stAppViewContainer"] [data-baseweb="input"],
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="input"] {
  background-color: #ffffff !important;
  color: #0f172a !important;
  border-color: #cbd5e1 !important;
}
[data-testid="stAppViewContainer"] [data-baseweb="select"] span,
[data-testid="stAppViewContainer"] [data-baseweb="input"] input,
[data-testid="stAppViewContainer"] [data-baseweb="textarea"],
[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-baseweb="input"] input {
  color: #0f172a !important;
}
[data-testid="stAppViewContainer"] [data-baseweb="tag"],
[data-testid="stSidebar"] [data-baseweb="tag"] {
  background-color: #e2e8f0 !important;
  color: #0f172a !important;
}

/* ── Portaled dropdowns / calendars (render outside stAppViewContainer) ───── */
body [data-baseweb="popover"],
body [data-baseweb="popover"] > div {
  background-color: #ffffff !important;
  color: #0f172a !important;
  border-color: #e2e8f0 !important;
  box-shadow: 0 4px 16px rgba(15, 23, 42, 0.12) !important;
}
body [data-baseweb="layer"],
body [data-baseweb="layer"] > div {
  color: #0f172a !important;
}
body [data-baseweb="menu"],
body [data-baseweb="menu"] ul {
  background-color: #ffffff !important;
  color: #0f172a !important;
  border-color: #e2e8f0 !important;
}
body [data-baseweb="menu"] li,
body [data-baseweb="menu"] li > div,
body [data-baseweb="menu"] a,
body [data-baseweb="menu"] [role="option"],
body [data-baseweb="menu"] [role="menuitem"] {
  color: #0f172a !important;
  background-color: #ffffff !important;
}
body [data-baseweb="menu"] li:hover,
body [data-baseweb="menu"] [role="option"]:hover,
body [data-baseweb="menu"] [aria-selected="true"] {
  background-color: #f1f5f9 !important;
  color: #0f172a !important;
}
body ul[role="listbox"],
body ul[role="listbox"] li {
  background-color: #ffffff !important;
  color: #0f172a !important;
}
body ul[role="listbox"] li:hover,
body ul[role="listbox"] li[aria-selected="true"] {
  background-color: #e2e8f0 !important;
  color: #0f172a !important;
}

/* Date / time picker panels */
body [data-baseweb="calendar"],
body [data-baseweb="calendar"] button {
  background-color: #ffffff !important;
  color: #0f172a !important;
}

/* Popover inner chrome (Base Web stacks nested divs; text can inherit dark theme) */
body [data-baseweb="popover"] div,
body [data-baseweb="popover"] span,
body [data-baseweb="popover"] p,
body [data-baseweb="popover"] li {
  color: #0f172a !important;
}
body [data-baseweb="popover"] [tabindex="0"],
body [data-baseweb="popover"] [role="listbox"],
body [data-baseweb="popover"] [role="presentation"] {
  background-color: #ffffff !important;
  color: #0f172a !important;
}

/* Virtualized / div-based options (Streamlit 1.5x Base Web) */
body [data-baseweb="popover"] [role="option"],
body [data-baseweb="popover"] [role="option"] > div,
body [data-baseweb="popover"] [role="option"][aria-selected="true"],
body [data-baseweb="popover"] [role="option"][aria-selected="true"] > div {
  background-color: #ffffff !important;
  color: #0f172a !important;
}
body [data-baseweb="popover"] [role="option"]:hover,
body [data-baseweb="popover"] [role="option"]:hover > div {
  background-color: #f1f5f9 !important;
  color: #0f172a !important;
}

/* ── st.dataframe / Glide (canvas reads --gdg-* from nodes; beat inline with !important) ─ */
[data-testid="stDataFrame"],
[data-testid="StyledDataFrame"] {
  --gdg-bg-cell: #ffffff !important;
  --gdg-bg-cell-medium: #f8fafc !important;
  --gdg-bg-header: #f1f5f9 !important;
  --gdg-bg-header-hovered: #e2e8f0 !important;
  --gdg-bg-header-has: #dbeafe !important;
  --gdg-text-dark: #0f172a !important;
  --gdg-text-medium: #334155 !important;
  --gdg-text-light: #64748b !important;
  --gdg-text-header: #0f172a !important;
  --gdg-border-color: #e2e8f0 !important;
  --gdg-horizontal-border-color: #e2e8f0 !important;
  --gdg-accent-color: #3d8ef8 !important;
  --gdg-accent-fg: #ffffff !important;
  --gdg-accent-light: rgba(61, 142, 248, 0.22) !important;
  --gdg-font-family: "DM Sans", sans-serif !important;
  --gdg-base-font-style: 13px "DM Sans", sans-serif !important;
  --gdg-header-font-style: 600 12px "DM Sans", sans-serif !important;
}
[data-testid="stDataFrame"] *,
[data-testid="StyledDataFrame"] * {
  --gdg-bg-cell: #ffffff !important;
  --gdg-bg-cell-medium: #f8fafc !important;
  --gdg-bg-header: #f1f5f9 !important;
  --gdg-bg-header-hovered: #e2e8f0 !important;
  --gdg-bg-header-has: #dbeafe !important;
  --gdg-text-dark: #0f172a !important;
  --gdg-text-medium: #334155 !important;
  --gdg-text-light: #64748b !important;
  --gdg-text-header: #0f172a !important;
  --gdg-border-color: #e2e8f0 !important;
  --gdg-horizontal-border-color: #e2e8f0 !important;
  --gdg-accent-color: #3d8ef8 !important;
  --gdg-accent-fg: #ffffff !important;
  --gdg-accent-light: rgba(61, 142, 248, 0.22) !important;
  --gdg-font-family: "DM Sans", sans-serif !important;
  --gdg-base-font-style: 13px "DM Sans", sans-serif !important;
  --gdg-header-font-style: 600 12px "DM Sans", sans-serif !important;
}
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="StyledDataFrame"] [role="columnheader"],
[data-testid="StyledDataFrame"] [role="gridcell"] {
  color: #0f172a !important;
  background-color: #ffffff !important;
}
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="StyledDataFrame"] [role="columnheader"] {
  background-color: #f1f5f9 !important;
}

/* Download / secondary-style actions */
[data-testid="stAppViewContainer"] .stDownloadButton button {
  background-color: #f1f5f9 !important;
  color: #0f172a !important;
  border: 1px solid #cbd5e1 !important;
}
"""


@functools.lru_cache(maxsize=1)
def _dark_streamlit_widget_overrides() -> str:
    """Tune widgets to Arcadia dark tokens. ``config.toml`` uses ``base = "dark"`` so Glide/menus
    are already dark from Streamlit; these rules align grays/accent with the rest of the app."""
    return """
/* ── In-app selects / inputs (sidebar + main) ─────────────────────────────── */
[data-testid="stAppViewContainer"] [data-baseweb="select"] > div,
[data-testid="stAppViewContainer"] [data-baseweb="input"],
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="input"] {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
  border-color: #2e3649 !important;
}
[data-testid="stAppViewContainer"] [data-baseweb="select"] span,
[data-testid="stAppViewContainer"] [data-baseweb="input"] input,
[data-testid="stAppViewContainer"] [data-baseweb="textarea"],
[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-baseweb="input"] input {
  color: #e8ecf4 !important;
}
[data-testid="stAppViewContainer"] [data-baseweb="tag"],
[data-testid="stSidebar"] [data-baseweb="tag"] {
  background-color: #2e3649 !important;
  color: #e8ecf4 !important;
}

/* ── Portaled dropdowns / calendars ───────────────────────────────────────── */
body [data-baseweb="popover"],
body [data-baseweb="popover"] > div {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
  border-color: #2e3649 !important;
  box-shadow: 0 8px 28px rgba(0, 0, 0, 0.45) !important;
}
body [data-baseweb="layer"],
body [data-baseweb="layer"] > div {
  color: #e8ecf4 !important;
}
body [data-baseweb="menu"],
body [data-baseweb="menu"] ul {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
  border-color: #2e3649 !important;
}
body [data-baseweb="menu"] li,
body [data-baseweb="menu"] li > div,
body [data-baseweb="menu"] a,
body [data-baseweb="menu"] [role="option"],
body [data-baseweb="menu"] [role="menuitem"] {
  color: #e8ecf4 !important;
  background-color: #181c25 !important;
}
body [data-baseweb="menu"] li:hover,
body [data-baseweb="menu"] [role="option"]:hover,
body [data-baseweb="menu"] [aria-selected="true"] {
  background-color: #1e2330 !important;
  color: #e8ecf4 !important;
}
body ul[role="listbox"],
body ul[role="listbox"] li {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
}
body ul[role="listbox"] li:hover,
body ul[role="listbox"] li[aria-selected="true"] {
  background-color: #1e2330 !important;
  color: #e8ecf4 !important;
}

body [data-baseweb="calendar"],
body [data-baseweb="calendar"] button {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
}

body [data-baseweb="popover"] div,
body [data-baseweb="popover"] span,
body [data-baseweb="popover"] p,
body [data-baseweb="popover"] li {
  color: #e8ecf4 !important;
}
body [data-baseweb="popover"] [tabindex="0"],
body [data-baseweb="popover"] [role="listbox"],
body [data-baseweb="popover"] [role="presentation"] {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
}

/* ── st.dataframe / Glide (match Arcadia palette; !important vs any inline tokens) ─ */
[data-testid="stDataFrame"],
[data-testid="StyledDataFrame"] {
  --gdg-bg-cell: #13161d !important;
  --gdg-bg-cell-medium: #181c25 !important;
  --gdg-bg-header: #181c25 !important;
  --gdg-bg-header-hovered: #1e2330 !important;
  --gdg-bg-header-has: rgba(61, 142, 248, 0.18) !important;
  --gdg-text-dark: #e8ecf4 !important;
  --gdg-text-medium: #8b95aa !important;
  --gdg-text-light: #64748b !important;
  --gdg-text-header: #8b95aa !important;
  --gdg-border-color: #252b3a !important;
  --gdg-horizontal-border-color: #252b3a !important;
  --gdg-accent-color: #3d8ef8 !important;
  --gdg-accent-fg: #ffffff !important;
  --gdg-accent-light: rgba(61, 142, 248, 0.22) !important;
  --gdg-font-family: "DM Sans", sans-serif !important;
  --gdg-base-font-style: 13px "DM Sans", sans-serif !important;
  --gdg-header-font-style: 600 12px "DM Sans", sans-serif !important;
}
[data-testid="stDataFrame"] *,
[data-testid="StyledDataFrame"] * {
  --gdg-bg-cell: #13161d !important;
  --gdg-bg-cell-medium: #181c25 !important;
  --gdg-bg-header: #181c25 !important;
  --gdg-bg-header-hovered: #1e2330 !important;
  --gdg-bg-header-has: rgba(61, 142, 248, 0.18) !important;
  --gdg-text-dark: #e8ecf4 !important;
  --gdg-text-medium: #8b95aa !important;
  --gdg-text-light: #64748b !important;
  --gdg-text-header: #8b95aa !important;
  --gdg-border-color: #252b3a !important;
  --gdg-horizontal-border-color: #252b3a !important;
  --gdg-accent-color: #3d8ef8 !important;
  --gdg-accent-fg: #ffffff !important;
  --gdg-accent-light: rgba(61, 142, 248, 0.22) !important;
  --gdg-font-family: "DM Sans", sans-serif !important;
  --gdg-base-font-style: 13px "DM Sans", sans-serif !important;
  --gdg-header-font-style: 600 12px "DM Sans", sans-serif !important;
}
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="StyledDataFrame"] [role="columnheader"] {
  color: #8b95aa !important;
  background-color: #181c25 !important;
}

/* Download / secondary actions */
[data-testid="stAppViewContainer"] .stDownloadButton button {
  background-color: #181c25 !important;
  color: #e8ecf4 !important;
  border: 1px solid #2e3649 !important;
}
"""


def _sync_streamlit_browser_theme(light: bool) -> None:
    """Align Streamlit’s internal Light/Dark theme (Glide canvas, Base Web) with the sidebar.

    The host stores Settings → Theme under ``localStorage['stActiveTheme-' + pathname + '-v2']``
    (see Streamlit ``storageUtils.ts``). If it differs from the sidebar choice, set it and reload
    once so ``st.dataframe`` uses the correct palette from ``[theme.light]`` / ``[theme.dark]``.
    """
    import streamlit.components.v1 as components

    desired = "Light" if light else "Dark"
    components.html(
        f"""<script>
(function () {{
  var desired = {json.dumps(desired)};
  function storage() {{
    try {{ return window.parent.localStorage; }}
    catch (e) {{
      try {{ return window.top.localStorage; }}
      catch (e2) {{ return window.localStorage; }}
    }}
  }}
  function path() {{
    try {{ return window.parent.location.pathname; }}
    catch (e) {{
      try {{ return window.top.location.pathname; }}
      catch (e2) {{ return window.location.pathname; }}
    }}
  }}
  try {{
    var key = "stActiveTheme-" + path() + "-v2";
    var raw = storage().getItem(key);
    var cur = null;
    if (raw) {{
      try {{ cur = JSON.parse(raw); }} catch (e) {{}}
    }}
    if (cur === desired) return;
    storage().setItem(key, JSON.stringify(desired));
    var href = (function () {{
      try {{ return window.parent.location.href; }}
      catch (e) {{
        try {{ return window.top.location.href; }}
        catch (e2) {{ return window.location.href; }}
      }}
    }})();
    var u = new URL(href);
    u.searchParams.set({json.dumps(THEME_QUERY_KEY)}, desired === "Light" ? "light" : "dark");
    try {{ window.parent.location.href = u.toString(); }}
    catch (e) {{ window.top.location.href = u.toString(); }}
  }} catch (e) {{}}
}})();
</script>""",
        height=0,
        width=0,
    )


def inject_app_styles(light: bool | None = None) -> None:
    """Inject global CSS. Pass ``light`` from the theme radio return value for a guaranteed match."""
    if light is None:
        light = is_light_theme()
    css = _shared_stylesheet(light)
    if light:
        css += _light_streamlit_widget_overrides()
    else:
        css += _dark_streamlit_widget_overrides()
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    _sync_streamlit_browser_theme(light)
