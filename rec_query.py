import json
import re
import time
import glob
import os
from datetime import date, timedelta
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import Window
from openai import OpenAI
from tqdm import tqdm
from databricks.connect import DatabricksSession
from dotenv import load_dotenv

# =============================================================================
# INFRASTRUCTURE: StepTimer, _spark_collect, save_chunked_csv
# =============================================================================

class StepTimer:
    TOTAL_STEPS = 15

    def __init__(self, step_num: int, label: str):
        self.step_num = step_num
        self.label    = label
        self._start   = None

    def __enter__(self):
        self._start = time.time()
        print(f"\n[{self.step_num:>2}/{self.TOTAL_STEPS}] ▶  {self.label} ...", flush=True)
        return self

    def __exit__(self, *_):
        elapsed = time.time() - self._start
        print(f"[{self.step_num:>2}/{self.TOTAL_STEPS}] ✔  {self.label}  ({elapsed:.1f}s)", flush=True)


def _spark_collect(sdf, desc: str) -> pd.DataFrame:
    with tqdm(total=0, desc=desc, bar_format="{desc}: {elapsed}  [collecting from Spark...]", dynamic_ncols=True) as pbar:
        result = sdf.toPandas()
        pbar.set_postfix_str(f"done — {len(result):,} rows")
    return result


def save_chunked_csv(
    df: pd.DataFrame,
    base_dir: str,
    base_filename: str,
    max_bytes: int = 9 * 1024 * 1024,
) -> List[str]:
    out_dir = base_dir
    os.makedirs(out_dir, exist_ok=True)

    existing = glob.glob(os.path.join(out_dir, f"{base_filename}_*.csv"))
    for f in existing:
        os.remove(f)
    if existing:
        print(f"  Cleared {len(existing)} existing shard(s) from {out_dir}", flush=True)

    sample_size    = min(500, len(df))
    sample_csv     = df.iloc[:sample_size].to_csv(index=False)
    header_bytes   = len(sample_csv.encode("utf-8").split(b"\n", 1)[0]) + 1
    body_bytes     = len(sample_csv.encode("utf-8")) - header_bytes
    bytes_per_row  = body_bytes / sample_size if sample_size else 200
    rows_per_chunk = max(1, int((max_bytes - header_bytes) / bytes_per_row))

    total_rows   = len(df)
    n_chunks_est = max(1, -(-total_rows // rows_per_chunk))
    print(f"  ~{bytes_per_row:.0f} bytes/row → ~{rows_per_chunk:,} rows/chunk (~{n_chunks_est} file(s))", flush=True)

    written_files: List[str] = []
    file_index = 1

    def _write_chunk(chunk_df: pd.DataFrame) -> None:
        nonlocal file_index
        if chunk_df.empty:
            return
        file_path = os.path.join(out_dir, f"{base_filename}_{file_index}.csv")
        chunk_df.to_csv(file_path, index=False)
        actual_bytes = os.path.getsize(file_path)
        if actual_bytes > max_bytes and len(chunk_df) > 1:
            os.remove(file_path)
            mid = len(chunk_df) // 2
            _write_chunk(chunk_df.iloc[:mid])
            _write_chunk(chunk_df.iloc[mid:])
        else:
            tqdm.write(f"  ✔ {file_path}  ({actual_bytes/1024/1024:.2f} MB, {len(chunk_df):,} rows)")
            written_files.append(file_path)
            file_index += 1

    for start in tqdm(range(0, total_rows, rows_per_chunk), desc="Writing chunked CSV", unit="chunk", dynamic_ncols=True):
        _write_chunk(df.iloc[start : start + rows_per_chunk])

    print(f"\nTotal files written: {len(written_files)}")
    return written_files


# =============================================================================
# PITCH MATCH CACHE  (./data/pitch_match_cache.csv)
#
# Schema carries no_match_confidence alongside match_confidence.
# Unresolved entries are only cached when no_match_confidence >= NO_MATCH_CACHE_THRESHOLD
# so that phonetic variants and plan names not yet in v_orders are retried next run.
# =============================================================================

CACHE_PATH = os.path.join(".", "data", "pitch_match_cache.csv")
CACHE_COLS = ["product_pitched", "matched_plan_name", "match_confidence", "no_match_confidence"]

# Minimum match_confidence to accept — below this demoted to unspecified
MIN_MATCH_CONFIDENCE: float = 0.55

# Cache a no-match result only when the LLM was this confident nothing matches.
# Below threshold the entry is excluded from the cache and retries next run.
NO_MATCH_CACHE_THRESHOLD: float = 0.85

# Known-wrong masterlist plan_category values, corrected from validation.
# Applied in Spark after the masterlist join so classification logic uses the
# correct plan type rather than a stale masterlist value.
PLAN_CATEGORY_CORRECTIONS: Dict[str, str] = {
    "TXU Energy Simple Start 15":       "Bundled",
    "TXU Energy Simple Start 16":       "Bundled",
    "TriEagle Energy Silver Eagle 12":  "Bundled",
    "TriEagle Energy Bronze Eagle 24":  "Bundled",
    "TriEagle Energy Real Deal 12":     "Bundled",
    "TriEagle Energy Real Deal 24":     "Bundled",
    "TriEagle Energy Real Deal 36":     "Bundled",
    "TriEagle Energy Real Saver 12":    "Bundled",
    "TriEagle Energy Real Saver 24":    "Bundled",
    "TriEagle Energy Real Saver 36":    "Bundled",
    "TXU Energy Flex Rewards":          "Variable",
    "Payless Power 12 MONTH - PREPAID": "Fixed",
    "Payless Power 12 Month - prepaid": "Fixed",
    "Payless Power 6 MONTH - PREPAID":  "Fixed",
    "Payless Power 6 Month - prepaid":  "Fixed",
}

UNRESOLVED_PITCH_VALUES = ["UNMATCHED", "unspecified"]


def load_match_cache() -> pd.DataFrame:
    if os.path.exists(CACHE_PATH):
        cache = pd.read_csv(
            CACHE_PATH,
            dtype={
                "product_pitched":     str,
                "matched_plan_name":   str,
                "match_confidence":    float,
                "no_match_confidence": float,
            },
        )
        cache = cache.dropna(subset=["product_pitched"]).drop_duplicates(subset=["product_pitched"])
        if "no_match_confidence" not in cache.columns:
            cache["no_match_confidence"] = 0.0
        print(f"  [cache] Loaded {len(cache):,} cached matches from {CACHE_PATH}", flush=True)
        return cache
    print(f"  [cache] No cache found at {CACHE_PATH} — will match all pitches via LLM.", flush=True)
    return pd.DataFrame(columns=CACHE_COLS)


def save_match_cache(cache_df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    cache_df.to_csv(CACHE_PATH, index=False)
    print(f"  [cache] Saved {len(cache_df):,} entries to {CACHE_PATH}", flush=True)


def should_cache_result(matched: str, match_conf: float, no_match_conf: float) -> bool:
    if matched not in UNRESOLVED_PITCH_VALUES:
        return True
    return no_match_conf >= NO_MATCH_CACHE_THRESHOLD


# =============================================================================
# LLM PLAN MATCHING
# =============================================================================

def build_match_prompt(
    product_pitched: str,
    pitch_summary: Optional[str],
    plan_list: List[str],
) -> str:
    plans_formatted = "\n".join(f"- {p}" for p in plan_list)
    summary_block = (
        f"\nPitch Summary (additional context):\n{pitch_summary}\n"
        if pitch_summary else ""
    )
    return f"""You are an expert at matching raw energy plan names extracted from noisy sales call transcripts to a canonical plan list.

Raw extracted plan name: "{product_pitched}"{summary_block}

Canonical plan list:
{plans_formatted}

=== MATCHING RULES ===

MATCH aggressively. Transcripts contain mishearings, misspellings, OCR errors, missing term lengths, and garbled provider names.

Phonetic / spelling variants — always match:
  "Jexa Eco Saver Plus 24"        → "Gexa Energy Gexa Eco Saver Plus 24"   (J/G mishearing)
  "Jexa EcoSaver Plus 24"         → "Gexa Energy Gexa Eco Saver Plus 24"   (spacing + phonetic)
  "Jexa Eco Saver Premier 24"     → "Gexa Energy Gexa Eco Saver Plus 24"   (Premier≈Plus phonetic)
  "JAXA Energy Freedom 12"        → "Gexa Energy Gexa Freedom 12"           (J/G phonetic, full garble)
  "Simply Secure 12"              → "<Provider> Simple Secure 12"           (mishearing)
  "Tri Eagle Silver Eagle 12"     → "TriEagle Energy Silver Eagle 12"       (partial provider)
  "Ciro Energy Simple Advantage"  → "Cirro Simple Advantage 12"             (phonetic + missing term)
  "AP&PG and E Savior Plan"       → "APG&E SimpleSaver 12"                  (garbled + term missing)
  "Rhythm Saver 15 (Wisdom)"      → "Rhythm Energy Rhythm Saver 15"         (parenthetical noise)
  "Octopus Energy Octo Vault 12"  → "Octopus Energy Octo Volt 12"           (Vault/Volt phonetic)

Missing term lengths — still match on plan name + provider:
  "Express Energy Flash Value"    → "Express Energy Flash Value 12"

Provider name garbled — match on plan name portion even with garbled provider.

DO NOT match (return unspecified) when the raw name:
  - Is genuinely vague with NO identifiable plan name: "none", "no product pitched yet",
    "not yet pitched", "marketplace options", "unspecified plan", "something cheap",
    "no specific plan named yet", "pre-pitch phase"
  - Describes a rate or deposit requirement but names no specific plan:
    "7 Cent Fixed Rate Plan (Provider not specified)", "Zero or Low Deposit Plans"
  - Describes a category of plans, not a specific plan:
    "Saving Energy - Marketplace (general pitch, no specific plan named yet)"
  - Is a current customer plan being referenced, not a plan being pitched:
    "Green Mountain - Usage Requirement Plan (current plan, not pitched for sale)"

Key rule: if the input contains a real, identifiable plan name (even heavily garbled), MATCH it.

=== OUTPUT FORMAT ===

Respond ONLY with valid JSON — no explanation, no markdown:
{{
  "matched_plan": "<exact canonical plan name, or 'unspecified'>",
  "match_confidence": <0.0–1.0, certainty the match is correct>,
  "no_match_confidence": <0.0–1.0, certainty there is NO matchable plan name in the input>
}}

match_confidence: 1.0=exact, 0.8-0.9=strong, 0.6-0.7=plausible, 0.55-0.59=borderline, <0.55=use unspecified
no_match_confidence (only when unspecified): 1.0=clear placeholder, 0.8-0.9=vague, <0.85=uncertain"""


def match_plan_with_llm(
    product_pitched: str,
    pitch_summary: Optional[str],
    plan_list: List[str],
    client: OpenAI,
    max_retries: int = 3,
    timeout: float = 25.0,
) -> Tuple[str, float, float]:
    """Returns (matched_plan_name, match_confidence, no_match_confidence)."""
    if not product_pitched or not product_pitched.strip():
        return ("unspecified", 0.0, 1.0)

    prompt = build_match_prompt(product_pitched, pitch_summary, plan_list)

    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=80,
                temperature=0,
                timeout=timeout,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```json\s*|```$", "", raw, flags=re.MULTILINE).strip()
            parsed        = json.loads(raw)
            matched       = str(parsed.get("matched_plan", "")).strip()
            match_conf    = float(parsed.get("match_confidence", 0.0))
            no_match_conf = float(parsed.get("no_match_confidence", 0.0))

            if matched == "unspecified" or match_conf < MIN_MATCH_CONFIDENCE:
                return ("unspecified", 0.0, no_match_conf)

            if matched in plan_list:
                return (matched, match_conf, 0.0)

            matched_lower = matched.lower()
            for plan in plan_list:
                if plan.lower() == matched_lower:
                    return (plan, match_conf, 0.0)

            for plan in plan_list:
                if matched_lower in plan.lower() or plan.lower() in matched_lower:
                    return (plan, max(MIN_MATCH_CONFIDENCE, match_conf - 0.15), 0.0)

            return ("unspecified", 0.0, no_match_conf)

        except Exception as e:
            wait = 2 ** attempt
            if attempt < max_retries:
                print(f"  [attempt {attempt}/{max_retries}] LLM match failed for '{product_pitched}': {e} — retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [attempt {attempt}/{max_retries}] LLM match failed for '{product_pitched}': {e} — giving up.")
                return ("unspecified", 0.0, 0.0)

    return ("unspecified", 0.0, 0.0)


def build_pitch_match_lookup(
    pitches_pdf: pd.DataFrame,
    plan_list: List[str],
    api_key: str,
    max_workers: int = 5,
) -> Dict[str, Tuple[str, float, float]]:
    client = OpenAI(api_key=api_key)
    deduped = (
        pitches_pdf
        .dropna(subset=["product_pitched"])
        .drop_duplicates(subset=["product_pitched"])
        .reset_index(drop=True)
    )
    print(f"    Deduped {len(pitches_pdf):,} rows -> {len(deduped):,} unique product_pitched values.", flush=True)

    lookup: Dict[str, Tuple[str, float, float]] = {}

    def _match(row):
        pp = row["product_pitched"]
        ps = row.get("pitch_summary")
        m, mc, nmc = match_plan_with_llm(pp, ps, plan_list, client)
        return pp, m, mc, nmc

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_match, row): i for i, (_, row) in enumerate(deduped.iterrows())}
        print(f"    {len(futures):,} LLM calls submitted across {max_workers} workers...", flush=True)
        for future in tqdm(as_completed(futures), total=len(futures), desc="  LLM plan matching", unit="pitch"):
            pp, matched, mc, nmc = future.result()
            lookup[pp] = (matched, mc, nmc)

    return lookup


# =============================================================================
# MAIN DATA PIPELINE
# =============================================================================

def get_data(openai_api_key: str) -> pd.DataFrame:

    # -------------------------------------------------------------------------
    # Step 1 — Spark session
    # -------------------------------------------------------------------------
    with StepTimer(1, "Initialising Spark session"):
        spark = DatabricksSession.builder \
            .host("redventures-rv-energy-prod-production-9xwiei.cloud.databricks.com") \
            .serverless(True) \
            .getOrCreate()

    # -------------------------------------------------------------------------
    # CONSTANTS / CONFIG
    # -------------------------------------------------------------------------
    START_DATE = "2026-01-01"
    END_DATE   = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    RAW_MODEL_EVALUATED_TABLE        = "lakehouse_production.ai_products.raw_model_evaluated"
    PITCH_TABLE                      = "ai_products_prod.energy.pitch_extraction"
    ARCADIA_TABLE                    = "energy_prod.energy.rpt_arcadia_frontend"
    V_AGENT_CALLS_TABLE              = "energy_prod.energy.v_agent_calls"
    RPT_AGENT_CALLS_TABLE            = "lakehouse_production.energy.rpt_agent_calls"
    ORDER_POINTS_TABLE               = "lakehouse_production.energy.event_integration_orderpointssubmitted"
    ENERGY_QUALIFICATIONRESULT_TABLE = "lakehouse_production.energy.event_energy_qualificationresult"
    ELEMENT_VIEWED_TABLE             = "lakehouse_production.energy.event_arcadia_elementviewed"
    V_ORDERS_TABLE                   = "energy_prod.energy.v_orders"
    PLAN_MASTERLIST_TABLE            = "ai_products_prod.arcadia.energy_plan_masterlist"
    V_CALLS_TABLE                    = "energy_prod.energy.v_calls"

    PITCH_ORDER_CANDIDATES        = ["pitch_index", "pitch_order", "turn_index", "created_at", "event_ts", "index"]
    ELEMENT_DATE_CANDIDATES       = ["call_date", "event_date", "event_ts", "created_at", "_timeStamp", "timestamp"]
    QUAL_DATE_CANDIDATES          = ["call_date", "event_date", "event_ts", "created_at", "_timeStamp", "timestamp"]
    GCV_V2_COL_CANDIDATES         = ["gcv_v2"]
    ORDER_SUPPLIER_COL_CANDIDATES = ["supplier_name", "partner_name", "brand_name"]

    TARGET_CENTER_LOCATIONS = ["Durban", "Jamaica", "Charlotte"]
    FAILED_QUAL_PROVIDERS   = ["TXU Energy", "TriEagle Energy"]
    SILVER_POINTS_THRESHOLD = 25.0
    LLM_MAX_WORKERS         = 5

    PRODUCT_NAME_TO_ID: Dict[str, str] = {
        # ── Original entries ──────────────────────────────────────────────────────
        "Real Deal 12":                         "c9df5c1f-8b24-4ac9-809a-3cad441fee6e",
        "Simple Choice 10":                     "c2c76d42-4f59-403d-b763-3098e0c723a9",
        "Live Your Free 15":                    "eb5874c9-1458-42b7-8590-5a57625ebd1a",
        "Silver Eagle 12":                      "6781e7f4-9b98-4b10-a1df-b2e7e3f83f4b",
        "Golden Eagle 36":                      "6c559d6f-bdbb-4c96-bae1-d8e8aab0d971",
        "Real Saver 12":                        "ac2b67c9-e28c-4cb9-a13f-52fec059ab3a",
        "Simple Start 16":                      "dc6175b6-9a11-4856-928f-c6b93844b954",
        "Smart Edge 24":                        "46b0fa79-9cc3-4ee9-9bd8-e48aa9bb0eda",
        "Gexa Eco Saver Lite 12":               "a2bc35c3-6426-4ec9-a19a-3c329c4d1bcf",
        "Smart Deal 12":                        "71225e97-12aa-4775-b043-2bbafd3c5b15",
        "Gexa Prime Preferred Plus 12":         "82a2a92b-b5a2-4a87-a6f2-f7793b126f91",
        "Flex Rewards":                         "9cd47f9f-7701-4f91-b7ec-e1f3a266b86d",
        "Live Your Free 12":                    "bcdee2eb-479c-4b21-b5f5-f61838ee3ad3",
        "Smart Deal 36":                        "d20bf7aa-0851-478c-89e7-68ed3f7f49b2",
        "Texas Choice 12":                      "1a4b9dd6-5cb1-4433-80ec-963d37fb9bd9",
        "Smart Edge 12":                        "66c928b6-ba6b-49b2-b2eb-1175c16b6f93",
        "Simple Start 15":                      "92bf15f0-737b-4a1e-9153-ab878d2ca41d",
        "Gexa Eco Saver Plus 12":               "f93d5235-7013-4328-ad57-7bd9c4d2c075",
        "Simple Secure 12":                     "1cf7fa4a-a7a6-48ae-be92-5000b1c60392",
        "Simple Secure 24":                     "4a3af58a-224d-42fc-bec4-dd96769079bc",
        "Simple Rate 12":                       "c28c8ad0-4aef-4da2-9902-03fb2274a758",
        "Simple Rate 24":                       "773f12b7-8cc0-459c-be56-8f5acc348990",
        "TXU Solar Buyback Plus 24":            "a2c0643e-97f0-4b6f-a59a-4f38849c7647",
        "TXU Solar Buyback Saver 24":           "a6326ec7-a01d-4a62-a815-035faea6ba65",
        "Gexa Solar Export Saver 12":           "5dd4b58e-6264-4e49-937b-076b87798672",
        "Gexa Freedom 12":                      "36df18a3-f791-4216-a9b4-3fdd86efea97",
        "Gexa Prime Preferred 12":              "5c3545ed-db2f-4fd6-8b19-6cebbffb9064",
        "Octo Volt 14":                         "e9879690-7a0e-45e1-9cf1-986d7da59f93",

        # ── Name-variant aliases (same UUID as existing entry, different string) ──
        "Gexa Prime Preferred 12 Plan":         "5c3545ed-db2f-4fd6-8b19-6cebbffb9064",  # alias of "Gexa Prime Preferred 12"
        "Solar Buyback Saver 24":               "a6326ec7-a01d-4a62-a815-035faea6ba65",  # alias of "TXU Solar Buyback Saver 24"
        "Solar Buyback Plus 24":                "a2c0643e-97f0-4b6f-a59a-4f38849c7647",  # alias of "TXU Solar Buyback Plus 24"

        # ── New plans (genuinely missing from the original map) ───────────────────
        "Gexa Saver Freedom 36":                "894a4bc0-f077-4153-bebd-4a4433f1c505",
        "Octo Volt 18":                         "120bcce7-a78b-4fbb-bcb8-7b0ec792922f",
        "12 Month - prepaid":                   "f906a20b-a9c0-4964-818a-1bea968fd692",
        "Texas Choice 24":                      "0fad406e-fb1c-4a34-aeec-27fc5aa9dce6",
        "Octopus Lite 12":                      "ca1913e2-f533-4f42-b0e3-b99f6452f0f8",
        "6 Month - prepaid":                    "a781c784-3c32-4dbc-a0a7-43ef2eddb60e",
        "Green Eagle 36":                       "daa370be-44f1-488b-aeba-94b79970b4ad",
        "Gexa Straight Saver 24":               "58a4caba-6f97-4638-ab42-c24a83b21ae1",
        "Real Saver 36":                        "09ad56cf-6e0a-4c2b-82cb-0d633e8d3a11",
        "Solar Club 12":                        "dacb9e9e-b668-4304-9e5d-0e33aebe8dd4",
        "Reliant Conservation 24 plan":         "71d40776-ef8f-40b9-9609-3e68e64e6d14",
        "On Your Terms":                        "06254a15-0660-483c-b0d8-6178e284705a",
        "Simple Choice 16":                     "c2e9a291-7082-40ae-8ab1-4d281c8c4625",
        "Daytime Pass 24":                      "47f20a4e-5bc5-4bee-ba24-d6b35082bf09",
        "Free Nights & Solar Days 24 (8 pm)":   "e1645840-b07f-4f7d-8a9d-7fd849856848",
        "Simple Choice 24":                     "76ffd2d5-28d4-4324-997c-912bc7dfe27a",
        "Luminous Green 12":                    "0e17af0d-853a-4ba7-8fe2-9fbb6641ef1a",
        "Gexa Saver Freedom 24":                "8a8b7b78-a6fc-42c8-af5d-b3a56a36a775",
        "Octo Volt 12":                         "4ce40594-e206-4454-b02c-f9c8705ecc31",
    }

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def pick_first_existing_column(df, candidates):
        cols = set(df.columns)
        for c in candidates:
            if c in cols:
                return c
        return None

    def sample_dicts(sdf, limit: int = 20) -> List[Dict[str, Any]]:
        return [row.asDict(recursive=True) for row in sdf.limit(limit).collect()]

    def plan_compare_key_col(col_expr):
        c = F.col(col_expr) if isinstance(col_expr, str) else col_expr
        key = F.regexp_replace(F.lower(F.trim(c)), r"[^a-z0-9]", "")
        return F.when(c.isNull() | (F.length(key) == 0), F.lit(None).cast("string")).otherwise(key)

    def plan_compare_key_sql(col_expr: str) -> str:
        key = f"regexp_replace(lower(trim({col_expr})), '[^a-z0-9]', '')"
        return f"case when {col_expr} is null or {key} = '' then null else {key} end"

    def _norm_plan_type_key(k: Optional[str]) -> Optional[str]:
        if not k:
            return None
        s = str(k).strip().lower()
        if "fixed" in s: return "Fixed"
        if "tier"  in s: return "Tiered"
        if "bund"  in s: return "Bundled"
        if "low"   in s: return "Low"
        return None

    def _get_prob_weight(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
        raw_probs = entry.get("raw_probabilities") or {}
        weights   = entry.get("points_weights") or {}
        out = {
            "raw_prob_fixed": None, "raw_prob_tiered": None,
            "raw_prob_bundled": None, "raw_prob_low": None,
            "weight_fixed": None, "weight_tiered": None,
            "weight_bundled": None, "weight_low": None,
        }
        for k, v in (raw_probs.items() if isinstance(raw_probs, dict) else []):
            nk = _norm_plan_type_key(k)
            if nk: out[f"raw_prob_{nk.lower()}"] = float(v) if v is not None else None
        for k, v in (weights.items() if isinstance(weights, dict) else []):
            nk = _norm_plan_type_key(k)
            if nk: out[f"weight_{nk.lower()}"] = float(v) if v is not None else None
        return out

    def _expected_points_and_gaps(pw: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        def ep(prob_key, weight_key):
            p, w = pw.get(prob_key), pw.get(weight_key)
            return p * w if p is not None and w is not None else None
        eps   = [x for x in [ep("raw_prob_fixed","weight_fixed"), ep("raw_prob_tiered","weight_tiered"),
                               ep("raw_prob_bundled","weight_bundled"), ep("raw_prob_low","weight_low")] if x is not None]
        eps_s = sorted(eps, reverse=True)
        return {
            "expected_points_fixed":   ep("raw_prob_fixed",   "weight_fixed"),
            "expected_points_tiered":  ep("raw_prob_tiered",  "weight_tiered"),
            "expected_points_bundled": ep("raw_prob_bundled", "weight_bundled"),
            "expected_points_low":     ep("raw_prob_low",     "weight_low"),
            "expected_points_gap_1_2": (eps_s[0] - eps_s[1]) if len(eps_s) >= 2 else None,
            "expected_points_gap_2_3": (eps_s[1] - eps_s[2]) if len(eps_s) >= 3 else None,
        }

    def parse_rank_payload_for_etl(payload: str) -> Dict[str, object]:
        out = {
            "product_category_1_plan_category": None, "product_category_1_product_name_1": None,
            "product_category_1_product_name_2": None, "product_category_2_plan_category": None,
            "product_category_2_product_name_1": None, "product_category_2_product_name_2": None,
            "product_category_3_plan_category": None,  "product_category_3_product_name_1": None,
            "product_category_3_product_name_2": None, "product_category_4_plan_category": None,
            "product_category_4_product_name_1": None, "product_category_4_product_name_2": None,
            "raw_prob_fixed": None, "raw_prob_tiered": None, "raw_prob_bundled": None, "raw_prob_low": None,
            "expected_points_fixed": None, "expected_points_tiered": None,
            "expected_points_bundled": None, "expected_points_low": None,
            "expected_points_gap_1_2": None, "expected_points_gap_2_3": None,
        }
        if not payload:
            return out
        try:
            obj = json.loads(payload)
        except Exception:
            return out
        data = obj.get("data")
        if not isinstance(data, list) or not data:
            return out
        entry = data[0] or {}
        for i in (1, 2, 3, 4):
            cat = entry.get(f"product_category_{i}") or {}
            if not isinstance(cat, dict):
                continue
            out[f"product_category_{i}_plan_category"]  = cat.get("product_category")
            out[f"product_category_{i}_product_name_1"] = (cat.get("product_1") or {}).get("product_name")
            out[f"product_category_{i}_product_name_2"] = (cat.get("product_2") or {}).get("product_name")
        pw = _get_prob_weight(entry)
        out.update({k: pw[k] for k in ["raw_prob_fixed","raw_prob_tiered","raw_prob_bundled","raw_prob_low"]})
        out.update(_expected_points_and_gaps(pw))
        return out

    def select_recommended_4(parsed):
        return [x for x in [
            parsed.get("product_category_1_product_name_1"),
            parsed.get("product_category_1_product_name_2"),
            parsed.get("product_category_2_product_name_1"),
            parsed.get("product_category_3_product_name_1"),
        ] if x]

    def select_recommended_plan_types_in_order_raw(parsed):
        products = [
            parsed.get("product_category_1_product_name_1"),
            parsed.get("product_category_1_product_name_2"),
            parsed.get("product_category_2_product_name_1"),
            parsed.get("product_category_3_product_name_1"),
        ]
        types = [
            parsed.get("product_category_1_plan_category"),
            parsed.get("product_category_1_plan_category"),
            parsed.get("product_category_2_plan_category"),
            parsed.get("product_category_3_plan_category"),
        ]
        return [t for p, t in zip(products, types) if p]

    STANDARDIZE_PLAN_TYPE_SQL = """
      case
        when {col} is null then null
        when lower(trim({col})) like '%fixed%'  then 'Fixed'
        when lower(trim({col})) like '%tier%'   then 'Tiered'
        when lower(trim({col})) like '%bund%'   then 'Bundled'
        when lower(trim({col})) like '%low%'    then 'Low'
        else initcap(element_at(split(lower(trim({col})), '\\\\s+'), 1))
      end
    """

    # -------------------------------------------------------------------------
    # Step 2 — Canonical plan list + product/category lookups
    # -------------------------------------------------------------------------
    with StepTimer(2, "Loading canonical plans + product/category lookups"):
        canonical_orders_sdf = (
            spark.read.table(V_ORDERS_TABLE)
            .where(F.col("order_date_est") > F.lit("2026-01-01"))
            .where(F.col("order_type") == F.lit("Phone"))
            .where(F.col("company_id") == F.lit(25))
            .where(F.col("partner_name").isNotNull())
            .where(F.col("product_name").isNotNull())
            .select(
                F.concat("partner_name", F.lit(" "), "product_name").alias("canonical_plan_name"),
                "product_id",
                F.col("order_date_est").cast("timestamp").alias("order_date_est"),
            )
        )

        canonical_plan_pdf = _spark_collect(
            canonical_orders_sdf
            .select("canonical_plan_name")
            .distinct()
            .orderBy("canonical_plan_name"),
            "canonical plans toPandas"
        )
        CANONICAL_PLAN_LIST: List[str] = canonical_plan_pdf["canonical_plan_name"].tolist()
        print(f"    {len(CANONICAL_PLAN_LIST)} canonical plans loaded.", flush=True)

        duplicate_pid_names_sdf = (
            canonical_orders_sdf
            .where(F.col("product_id").isNotNull())
            .groupBy("canonical_plan_name")
            .agg(F.countDistinct("product_id").alias("distinct_product_ids"))
            .where(F.col("distinct_product_ids") > 1)
        )
        duplicate_pid_name_count = duplicate_pid_names_sdf.count()
        print(f"    {duplicate_pid_name_count:,} canonical plan names map to multiple product_ids in v_orders.", flush=True)

        w_name_to_pid = Window.partitionBy("canonical_plan_name").orderBy(
            F.col("order_date_est").desc_nulls_last(),
            F.col("product_id").asc_nulls_last(),
        )
        name_to_pid_sdf = (
            canonical_orders_sdf
            .where(F.col("product_id").isNotNull())
            .withColumn("rn", F.row_number().over(w_name_to_pid))
            .where(F.col("rn") == 1)
            .select("canonical_plan_name", "product_id")
        )

        masterlist_base_sdf = spark.read.table(PLAN_MASTERLIST_TABLE)
        if "plan_category" not in masterlist_base_sdf.columns:
            raise ValueError(f"{PLAN_MASTERLIST_TABLE} missing required category column: plan_category")
        if "plan_id" not in masterlist_base_sdf.columns:
            raise ValueError(f"{PLAN_MASTERLIST_TABLE} missing required id column: plan_id")

        raw_category_values = [
            row["plan_category"]
            for row in (
                masterlist_base_sdf
                .select("plan_category")
                .where(F.col("plan_category").isNotNull())
                .distinct()
                .orderBy("plan_category")
                .limit(50)
                .collect()
            )
        ]
        print(f"    masterlist plan_category raw values (up to 50): {raw_category_values}", flush=True)

        plan_category_candidates_sdf = (
            masterlist_base_sdf
            .select(
                "plan_id",
                F.expr(STANDARDIZE_PLAN_TYPE_SQL.format(col="plan_category")).alias("plan_category"),
            )
            .where(F.col("plan_id").isNotNull())
            .dropDuplicates(["plan_id", "plan_category"])
        )

        standardized_category_values = [
            row["plan_category"]
            for row in (
                plan_category_candidates_sdf
                .select("plan_category")
                .where(F.col("plan_category").isNotNull())
                .distinct()
                .orderBy("plan_category")
                .limit(50)
                .collect()
            )
        ]
        print(f"    masterlist plan_category standardized values (up to 50): {standardized_category_values}", flush=True)

        category_conflicts_sdf = (
            name_to_pid_sdf
            .join(plan_category_candidates_sdf, F.col("product_id") == F.col("plan_id"), how="inner")
            .where(F.col("plan_category").isNotNull())
            .groupBy("canonical_plan_name", "product_id")
            .agg(
                F.countDistinct("plan_category").alias("distinct_plan_categories"),
                F.sort_array(F.collect_set("plan_category")).alias("plan_categories"),
            )
            .where(F.col("distinct_plan_categories") > 1)
        )
        category_conflict_count = category_conflicts_sdf.count()
        if category_conflict_count:
            print(
                f"    WARNING: {category_conflict_count:,} canonical names resolve to product_ids with conflicting masterlist categories.",
                flush=True,
            )
            for sample in sample_dicts(category_conflicts_sdf.orderBy("canonical_plan_name"), limit=20):
                print(f"      {sample}", flush=True)

        w_plan_category = Window.partitionBy("plan_id").orderBy(F.col("plan_category").asc_nulls_last())
        plan_category_lookup_sdf = (
            plan_category_candidates_sdf
            .withColumn("rn", F.row_number().over(w_plan_category))
            .where(F.col("rn") == 1)
            .select("plan_id", "plan_category")
        )

    # -------------------------------------------------------------------------
    # Step 3 — Pitch data + center location filter
    # -------------------------------------------------------------------------
    with StepTimer(3, "Reading pitch data + center location filter"):
        pitch_source_sdf = spark.read.table(PITCH_TABLE)
        required_pitch_cols = {"call_id", "call_date", "product_pitched"}
        missing_pitch_cols = sorted(required_pitch_cols - set(pitch_source_sdf.columns))
        if missing_pitch_cols:
            raise ValueError(f"{PITCH_TABLE} missing required pitch columns: {missing_pitch_cols}")

        pitch_order_col = pick_first_existing_column(pitch_source_sdf, PITCH_ORDER_CANDIDATES)
        if pitch_order_col is None:
            raise ValueError(f"No pitch ordering column found. Tried: {PITCH_ORDER_CANDIDATES}")
        print(f"    pitch_order_col = '{pitch_order_col}'", flush=True)

        HAS_PITCH_SUMMARY = "pitch_summary" in pitch_source_sdf.columns
        print(f"    pitch_summary available: {HAS_PITCH_SUMMARY}", flush=True)

        pitch_sdf = (
            pitch_source_sdf
            .where(F.col("call_date").between(START_DATE, END_DATE))
            .where(F.col("product_pitched").isNotNull())
        )

        vac_locations_sdf = (
            spark.read.table(V_AGENT_CALLS_TABLE)
            .select("call_id", "center_location")
            .where(F.col("call_id").isNotNull())
            .where(F.col("center_location").isin(TARGET_CENTER_LOCATIONS))
            .dropDuplicates(["call_id"])
        )

        arcadia_call_ids_sdf = (
            spark.read.table(ARCADIA_TABLE)
            .select("call_id", "session_start_date")
            .dropna(subset=["call_id", "session_start_date"])
            .withColumn("session_date", F.to_date("session_start_date"))
            .where(F.col("session_date").between(START_DATE, END_DATE))
            .join(vac_locations_sdf.select("call_id"), on="call_id", how="inner")
            .select("call_id")
            .dropDuplicates()
        )

        pitch_arcadia_sdf = pitch_sdf.join(arcadia_call_ids_sdf, on="call_id", how="left_semi")
        pitch_count = pitch_arcadia_sdf.count()
        print(f"    {pitch_count:,} pitch rows after Arcadia + center location filter.", flush=True)

    # -------------------------------------------------------------------------
    # Step 4 — Arcadia attrs + failed qualification flag
    # -------------------------------------------------------------------------
    with StepTimer(4, "Building Arcadia attrs + failed-qualification flag"):
        w_arc = Window.partitionBy("call_id").orderBy(F.col("session_date").desc_nulls_last())
        arcadia_target_attrs_sdf = (
            spark.read.table(ARCADIA_TABLE)
            .select("call_id", "session_start_date", "objection_reason")
            .dropna(subset=["call_id", "session_start_date"])
            .withColumn("session_date", F.to_date("session_start_date"))
            .where(F.col("session_date").between(START_DATE, END_DATE))
            .join(vac_locations_sdf.select("call_id"), on="call_id", how="inner")
            .withColumn("rn", F.row_number().over(w_arc))
            .where(F.col("rn") == 1)
            .select("call_id", "objection_reason")
        )

        qual_sdf      = spark.read.table(ENERGY_QUALIFICATIONRESULT_TABLE)
        qual_date_col = pick_first_existing_column(qual_sdf, QUAL_DATE_CANDIDATES)
        if qual_date_col:
            qual_sdf = (
                qual_sdf
                .withColumn("qual_date", F.to_date(F.col(qual_date_col)))
                .where(F.col("qual_date").between(START_DATE, END_DATE))
            )
        failed_qual_flag_sdf = (
            qual_sdf
            .where(F.col("providerName").isin(FAILED_QUAL_PROVIDERS))
            .where(F.upper(F.trim(F.col("response"))) == F.lit("FAILURE"))
            .select("call_id")
            .where(F.col("call_id").isNotNull())
            .dropDuplicates()
            .withColumn("failed_qualification", F.lit(True))
        )

    # -------------------------------------------------------------------------
    # Step 5 — LLM pitch matching (cache-backed, selective cache writes)
    # -------------------------------------------------------------------------
    with StepTimer(5, "LLM pitch matching (cache-backed)"):
        select_cols = ["product_pitched"]
        if HAS_PITCH_SUMMARY:
            select_cols.append("pitch_summary")

        pitches_pdf = (
            pitch_arcadia_sdf
            .select(*select_cols)
            .dropna(subset=["product_pitched"])
            .toPandas()
        )
        if not HAS_PITCH_SUMMARY:
            pitches_pdf["pitch_summary"] = None

        all_unique = pitches_pdf["product_pitched"].dropna().unique()
        print(f"    {len(pitches_pdf):,} pitch rows | {len(all_unique):,} unique product_pitched values.", flush=True)

        cache_df    = load_match_cache()
        cached_keys = set(cache_df["product_pitched"].tolist())

        misses_mask = ~pitches_pdf["product_pitched"].isin(cached_keys)
        misses_pdf  = (
            pitches_pdf[misses_mask]
            .drop_duplicates(subset=["product_pitched"])
            .reset_index(drop=True)
        )
        print(f"    {len(misses_pdf):,} cache misses — sending to LLM.", flush=True)

        if not misses_pdf.empty:
            new_match_lookup = build_pitch_match_lookup(
                misses_pdf, CANONICAL_PLAN_LIST, openai_api_key, LLM_MAX_WORKERS
            )

            cacheable = [
                (pp, m, mc, nmc)
                for pp, (m, mc, nmc) in new_match_lookup.items()
                if should_cache_result(m, mc, nmc)
            ]
            skipped = len(new_match_lookup) - len(cacheable)

            unspec       = sum(1 for m, _, _ in new_match_lookup.values() if m in UNRESOLVED_PITCH_VALUES)
            real_matches = len(new_match_lookup) - unspec
            low_conf     = [(pp, m, mc) for pp, (m, mc, nmc) in new_match_lookup.items()
                            if m not in UNRESOLVED_PITCH_VALUES and mc < 0.70]

            print(f"    New matches: {real_matches} resolved | {unspec} unspecified | "
                  f"low-confidence (<0.70): {len(low_conf)} | "
                  f"uncertain unspecified (not cached): {skipped}", flush=True)
            for pp, m, mc in low_conf:
                print(f"      '{pp}' -> '{m}' (confidence: {mc:.2f})")

            new_rows_df = pd.DataFrame(cacheable, columns=CACHE_COLS)
            cache_df = (
                pd.concat([cache_df, new_rows_df], ignore_index=True)
                .drop_duplicates(subset=["product_pitched"])
                .reset_index(drop=True)
            )
            save_match_cache(cache_df)
        else:
            print(f"    All pitches already cached — no LLM calls needed.", flush=True)

        run_keys     = set(all_unique)
        cache_subset = cache_df[cache_df["product_pitched"].isin(run_keys)]

        # Spark join only needs the three core columns; no_match_confidence stays in cache file only
        match_lookup_sdf = spark.createDataFrame(
            cache_subset[["product_pitched", "matched_plan_name", "match_confidence"]]
        )

    # -------------------------------------------------------------------------
    # Step 6 — Call-level ordered pitches, unresolved pitches dropped + re-indexed
    #
    # Unspecified / unmatched pitches are dropped before call-level arrays are
    # assembled. These represent general plan theme discussions, not actual
    # plan pitches. Remaining resolved pitches are re-indexed so that
    # first_pitch_matched always refers to the first *valid* plan pitch on the
    # call. A call where every pitch was unresolved will have empty matched
    # arrays and null first_pitch_matched.
    # -------------------------------------------------------------------------
    with StepTimer(6, "Building call-level ordered pitches (unresolved dropped + re-indexed)"):

        # Join match lookup at the individual pitch level before aggregating
        all_pitches_sdf = (
            pitch_arcadia_sdf
            .select(
                "call_id", "call_date", "product_pitched",
                F.col(pitch_order_col).alias("pitch_order")
            )
            .join(match_lookup_sdf, on="product_pitched", how="left")
            .withColumn("matched_plan_name", F.coalesce("matched_plan_name", F.lit("UNMATCHED")))
            .withColumn("match_confidence",  F.coalesce("match_confidence",  F.lit(0.0)))
            .join(
                name_to_pid_sdf,
                F.col("matched_plan_name") == F.col("canonical_plan_name"),
                how="left",
            )
            .join(
                plan_category_lookup_sdf,
                F.col("product_id") == F.col("plan_id"),
                how="left",
            )
            .drop("canonical_plan_name", "plan_id")
            .withColumn("is_resolved",
                ~F.col("matched_plan_name").isin(UNRESOLVED_PITCH_VALUES))
        )

        category_counts = all_pitches_sdf.where(F.col("is_resolved")).agg(
            F.sum(F.when(F.col("plan_category").isNotNull(), 1).otherwise(0)).alias("resolved_with_plan_category"),
            F.sum(F.when(F.col("plan_category").isNull(), 1).otherwise(0)).alias("resolved_without_plan_category"),
        ).collect()[0]
        print(
            "    Resolved pitch category coverage: "
            f"{category_counts['resolved_with_plan_category'] or 0:,} with category | "
            f"{category_counts['resolved_without_plan_category'] or 0:,} without category.",
            flush=True,
        )

        missing_category_sdf = (
            all_pitches_sdf
            .where(F.col("is_resolved"))
            .where(F.col("product_id").isNotNull())
            .where(F.col("plan_category").isNull())
            .select("matched_plan_name", "product_id")
            .dropDuplicates()
            .orderBy("matched_plan_name", "product_id")
        )
        missing_category_count = missing_category_sdf.count()
        print(
            f"    {missing_category_count:,} matched plan/product_id pairs have no masterlist category.",
            flush=True,
        )
        if missing_category_count:
            print("    Sample missing masterlist categories:", flush=True)
            for sample in sample_dicts(missing_category_sdf, limit=20):
                print(f"      {sample}", flush=True)

        run_category_conflicts_sdf = (
            all_pitches_sdf
            .where(F.col("is_resolved"))
            .where(F.col("product_id").isNotNull())
            .select("matched_plan_name", "product_id")
            .dropDuplicates()
            .join(
                plan_category_candidates_sdf.select(
                    F.col("plan_id").alias("candidate_plan_id"),
                    F.col("plan_category").alias("candidate_plan_category"),
                ),
                F.col("product_id") == F.col("candidate_plan_id"),
                how="inner",
            )
            .where(F.col("candidate_plan_category").isNotNull())
            .groupBy("matched_plan_name", "product_id")
            .agg(
                F.countDistinct("candidate_plan_category").alias("distinct_plan_categories"),
                F.sort_array(F.collect_set("candidate_plan_category")).alias("plan_categories"),
            )
            .where(F.col("distinct_plan_categories") > 1)
            .orderBy("matched_plan_name", "product_id")
        )
        run_category_conflict_count = run_category_conflicts_sdf.count()
        if run_category_conflict_count:
            print(
                f"    WARNING: {run_category_conflict_count:,} matched plan/product_id pairs have conflicting masterlist categories.",
                flush=True,
            )
            for sample in sample_dicts(run_category_conflicts_sdf, limit=20):
                print(f"      {sample}", flush=True)

        # Raw pitch arrays — ALL pitches regardless of match status, for display/debug
        raw_pitches_sdf = (
            all_pitches_sdf
            .groupBy("call_id")
            .agg(
                F.min("call_date").alias("call_date"),
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            F.col("pitch_order").alias("ord"),
                            F.col("product_pitched").alias("product_pitched"),
                            F.col("plan_category").alias("plan_category"),
                        )
                    )
                ).alias("pitches_struct")
            )
            .withColumn("pitches_in_order",
                F.expr("transform(pitches_struct, x -> x.product_pitched)"))
            .withColumn("pitches_plan_category_in_order",
                F.expr("transform(pitches_struct, x -> x.plan_category)"))
            .withColumn("first_pitch", F.element_at("pitches_in_order", 1))
            .drop("pitches_struct")
        )

        # Resolved-only pitch arrays — unresolved pitches dropped, remainder re-indexed.
        # Position in the sorted-and-filtered array becomes the new effective pitch order.
        resolved_pitches_sdf = (
            all_pitches_sdf
            .where(F.col("is_resolved"))
            .groupBy("call_id")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            F.col("pitch_order").alias("ord"),
                            F.col("matched_plan_name").alias("matched_plan_name"),
                            F.col("match_confidence").alias("match_confidence"),
                            F.col("plan_category").alias("plan_category"),
                        )
                    )
                ).alias("res_struct")
            )
            .withColumn("pitches_matched_in_order",
                F.expr("transform(res_struct, x -> x.matched_plan_name)"))
            .withColumn("pitches_match_confidence",
                F.expr("transform(res_struct, x -> x.match_confidence)"))
            .withColumn("first_pitch_matched",
                F.element_at("pitches_matched_in_order", 1))
            .withColumn("first_pitch_match_confidence",
                F.element_at("pitches_match_confidence", 1))
            # Masterlist-derived plan_category for the first resolved pitch
            .withColumn("first_pitch_plan_category_raw",
                F.expr("res_struct[0].plan_category"))
            .drop("res_struct")
        )

        # Apply plan_category corrections to first_pitch_plan_category.
        # Corrections override known-wrong masterlist values identified through validation.
        corrections_sdf = spark.createDataFrame(
            pd.DataFrame(
                list(PLAN_CATEGORY_CORRECTIONS.items()),
                columns=["first_pitch_matched", "corrected_plan_category"],
            )
        )

        resolved_pitches_sdf = (
            resolved_pitches_sdf
            .join(corrections_sdf, on="first_pitch_matched", how="left")
            .withColumn(
                "first_pitch_plan_category",
                F.coalesce("corrected_plan_category", "first_pitch_plan_category_raw"),
            )
            .drop("corrected_plan_category", "first_pitch_plan_category_raw")
        )

        # Merge raw and resolved onto all calls that had any pitch rows.
        # Calls whose every pitch was unresolved keep raw display arrays but get
        # empty matched arrays and null first_pitch_matched.
        pitches_matched_sdf = (
            raw_pitches_sdf
            .join(
                resolved_pitches_sdf.drop("call_date"),
                on="call_id",
                how="left",
            )
            .withColumn("pitches_matched_in_order",
                F.coalesce("pitches_matched_in_order", F.array().cast("array<string>")))
            .withColumn("pitches_match_confidence",
                F.coalesce("pitches_match_confidence", F.array().cast("array<double>")))
            .withColumn("first_pitch_plan_category",
                F.coalesce("first_pitch_plan_category", F.lit(None).cast("string")))
        )

        pitches_matched_sdf = (
            pitches_matched_sdf
            .join(failed_qual_flag_sdf, on="call_id", how="left")
            .withColumn("failed_qualification", F.coalesce("failed_qualification", F.lit(False)))
        )

        pitch_count_after = pitches_matched_sdf.count()
        print(f"    {pitch_count_after:,} call-level rows after pitch re-indexing.", flush=True)

    # -------------------------------------------------------------------------
    # Step 7 — Rank model outputs + recommendation normalization
    # -------------------------------------------------------------------------
    with StepTimer(7, "Rank model outputs + recommendation normalization via product ID lookup"):
        rank_sdf = (
            spark.read.table(RAW_MODEL_EVALUATED_TABLE)
            .where(F.col("modelFieldName").ilike("agent-assist-product-rank"))
            .select("correlationId", "_timeStamp", "outputValueString")
            .dropna(subset=["correlationId", "outputValueString"])
        )
        rank_pdf = _spark_collect(rank_sdf, "rank model toPandas")
        if not rank_pdf.empty:
            rank_pdf = (
                rank_pdf.sort_values(["correlationId", "_timeStamp"], ascending=[True, False])
                .drop_duplicates(subset=["correlationId"], keep="first")
                .reset_index(drop=True)
            )
        print(f"    {len(rank_pdf):,} deduplicated rank rows to parse", flush=True)

        parsed_rank_rows = []
        for _, row in tqdm(rank_pdf.iterrows(), total=len(rank_pdf), desc="Parsing rank payloads", unit="row", dynamic_ncols=True):
            parsed = parse_rank_payload_for_etl(row["outputValueString"])
            parsed["call_id"]                             = row["correlationId"]
            parsed["recommended_4_in_order"]              = select_recommended_4(parsed)
            parsed["recommended_plan_types_in_order_raw"] = select_recommended_plan_types_in_order_raw(parsed)
            parsed["top_recommended_plan_type_raw"]       = parsed.get("product_category_1_plan_category")
            parsed_rank_rows.append(parsed)

        rank_flat_pdf = (
            pd.DataFrame(parsed_rank_rows) if parsed_rank_rows
            else pd.DataFrame(columns=[
                "call_id", "recommended_4_in_order", "recommended_plan_types_in_order_raw",
                "top_recommended_plan_type_raw", "raw_prob_fixed", "raw_prob_tiered",
                "raw_prob_bundled", "raw_prob_low", "expected_points_fixed", "expected_points_tiered",
                "expected_points_bundled", "expected_points_low", "expected_points_gap_1_2", "expected_points_gap_2_3",
            ])
        )
        rank_flat_sdf = spark.createDataFrame(rank_flat_pdf)

        rank_with_lists_sdf = (
            rank_flat_sdf
            .withColumn("recommended_in_order",
                F.expr("filter(recommended_4_in_order, x -> x is not null)"))
            .withColumn("recommended_plan_types_in_order_raw",
                F.expr("filter(recommended_plan_types_in_order_raw, x -> x is not null)"))
            .withColumn("recommended_plan_types_in_order",
                F.expr(f"transform(recommended_plan_types_in_order_raw, x -> {STANDARDIZE_PLAN_TYPE_SQL.format(col='x')})"))
            .withColumn("top_recommended_plan_type",
                F.expr(STANDARDIZE_PLAN_TYPE_SQL.format(col="top_recommended_plan_type_raw")))
            .drop("recommended_4_in_order", "recommended_plan_types_in_order_raw", "top_recommended_plan_type_raw")
        )

        rec_product_ids = list(PRODUCT_NAME_TO_ID.values())
        rec_lookup_source_sdf = (
            spark.read.table(V_ORDERS_TABLE)
            .where(F.col("product_id").isin(rec_product_ids))
            .where(F.col("partner_name").isNotNull())
            .where(F.col("product_name").isNotNull())
            .select(
                "product_id",
                F.concat("partner_name", F.lit(" "), "product_name").alias("rec_canonical_name"),
                F.col("order_date_est").cast("timestamp").alias("order_date_est"),
            )
        )
        w_rec_lookup = Window.partitionBy("product_id").orderBy(
            F.col("order_date_est").desc_nulls_last(),
            F.col("rec_canonical_name").asc_nulls_last(),
        )
        rec_id_to_canonical_pdf = _spark_collect(
            rec_lookup_source_sdf
            .withColumn("rn", F.row_number().over(w_rec_lookup))
            .where(F.col("rn") == 1)
            .select("product_id", "rec_canonical_name"),
            "rec product lookup toPandas"
        )
        id_to_canonical = dict(zip(rec_id_to_canonical_pdf["product_id"], rec_id_to_canonical_pdf["rec_canonical_name"]))
        rec_name_to_canonical = {
            raw_name: id_to_canonical.get(pid, "UNKNOWN")
            for raw_name, pid in PRODUCT_NAME_TO_ID.items()
        }
        rec_name_lookup_sdf = spark.createDataFrame(
            pd.DataFrame(list(rec_name_to_canonical.items()), columns=["raw_rec_name", "rec_canonical_name"])
        )

        rank_exploded_sdf = (
            rank_with_lists_sdf
            .select(
                "call_id", "recommended_plan_types_in_order", "top_recommended_plan_type",
                "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled", "raw_prob_low",
                "expected_points_fixed", "expected_points_tiered",
                "expected_points_bundled", "expected_points_low",
                "expected_points_gap_1_2", "expected_points_gap_2_3",
                F.posexplode("recommended_in_order").alias("pos", "raw_rec_name"),
            )
            .join(rec_name_lookup_sdf, on="raw_rec_name", how="left")
            .withColumn("rec_canonical_name", F.coalesce("rec_canonical_name", F.lit("UNKNOWN")))
        )

        rank_normalized_sdf = (
            rank_exploded_sdf
            .groupBy(
                "call_id", "recommended_plan_types_in_order", "top_recommended_plan_type",
                "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled", "raw_prob_low",
                "expected_points_fixed", "expected_points_tiered",
                "expected_points_bundled", "expected_points_low",
                "expected_points_gap_1_2", "expected_points_gap_2_3",
            )
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(F.col("pos").alias("ord"), "rec_canonical_name", "raw_rec_name")
                    )
                ).alias("rec_struct")
            )
            .withColumn("recommended_matched_in_order",
                F.expr("transform(rec_struct, x -> x.rec_canonical_name)"))
            .withColumn("top_recommended_matched",
                F.element_at("recommended_matched_in_order", 1))
            .withColumn("recommended_raw_in_order",
                F.expr("transform(rec_struct, x -> x.raw_rec_name)"))
            .drop("rec_struct")
        )

        unresolved_recs = [k for k, v in rec_name_to_canonical.items() if v == "UNKNOWN"]
        print(f"    {len(rec_name_to_canonical)-len(unresolved_recs)}/{len(rec_name_to_canonical)} rec products resolved.", flush=True)
        if unresolved_recs:
            print(f"    Unresolved: {unresolved_recs}")

    # -------------------------------------------------------------------------
    # Step 8 — Element-view flags
    # -------------------------------------------------------------------------
    with StepTimer(8, "Building element-view flags"):
        element_viewed_sdf = spark.read.table(ELEMENT_VIEWED_TABLE)
        element_date_col   = pick_first_existing_column(element_viewed_sdf, ELEMENT_DATE_CANDIDATES)
        if element_date_col:
            element_viewed_sdf = (
                element_viewed_sdf
                .withColumn("element_date", F.to_date(element_date_col))
                .where(F.col("element_date").between(START_DATE, END_DATE))
            )
        element_flags_sdf = (
            element_viewed_sdf
            .select(F.col("callId").alias("call_id"), "moduleName")
            .where(F.col("callId").isNotNull())
            .where(F.col("moduleName").isin("top_rec_pitch", "slide_recs_pitch", "all_plans_pitch"))
            .groupBy("call_id")
            .agg(
                F.max(F.when(F.col("moduleName") == "top_rec_pitch",    1).otherwise(0)).alias("has_top_rec_pitch_view_int"),
                F.max(F.when(F.col("moduleName") == "slide_recs_pitch", 1).otherwise(0)).alias("has_slide_recs_pitch_view_int"),
                F.max(F.when(F.col("moduleName") == "all_plans_pitch",  1).otherwise(0)).alias("has_all_plans_pitch_view_int"),
            )
            .withColumn("has_top_rec_pitch_view",    F.col("has_top_rec_pitch_view_int")    == 1)
            .withColumn("has_slide_recs_pitch_view", F.col("has_slide_recs_pitch_view_int") == 1)
            .withColumn("has_all_plans_pitch_view",  F.col("has_all_plans_pitch_view_int")  == 1)
            .select("call_id", "has_top_rec_pitch_view", "has_slide_recs_pitch_view", "has_all_plans_pitch_view")
        )

    # -------------------------------------------------------------------------
    # Step 9 — Agent metadata, points, GCV, v_calls attrs
    # -------------------------------------------------------------------------
    with StepTimer(9, "Building agent metadata, points, GCV, v_calls attrs"):
        agent_base_sdf = (
            spark.read.table(RPT_AGENT_CALLS_TABLE).alias("rac")
            .join(vac_locations_sdf.alias("vac"), on="call_id", how="inner")
            .select(
                F.col("rac.call_id").alias("call_id"),
                F.col("vac.center_location").alias("center_location"),
                F.col("rac.order_count").alias("order_count"),
                F.col("rac.agent_tier").alias("agent_tier"),
                F.col("rac.agent_name").alias("agent_name"),
            )
        )
        w_agent = Window.partitionBy("call_id").orderBy(
            F.col("order_count").desc_nulls_last(),
            F.col("agent_name").asc_nulls_last(),
            F.col("agent_tier").asc_nulls_last(),
            F.col("center_location").asc_nulls_last(),
        )
        agent_sdf = (
            agent_base_sdf
            .withColumn("rn", F.row_number().over(w_agent))
            .where(F.col("rn") == 1)
            .drop("rn")
            .withColumn("order_rate", F.when(F.col("order_count") > 0, F.lit(1.0)).otherwise(F.lit(0.0)))
        )

        points_by_call_sdf = (
            spark.read.table(ORDER_POINTS_TABLE)
            .select("call_id", "points")
            .where(F.col("call_id").isNotNull())
            .where(F.col("points").isNotNull())
            .groupBy("call_id")
            .agg(F.sum(F.col("points").cast("double")).alias("points"))
        )

        orders_sdf = spark.read.table(V_ORDERS_TABLE)
        gcv_v2_col = pick_first_existing_column(orders_sdf, GCV_V2_COL_CANDIDATES)
        if gcv_v2_col is None:
            raise ValueError(f"Could not find gcv_v2 column. Tried: {GCV_V2_COL_CANDIDATES}")

        gcv_by_call_sdf = (
            orders_sdf
            .select("call_id", F.col(gcv_v2_col).cast("double").alias("gcv_v2"))
            .where(F.col("call_id").isNotNull())
            .where(F.col("gcv_v2").isNotNull())
            .groupBy("call_id")
            .agg(F.sum("gcv_v2").alias("gcv"))
        )

        masterlist_sdf = (
            spark.read.table(PLAN_MASTERLIST_TABLE)
            .select("plan_id", "plan_name", "supplier_name")
            .where(F.col("plan_id").isNotNull())
            .withColumn("plan_canonical_key",
                F.regexp_replace(
                    F.lower(F.trim(F.concat_ws("", "supplier_name", "plan_name"))),
                    r"[\s\-]+", ""
                )
            )
        )

        w_plan = Window.partitionBy("plan_canonical_key").orderBy(F.col("call_id").desc())
        plan_points_lookup_sdf = (
            orders_sdf
            .select("call_id", "product_id")
            .where(F.col("call_id").isNotNull())
            .where(F.col("product_id").isNotNull())
            .join(masterlist_sdf.select("plan_id", "plan_canonical_key"),
                  F.col("product_id") == F.col("plan_id"), how="inner")
            .join(
                spark.read.table(ORDER_POINTS_TABLE)
                    .select("call_id", F.col("points").cast("double").alias("points"))
                    .where(F.col("call_id").isNotNull())
                    .where(F.col("points").isNotNull()),
                on="call_id", how="inner"
            )
            .withColumn("rn", F.row_number().over(w_plan))
            .where(F.col("rn") == 1)
            .select("plan_canonical_key", F.col("points").alias("first_pitch_plan_points"))
        )

        plan_name_points_lookup_sdf = (
            plan_points_lookup_sdf
            .withColumn(
                "pk_noterm",
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace("plan_canonical_key", r"prepay$", ""),
                    r"plan$", ""),
                r"\d+$", "")
            )
            .select(
                F.col("pk_noterm").alias("plan_noterm_key"),
                F.col("first_pitch_plan_points").alias("plan_points")
            )
            .groupBy("plan_noterm_key")
            .agg(F.max("plan_points").alias("plan_points"))
        )

        first_pitch_points_sdf = (
            pitches_matched_sdf
            .select("call_id", "first_pitch_matched")
            .where(F.col("first_pitch_matched").isNotNull())
            .where(~F.col("first_pitch_matched").isin(UNRESOLVED_PITCH_VALUES))
            .withColumn("fp_noterm",
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.lower(F.trim(F.regexp_replace("first_pitch_matched", r"[^a-zA-Z0-9\s]", ""))),
                            r"\s+", ""),
                        r"prepay$", ""),
                r"plan$", ""),
                r"\d+$", "")
            )
            .join(
                plan_name_points_lookup_sdf,
                F.col("fp_noterm") == F.col("plan_noterm_key"),
                how="left"
            )
            .select("call_id", F.col("plan_points").alias("first_pitch_plan_points"))
        )

        v_calls_attrs_sdf = (
            spark.read.table(V_CALLS_TABLE)
            .select("call_id", "web_session_id", "ivr_split_name", "mover_switcher", "talk_time_minutes")
            .where(F.col("call_id").isNotNull())
            .dropDuplicates(["call_id"])
            .withColumn("site_serp",
                F.when(F.col("web_session_id").isNull(), F.lit("SERP")).otherwise(F.lit("Site")))
            .withColumn("marketing_bucket",
                F.when(F.col("ivr_split_name").isin("natural_marketingbucket", "natural_marketingbucket_serp"), F.lit("Natural"))
                .when(F.col("ivr_split_name").isin("brandpartner_marketingbucket", "brandpartner_marketingbucket_serp"), F.lit("Brand-Partner"))
                .when(F.col("ivr_split_name").isin("generic_marketingbucket", "generic_marketingbucket_serp"), F.lit("Generic"))
                .when(F.col("ivr_split_name").isin("aggregator_marketingbucket", "aggregator_marketingbucket_serp"), F.lit("Aggregator"))
                .when(F.col("ivr_split_name").isin("competitor_marketingbucket", "competitor_marketingbucket_serp"), F.lit("Competitor"))
                .when(F.col("ivr_split_name").isin("dereg_utility_check", "dereg_utility_check_serp"), F.lit("Utility"))
                .when(F.col("ivr_split_name").isin("pmax_marketingbucket", "pmax_marketingbucket_serp"), F.lit("PMax"))
                .when(F.col("ivr_split_name").isin("nrg_bucket", "nrg_bucket_serp"), F.lit("NRG"))
                .otherwise(F.lit("Other Bucket")))
            .select("call_id", "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes")
        )

    # -------------------------------------------------------------------------
    # Step 10 — Sold product canonical + sale_type noterm keys
    # -------------------------------------------------------------------------
    with StepTimer(10, "Building sold-product canonical keys + sale_type noterm keys"):
        order_supplier_col = pick_first_existing_column(orders_sdf, ORDER_SUPPLIER_COL_CANDIDATES)
        _sold_cols = ["call_id", "product_id", "product_name"]
        if order_supplier_col:
            _sold_cols.append(order_supplier_col)

        sold_base_sdf = (
            orders_sdf
            .select(*_sold_cols)
            .where(F.col("call_id").isNotNull())
            .where(F.col("product_name").isNotNull())
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("call_id").orderBy(
                    F.col("product_name").asc_nulls_last(),
                    F.col("product_id").asc_nulls_last(),
                )
            ))
            .where(F.col("rn") == 1)
        )

        if order_supplier_col:
            sold_base_sdf = sold_base_sdf.withColumn("sold_partner_name", F.col(order_supplier_col))
        else:
            sold_base_sdf = sold_base_sdf.join(
                masterlist_sdf.select(F.col("plan_id"), F.col("supplier_name").alias("sold_partner_name")),
                F.col("product_id") == F.col("plan_id"),
                how="left"
            )

        sold_product_canon_sdf = (
            sold_base_sdf
            .withColumn(
                "product_canon",
                F.regexp_replace(
                    F.lower(F.trim(F.concat_ws(" ", F.col("sold_partner_name"), F.col("product_name")))),
                    r"[^a-z0-9\s]", ""
                )
            )
            .withColumn("product_canon", F.regexp_replace("product_canon", r"\s+", " "))
            .withColumn("sold_product_canon_noterm",
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace("product_canon", r"\s+prepay$", ""),
                    r"\s+plan$", ""),
                r"\s+\d+$", ""))
            .select(
                "call_id",
                "sold_product_canon_noterm",
                F.col("product_name").alias("sold_plan_name"),
                "sold_partner_name",
            )
        )

        sold_product_points_sdf = (
            orders_sdf
            .select("call_id")
            .where(F.col("call_id").isNotNull())
            .join(
                spark.read.table(ORDER_POINTS_TABLE)
                    .select("call_id", F.col("points").cast("double").alias("sold_product_points"))
                    .where(F.col("call_id").isNotNull())
                    .where(F.col("points").isNotNull()),
                on="call_id", how="left"
            )
            .groupBy("call_id")
            .agg(F.max("sold_product_points").alias("sold_product_points"))
        )

        def _noterm_expr(col_expr: str) -> str:
            return (
                f"regexp_replace(regexp_replace(regexp_replace(regexp_replace("
                f"regexp_replace(lower(trim({col_expr})), '[^a-z0-9\\\\s]', ''), "
                f"'\\\\s+', ' '), '\\\\s+prepay$', ''), '\\\\s+plan$', ''), '\\\\s+\\\\d+$', '')"
            )

        rec_noterm_sdf = (
            rank_normalized_sdf
            .select("call_id", "recommended_matched_in_order")
            .withColumn("rec1_noterm",
                F.when(F.size("recommended_matched_in_order") >= 1,
                    F.expr(_noterm_expr("element_at(recommended_matched_in_order, 1)"))))
            .withColumn("rec2_noterm",
                F.when(F.size("recommended_matched_in_order") >= 2,
                    F.expr(_noterm_expr("element_at(recommended_matched_in_order, 2)"))))
            .withColumn("rec3_noterm",
                F.when(F.size("recommended_matched_in_order") >= 3,
                    F.expr(_noterm_expr("element_at(recommended_matched_in_order, 3)"))))
            .withColumn("rec4_noterm",
                F.when(F.size("recommended_matched_in_order") >= 4,
                    F.expr(_noterm_expr("element_at(recommended_matched_in_order, 4)"))))
            .select("call_id", "rec1_noterm", "rec2_noterm", "rec3_noterm", "rec4_noterm")
        )

    # -------------------------------------------------------------------------
    # Step 11 — Per-pitch points array (resolved pitches only, re-indexed)
    # -------------------------------------------------------------------------
    with StepTimer(11, "Building per-pitch points array (pitches_plan_points_in_order)"):
        pitches_with_points_sdf = (
            pitches_matched_sdf
            .select("call_id", F.posexplode("pitches_matched_in_order").alias("pos", "pitch_matched"))
            .withColumn(
                "pitch_noterm",
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.lower(F.trim(F.regexp_replace("pitch_matched", r"[^a-zA-Z0-9\s]", ""))),
                            r"\s+", ""),
                        r"prepay$", ""),
                    r"plan$", ""),
                r"\d+$", "")
            )
            .join(
                plan_name_points_lookup_sdf,
                F.col("pitch_noterm") == F.col("plan_noterm_key"),
                how="left"
            )
            .groupBy("call_id")
            .agg(
                F.sort_array(
                    F.collect_list(F.struct(F.col("pos").alias("ord"), F.col("plan_points")))
                ).alias("points_struct")
            )
            .withColumn(
                "pitches_plan_points_in_order",
                F.expr("transform(points_struct, x -> x.plan_points)")
            )
            .select("call_id", "pitches_plan_points_in_order")
        )

        pitches_matched_sdf = pitches_matched_sdf.join(pitches_with_points_sdf, on="call_id", how="left")
        pitches_matched_sdf = pitches_matched_sdf.withColumn(
            "pitches_plan_points_in_order",
            F.coalesce("pitches_plan_points_in_order", F.array().cast("array<double>"))
        )

    # -------------------------------------------------------------------------
    # Step 12 — Final join + derived columns
    # -------------------------------------------------------------------------
    print(f"\n[12/{StepTimer.TOTAL_STEPS}] ▶  Building final call-level DataFrame ...", flush=True)
    _t12 = time.time()

    # Calls with no pitch rows at all (never appeared in the pitch source table)
    empty_pitch_defaults_sdf = (
        arcadia_call_ids_sdf
        .join(pitches_matched_sdf.select("call_id"), on="call_id", how="left_anti")
        .withColumn("call_date",                      F.lit(None).cast("date"))
        .withColumn("pitches_in_order",               F.array().cast("array<string>"))
        .withColumn("pitches_plan_category_in_order", F.array().cast("array<string>"))
        .withColumn("first_pitch",                    F.lit(None).cast("string"))
        .withColumn("first_pitch_plan_category",      F.lit(None).cast("string"))
        .withColumn("pitches_matched_in_order",       F.array().cast("array<string>"))
        .withColumn("pitches_match_confidence",       F.array().cast("array<double>"))
        .withColumn("pitches_plan_points_in_order",   F.array().cast("array<double>"))
        .withColumn("first_pitch_matched",            F.lit(None).cast("string"))
        .withColumn("first_pitch_match_confidence",   F.lit(None).cast("double"))
        .withColumn("failed_qualification",           F.lit(False))
    )

    all_calls_sdf = pitches_matched_sdf.unionByName(empty_pitch_defaults_sdf)

    final_sdf = (
        all_calls_sdf
        .join(rank_normalized_sdf, on="call_id", how="inner")
        .where(F.col("recommended_matched_in_order").isNotNull() & (F.size("recommended_matched_in_order") > 0))

        .withColumn("has_payless_pitch",
            F.expr("exists(pitches_in_order, x -> x is not null and lower(x) like '%payless%')"))
        .withColumn("has_low_rec",
            F.expr("exists(recommended_plan_types_in_order, x -> x is not null and lower(trim(x)) = 'low')"))

        .withColumn("recommended_matched_keys_in_order",
            F.expr(f"transform(recommended_matched_in_order, x -> {plan_compare_key_sql('x')})"))
        .withColumn("pitches_matched_keys_in_order",
            F.expr(f"transform(pitches_matched_in_order, x -> {plan_compare_key_sql('x')})"))
        .withColumn("first_pitch_key", plan_compare_key_col("first_pitch_matched"))

        .withColumn("rec1", F.when(F.size("recommended_matched_in_order") >= 1, F.element_at("recommended_matched_in_order", 1)))
        .withColumn("rec2", F.when(F.size("recommended_matched_in_order") >= 2, F.element_at("recommended_matched_in_order", 2)))
        .withColumn("rec3", F.when(F.size("recommended_matched_in_order") >= 3, F.element_at("recommended_matched_in_order", 3)))
        .withColumn("rec4", F.when(F.size("recommended_matched_in_order") >= 4, F.element_at("recommended_matched_in_order", 4)))
        .withColumn("rec1_key", F.when(F.size("recommended_matched_keys_in_order") >= 1, F.element_at("recommended_matched_keys_in_order", 1)))
        .withColumn("rec2_key", F.when(F.size("recommended_matched_keys_in_order") >= 2, F.element_at("recommended_matched_keys_in_order", 2)))
        .withColumn("rec3_key", F.when(F.size("recommended_matched_keys_in_order") >= 3, F.element_at("recommended_matched_keys_in_order", 3)))
        .withColumn("rec4_key", F.when(F.size("recommended_matched_keys_in_order") >= 4, F.element_at("recommended_matched_keys_in_order", 4)))

        .join(agent_sdf,                on="call_id", how="left")
        .join(points_by_call_sdf,       on="call_id", how="left")
        .join(gcv_by_call_sdf,          on="call_id", how="left")
        .join(arcadia_target_attrs_sdf, on="call_id", how="left")
        .join(element_flags_sdf,        on="call_id", how="left")
        .join(first_pitch_points_sdf,   on="call_id", how="left")
        .join(v_calls_attrs_sdf,        on="call_id", how="left")
        .join(sold_product_canon_sdf,   on="call_id", how="left")
        .join(sold_product_points_sdf,  on="call_id", how="left")
        .join(rec_noterm_sdf,           on="call_id", how="left")

        .withColumn("points", F.coalesce("points", F.lit(0.0)))
        .withColumn("gcv",    F.coalesce("gcv",    F.lit(0.0)))
        .withColumn("has_top_rec_pitch_view",    F.coalesce("has_top_rec_pitch_view",    F.lit(False)))
        .withColumn("has_slide_recs_pitch_view", F.coalesce("has_slide_recs_pitch_view", F.lit(False)))
        .withColumn("has_all_plans_pitch_view",  F.coalesce("has_all_plans_pitch_view",  F.lit(False)))

        .withColumn("product_type_adhered",
            F.when(
                F.col("first_pitch_plan_category").isNotNull() &
                F.col("top_recommended_plan_type").isNotNull() &
                (F.col("first_pitch_plan_category") == F.col("top_recommended_plan_type")),
                F.lit(True)
            ).otherwise(F.lit(False)))

        .withColumn("pitched_top_rec_first",
            F.when(
                F.col("first_pitch_key").isNotNull() &
                F.col("rec1_key").isNotNull() &
                (F.col("first_pitch_key") == F.col("rec1_key")),
                F.lit(True)
            ).otherwise(F.lit(False)))
        .withColumn("pitched_slide_rec_first",
            F.when(
                F.col("first_pitch_key").isNotNull() &
                (F.col("pitched_top_rec_first") == F.lit(False)) &
                (
                    (F.col("rec2_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec2_key"))) |
                    (F.col("rec3_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec3_key"))) |
                    (F.col("rec4_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec4_key")))
                ),
                F.lit(True)
            ).otherwise(F.lit(False)))
        .withColumn("pitched_all_plans_first",
            F.when(
                F.col("first_pitch_key").isNotNull() &
                (F.col("rec1_key").isNull() | (F.col("first_pitch_key") != F.col("rec1_key"))) &
                (F.col("rec2_key").isNull() | (F.col("first_pitch_key") != F.col("rec2_key"))) &
                (F.col("rec3_key").isNull() | (F.col("first_pitch_key") != F.col("rec3_key"))) &
                (F.col("rec4_key").isNull() | (F.col("first_pitch_key") != F.col("rec4_key"))),
                F.lit(True)
            ).otherwise(F.lit(False)))

        .withColumn("adhered_call",
            F.when((F.col("pitched_top_rec_first") == F.lit(True)) & (F.col("has_top_rec_pitch_view") == F.lit(True)), F.lit(1.0))
            .otherwise(F.lit(0.0)))
        .withColumn("slide_call",
            F.when((F.col("pitched_slide_rec_first") == F.lit(True)) & (F.col("has_slide_recs_pitch_view") == F.lit(True)), F.lit(1.0))
            .otherwise(F.lit(0.0)))
        .withColumn("all_plans_call",
            F.when(
                (F.col("has_all_plans_pitch_view") == F.lit(True)) &
                (F.col("adhered_call") == F.lit(0.0)) &
                (F.col("slide_call")   == F.lit(0.0)),
                F.lit(1.0)
            ).otherwise(F.lit(0.0)))

        .withColumn("plan_adhered",                   F.col("adhered_call") == F.lit(1.0))
        .withColumn("slide_first",                    F.col("pitched_slide_rec_first"))
        .withColumn("all_plans_first",                F.col("pitched_all_plans_first"))
        .withColumn("all_plans_product_type_adhered", F.col("all_plans_call") == F.lit(1.0))
        .withColumn("classification_bucket",
            F.when(F.col("adhered_call")  == F.lit(1.0), F.lit("Adherence"))
            .when(F.col("slide_call")     == F.lit(1.0), F.lit("Slide"))
            .when(F.col("all_plans_call") == F.lit(1.0), F.lit("All Plans"))
            .otherwise(F.lit("Unclassified")))

        .withColumn("first_pitch_type",
            F.when(F.col("first_pitch_matched").isNull(), F.lit(None))
            .when(F.col("rec1_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec1_key")), F.lit("Diamond"))
            .when(
                (F.col("rec2_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec2_key"))) |
                (F.col("rec3_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec3_key"))) |
                (F.col("rec4_key").isNotNull() & (F.col("first_pitch_key") == F.col("rec4_key"))),
                F.lit("Gold"))
            .when(
                F.expr("get(pitches_plan_points_in_order, 0)").cast("double") >= F.lit(SILVER_POINTS_THRESHOLD),
                F.lit("Silver"))
            .otherwise(F.lit("Bronze")))

        .withColumn("pitch_types_in_order",
            F.expr(f"""
                case
                    when array_size(pitches_matched_in_order) = 0 then array()
                    else transform(
                        sequence(0, array_size(pitches_matched_in_order) - 1),
                        i ->
                        case
                            when pitches_matched_keys_in_order[i] is null then null
                            when rec1_key is not null and pitches_matched_keys_in_order[i] = rec1_key then 'Diamond'
                            when (
                                (rec2_key is not null and pitches_matched_keys_in_order[i] = rec2_key) or
                                (rec3_key is not null and pitches_matched_keys_in_order[i] = rec3_key) or
                                (rec4_key is not null and pitches_matched_keys_in_order[i] = rec4_key)
                            ) then 'Gold'
                            when cast(get(pitches_plan_points_in_order, i) as double) >= {SILVER_POINTS_THRESHOLD} then 'Silver'
                            else 'Bronze'
                        end
                    )
                end
            """))

        .withColumn("points_on_first_pitch",
            F.when((F.col("order_count") > 0) & (F.size("pitches_in_order") == 1), F.col("points"))
            .otherwise(F.lit(0.0)))
        .withColumn("gcv_on_first_pitch",
            F.when((F.col("order_count") > 0) & (F.size("pitches_in_order") == 1), F.col("gcv"))
            .otherwise(F.lit(0.0)))

        .withColumn("sale_type",
            F.when(F.col("order_count").isNull() | (F.col("order_count") == 0), F.lit(None))
            .when(F.col("sold_product_canon_noterm").isNull(), F.lit(None))
            .when(F.col("rec1_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec1_noterm")), F.lit("Diamond"))
            .when(
                (F.col("rec2_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec2_noterm"))) |
                (F.col("rec3_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec3_noterm"))) |
                (F.col("rec4_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec4_noterm"))),
                F.lit("Gold"))
            .when(F.col("sold_product_points").cast("double") >= F.lit(SILVER_POINTS_THRESHOLD), F.lit("Silver"))
            .otherwise(F.lit("Bronze")))

        .withColumn("happy_path",
            F.when(
                F.col("failed_qualification").isNull() | F.col("failed_qualification"),
                F.lit(0)
            )
            .when(F.col("has_payless_pitch") | F.col("has_low_rec"), F.lit(0))
            .when(
                F.col("first_pitch_matched").isNull() &
                (F.size("pitches_matched_in_order") == 0),
                F.lit(0)
            )
            .otherwise(F.lit(1)))

        .drop(
            "sold_product_canon_noterm", "sold_product_points",
            "rec1_noterm", "rec2_noterm", "rec3_noterm", "rec4_noterm",
            "rec1", "rec2", "rec3", "rec4",
            "rec1_key", "rec2_key", "rec3_key", "rec4_key", "first_pitch_key",
            "recommended_matched_keys_in_order", "pitches_matched_keys_in_order",
        )
    )

    duplicate_calls_sdf = (
        final_sdf
        .groupBy("call_id")
        .agg(F.count("*").alias("row_count"))
        .where(F.col("row_count") > 1)
    )
    duplicate_call_count = duplicate_calls_sdf.count()
    if duplicate_call_count:
        duplicate_extra_rows = duplicate_calls_sdf.agg(F.sum(F.col("row_count") - 1).alias("extra_rows")).collect()[0]["extra_rows"] or 0
        print(
            f"    WARNING: {duplicate_call_count:,} call_id values still had duplicate rows "
            f"({duplicate_extra_rows:,} extra rows). Keeping one deterministic row per call_id.",
            flush=True,
        )
        w_final_call = Window.partitionBy("call_id").orderBy(
            F.col("call_date").desc_nulls_last(),
            F.col("order_count").desc_nulls_last(),
            F.col("first_pitch_plan_points").desc_nulls_last(),
            F.col("first_pitch_match_confidence").desc_nulls_last(),
            F.col("points").desc_nulls_last(),
            F.col("agent_name").asc_nulls_last(),
        )
        final_sdf = (
            final_sdf
            .withColumn("_call_level_rn", F.row_number().over(w_final_call))
            .where(F.col("_call_level_rn") == 1)
            .drop("_call_level_rn")
        )

    final_sdf = final_sdf.select(
        "call_id", "center_location", "agent_name", "agent_tier", "call_date",
        "order_count", "order_rate", "points", "points_on_first_pitch",
        "gcv", "gcv_on_first_pitch",
        "objection_reason", "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes",
        "pitches_in_order", "pitches_plan_category_in_order",
        "first_pitch", "first_pitch_plan_category",
        "pitches_matched_in_order", "pitches_match_confidence",
        "pitches_plan_points_in_order",
        "first_pitch_matched", "first_pitch_match_confidence",
        "recommended_matched_in_order",
        "recommended_raw_in_order",
        "top_recommended_matched",
        "recommended_plan_types_in_order", "top_recommended_plan_type",
        "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled", "raw_prob_low",
        "expected_points_fixed", "expected_points_tiered",
        "expected_points_bundled", "expected_points_low",
        "expected_points_gap_1_2", "expected_points_gap_2_3",
        "has_top_rec_pitch_view", "has_slide_recs_pitch_view", "has_all_plans_pitch_view",
        "pitched_top_rec_first", "pitched_slide_rec_first", "pitched_all_plans_first",
        "product_type_adhered", "plan_adhered", "slide_first", "all_plans_first",
        "all_plans_product_type_adhered", "adhered_call", "slide_call", "all_plans_call",
        "classification_bucket",
        "first_pitch_type", "pitch_types_in_order", "sale_type",
        "sold_plan_name", "sold_partner_name", "first_pitch_plan_points",
        "failed_qualification", "has_payless_pitch", "has_low_rec", "happy_path",
    )

    print(f"[12/{StepTimer.TOTAL_STEPS}] ✔  Final call-level DataFrame built  ({time.time()-_t12:.1f}s)", flush=True)

    # -------------------------------------------------------------------------
    # Step 13 — Agent performance quartiles
    # -------------------------------------------------------------------------
    with StepTimer(13, "Computing agent-level performance quartiles"):
        agent_perf_sdf = (
            final_sdf
            .where(F.col("agent_name").isNotNull())
            .groupBy("agent_name")
            .agg(F.avg("points_on_first_pitch").alias("avg_points_on_first_pitch"))
        )
        w_perf = Window.orderBy(F.col("avg_points_on_first_pitch").desc_nulls_last())
        agent_perf_sdf = agent_perf_sdf.withColumn("performance_quartile", F.ntile(4).over(w_perf))

        final_sdf = final_sdf.join(
            agent_perf_sdf.select("agent_name", "avg_points_on_first_pitch", "performance_quartile"),
            on="agent_name", how="left"
        )

        final_sdf = final_sdf.select(
            "call_id", "center_location", "agent_name", "agent_tier",
            "performance_quartile", "avg_points_on_first_pitch",
            "call_date", "order_count", "order_rate", "points", "points_on_first_pitch",
            "gcv", "gcv_on_first_pitch", "objection_reason",
            "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes",
            "pitches_in_order", "pitches_plan_category_in_order",
            "first_pitch", "first_pitch_plan_category",
            "pitches_matched_in_order", "pitches_match_confidence",
            "pitches_plan_points_in_order",
            "first_pitch_matched", "first_pitch_match_confidence",
            "recommended_matched_in_order",
            "recommended_raw_in_order",
            "top_recommended_matched",
            "recommended_plan_types_in_order", "top_recommended_plan_type",
            "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled", "raw_prob_low",
            "expected_points_fixed", "expected_points_tiered",
            "expected_points_bundled", "expected_points_low",
            "expected_points_gap_1_2", "expected_points_gap_2_3",
            "has_top_rec_pitch_view", "has_slide_recs_pitch_view", "has_all_plans_pitch_view",
            "pitched_top_rec_first", "pitched_slide_rec_first", "pitched_all_plans_first",
            "product_type_adhered", "plan_adhered", "slide_first", "all_plans_first",
            "all_plans_product_type_adhered", "adhered_call", "slide_call", "all_plans_call",
            "classification_bucket",
            "first_pitch_type", "pitch_types_in_order", "sale_type",
            "sold_plan_name", "sold_partner_name", "first_pitch_plan_points",
            "failed_qualification", "has_payless_pitch", "has_low_rec", "happy_path",
        )

        final_sdf.createOrReplaceTempView("CALL_LEVEL_PITCHES_AND_RECS")

    # -------------------------------------------------------------------------
    # Step 14 — Collect to pandas
    # -------------------------------------------------------------------------
    with StepTimer(14, "Collecting final DataFrame from Spark → pandas"):
        result_df = _spark_collect(final_sdf, "final toPandas")
        print(f"    Final shape: {result_df.shape[0]:,} rows × {result_df.shape[1]} cols", flush=True)

    return result_df


def deploy_app():
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.apps import AppDeployment

    w = WorkspaceClient()
    print("Deploying product-rec-dash...")
    deployment = w.apps.deploy(
        app_name="arcadia-product-rec-dash",
        app_deployment=AppDeployment(
            source_code_path="/Workspace/Users/fnisbet@redventures.com/product-rec-dash"
        )
    )
    print(f"Deploy complete: {deployment}")


# -------------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------------

if __name__ == "__main__":
    _env_path = os.path.join(os.getcwd(), ".env")
    load_dotenv(_env_path)
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    if not openai_api_key:
        raise EnvironmentError(
            f"OPENAI_API_KEY not found. Looked for .env at: {_env_path}\n"
            "Make sure the .env file exists in the current working directory "
            f"({os.getcwd()}) and contains: OPENAI_API_KEY=sk-..."
        )

    df = get_data(openai_api_key=openai_api_key)

    save_chunked_csv(
        df=df,
        base_dir="./data",
        base_filename="call_level_data",
        max_bytes=9 * 1024 * 1024,
    )

    deploy_app()

    print("Done")
