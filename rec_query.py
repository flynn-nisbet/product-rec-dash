import json
import re
import time
import glob
from typing import Dict, Any, List, Optional
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType
import os
from datetime import date, timedelta
from databricks.connect import DatabricksSession
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Helpers: step-level progress tracking
# ---------------------------------------------------------------------------

class StepTimer:
    """Prints a labelled banner when entering a step and elapsed time on exit."""

    TOTAL_STEPS = 13  # updated: added step for sold_product_canon + rec_noterm

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
    """Wrap a .toPandas() call with a tqdm spinner."""
    with tqdm(total=0, desc=desc, bar_format="{desc}: {elapsed}  [collecting from Spark...]", dynamic_ncols=True) as pbar:
        result = sdf.toPandas()
        pbar.set_postfix_str(f"done — {len(result):,} rows")
    return result


def get_data():

    with StepTimer(1, "Initialising Spark session"):
        spark = DatabricksSession.builder \
            .host("redventures-rv-energy-prod-production-9xwiei.cloud.databricks.com") \
            .serverless(True) \
            .getOrCreate()

    # -----------------------------
    # CONSTANTS / CONFIG
    # -----------------------------

    START_DATE = "2026-02-16"
    END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    RAW_MODEL_EVALUATED_TABLE        = "lakehouse_production.ai_products.raw_model_evaluated"
    ENRICHED_PITCH_TABLE             = "ai_products_prod.arcadia.pitch_extraction_enriched"
    ARCADIA_TABLE                    = "energy_prod.energy.rpt_arcadia_frontend"
    V_AGENT_CALLS_TABLE              = "energy_prod.energy.v_agent_calls"
    RPT_AGENT_CALLS_TABLE            = "lakehouse_production.energy.rpt_agent_calls"
    ORDER_POINTS_TABLE               = "lakehouse_production.energy.event_integration_orderpointssubmitted"
    ENERGY_QUALIFICATIONRESULT_TABLE = "lakehouse_production.energy.event_energy_qualificationresult"
    ELEMENT_VIEWED_TABLE             = "lakehouse_production.energy.event_arcadia_elementviewed"
    PLAN_MASTERLIST_TABLE            = "ai_products_prod.compass_dev.energy_plan_masterlist"
    V_CALLS_TABLE                    = "energy_prod.energy.v_calls"
    V_ORDERS_TABLE                   = "energy_prod.energy.v_orders"

    FAILED_QUAL_PROVIDERS   = ["TXU Energy", "TriEagle Energy"]
    QUAL_DATE_CANDIDATES    = ["call_date", "event_date", "event_ts", "created_at", "_timeStamp", "timestamp"]
    PITCH_ORDER_CANDIDATES  = ["pitch_index", "pitch_order", "turn_index", "created_at", "event_ts", "index"]
    ELEMENT_DATE_CANDIDATES = ["call_date", "event_date", "event_ts", "created_at", "_timeStamp", "timestamp"]

    GCV_V2_COL_CANDIDATES = ["gcv_v2"]
    ORDER_SUPPLIER_COL_CANDIDATES = ["supplier_name", "partner_name", "brand_name"]

    TARGET_CENTER_LOCATIONS = ["Durban", "Jamaica", "Charlotte"]

    SILVER_POINTS_THRESHOLD = 25.0

    # -----------------------------
    # Canonicalization helpers
    # -----------------------------

    def canonicalize_py(s: str) -> str:
        if not isinstance(s, str):
            return ""
        s = s.lower()
        s = re.sub(r"[^a-z0-9\s]", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def strip_term_py(s: str) -> str:
        """Remove trailing standalone number (contract term)."""
        return re.sub(r"\s+\d+$", "", s).strip()

    def normalize_rec_name_py(s: str) -> str:
        """Canonicalize and strip known suffixes that don't appear in order product names."""
        if not isinstance(s, str):
            return ""
        c = canonicalize_py(s)
        c = re.sub(r"\s+prepay$", "", c).strip()
        c = re.sub(r"\s+plan$",   "", c).strip()
        c = strip_term_py(c)
        return c

    canonicalize_udf      = F.udf(canonicalize_py,      StringType())
    strip_term_udf        = F.udf(strip_term_py,         StringType())
    normalize_rec_name_udf = F.udf(normalize_rec_name_py, StringType())

    # -----------------------------
    # JSON parser helpers (rank model)
    # -----------------------------

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
        if isinstance(raw_probs, dict):
            for k, v in raw_probs.items():
                nk = _norm_plan_type_key(k)
                if nk == "Fixed":    out["raw_prob_fixed"]   = float(v) if v is not None else None
                elif nk == "Tiered": out["raw_prob_tiered"]  = float(v) if v is not None else None
                elif nk == "Bundled":out["raw_prob_bundled"] = float(v) if v is not None else None
                elif nk == "Low":    out["raw_prob_low"]     = float(v) if v is not None else None
        if isinstance(weights, dict):
            for k, v in weights.items():
                nk = _norm_plan_type_key(k)
                if nk == "Fixed":    out["weight_fixed"]   = float(v) if v is not None else None
                elif nk == "Tiered": out["weight_tiered"]  = float(v) if v is not None else None
                elif nk == "Bundled":out["weight_bundled"] = float(v) if v is not None else None
                elif nk == "Low":    out["weight_low"]     = float(v) if v is not None else None
        return out

    def _expected_points_and_gaps(prob_weight: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        ep_fixed   = (prob_weight["raw_prob_fixed"]   * prob_weight["weight_fixed"]
                      if prob_weight["raw_prob_fixed"]   is not None and prob_weight["weight_fixed"]   is not None else None)
        ep_tiered  = (prob_weight["raw_prob_tiered"]  * prob_weight["weight_tiered"]
                      if prob_weight["raw_prob_tiered"]  is not None and prob_weight["weight_tiered"]  is not None else None)
        ep_bundled = (prob_weight["raw_prob_bundled"] * prob_weight["weight_bundled"]
                      if prob_weight["raw_prob_bundled"] is not None and prob_weight["weight_bundled"] is not None else None)
        ep_low     = (prob_weight["raw_prob_low"]     * prob_weight["weight_low"]
                      if prob_weight["raw_prob_low"]     is not None and prob_weight["weight_low"]     is not None else None)
        eps        = [x for x in [ep_fixed, ep_tiered, ep_bundled, ep_low] if x is not None]
        eps_sorted = sorted(eps, reverse=True)
        gap_1_2 = (eps_sorted[0] - eps_sorted[1]) if len(eps_sorted) >= 2 else None
        gap_2_3 = (eps_sorted[1] - eps_sorted[2]) if len(eps_sorted) >= 3 else None
        return {
            "expected_points_fixed": ep_fixed, "expected_points_tiered": ep_tiered,
            "expected_points_bundled": ep_bundled, "expected_points_low": ep_low,
            "expected_points_gap_1_2": gap_1_2, "expected_points_gap_2_3": gap_2_3,
        }

    def parse_rank_payload_for_etl(payload: str) -> Dict[str, object]:
        out = {
            "product_category_1_plan_category": None, "product_category_1_product_name_1": None,
            "product_category_1_product_name_2": None, "product_category_2_plan_category": None,
            "product_category_2_product_name_1": None, "product_category_2_product_name_2": None,
            "product_category_3_plan_category": None,  "product_category_3_product_name_1": None,
            "product_category_3_product_name_2": None, "product_category_4_plan_category": None,
            "product_category_4_product_name_1": None, "product_category_4_product_name_2": None,
            "raw_prob_fixed": None, "raw_prob_tiered": None,
            "raw_prob_bundled": None, "raw_prob_low": None,
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
        out.update({
            "raw_prob_fixed": pw["raw_prob_fixed"], "raw_prob_tiered": pw["raw_prob_tiered"],
            "raw_prob_bundled": pw["raw_prob_bundled"], "raw_prob_low": pw["raw_prob_low"],
        })
        out.update(_expected_points_and_gaps(pw))
        return out

    def select_recommended_4(parsed: Dict[str, Any]) -> List[Optional[str]]:
        ordered = [
            parsed.get("product_category_1_product_name_1"),
            parsed.get("product_category_1_product_name_2"),
            parsed.get("product_category_2_product_name_1"),
            parsed.get("product_category_3_product_name_1"),
        ]
        return [x for x in ordered if x]

    def select_recommended_plan_types_in_order_raw(parsed: Dict[str, Any]) -> List[Optional[str]]:
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

    def pick_first_existing_column(df, candidates):
        cols = set(df.columns)
        for c in candidates:
            if c in cols:
                return c
        return None

    # -----------------------------
    # 0) Read pitch enriched data
    # -----------------------------

    with StepTimer(2, "Reading pitch enriched data + resolving pitch-order column"):
        pitch_enriched_sdf = (
            spark.read.table(ENRICHED_PITCH_TABLE)
            .where(F.col("call_date").between(START_DATE, END_DATE))
        )
        pitch_order_col = pick_first_existing_column(pitch_enriched_sdf, PITCH_ORDER_CANDIDATES)
        if pitch_order_col is None:
            raise ValueError(
                f"No pitch ordering column found in {ENRICHED_PITCH_TABLE}. "
                f"Tried: {PITCH_ORDER_CANDIDATES}. Available columns: {pitch_enriched_sdf.columns}"
            )
        print(f"    pitch_order_col = '{pitch_order_col}'")

    # -----------------------------
    # 0A) Center locations from v_agent_calls
    # -----------------------------

    with StepTimer(3, "Building center-location lookup (v_agent_calls)"):
        vac_locations_sdf = (
            spark.read.table(V_AGENT_CALLS_TABLE)
            .select("call_id", "center_location")
            .where(F.col("call_id").isNotNull())
            .where(F.col("center_location").isin(TARGET_CENTER_LOCATIONS))
            .dropDuplicates(["call_id"])
        )

    # -----------------------------
    # 1) Arcadia target flags + objection_reason
    # -----------------------------

    with StepTimer(4, "Building Arcadia target flags + objection_reason (steps 1 & 1A)"):
        arcadia_target_call_ids_sdf = (
            spark.read.table(ARCADIA_TABLE).alias("a")
            .select("call_id", "session_start_date")
            .dropna(subset=["call_id", "session_start_date"])
            .withColumn("session_date", F.to_date("session_start_date"))
            .where(F.col("session_date").between(START_DATE, END_DATE))
            .join(vac_locations_sdf.alias("vac"), on="call_id", how="inner")
            .select("call_id")
            .dropDuplicates()
        )

        in_arcadia_target_sdf = arcadia_target_call_ids_sdf.withColumn("in_arcadia_target", F.lit(True))

        pitch_arcadia_target_sdf = (
            pitch_enriched_sdf
            .join(in_arcadia_target_sdf, on="call_id", how="left")
            .withColumn("in_arcadia_target", F.coalesce(F.col("in_arcadia_target"), F.lit(False)))
        )

        arcadia_target_attrs_sdf = (
            spark.read.table(ARCADIA_TABLE).alias("a")
            .select("call_id", "session_start_date", "objection_reason")
            .dropna(subset=["call_id", "session_start_date"])
            .withColumn("session_date", F.to_date("session_start_date"))
            .where(F.col("session_date").between(START_DATE, END_DATE))
            .join(vac_locations_sdf.alias("vac"), on="call_id", how="inner")
        )
        w_arc = Window.partitionBy("call_id").orderBy(F.col("session_date").desc_nulls_last())
        arcadia_target_attrs_sdf = (
            arcadia_target_attrs_sdf
            .withColumn("rn", F.row_number().over(w_arc))
            .where(F.col("rn") == 1)
            .select("call_id", "objection_reason")
        )

    # -----------------------------
    # 1B) Failed qualification flag
    # -----------------------------

    with StepTimer(5, "Building failed-qualification flag (step 1B)"):
        qual_sdf      = spark.read.table(ENERGY_QUALIFICATIONRESULT_TABLE)
        qual_date_col = pick_first_existing_column(qual_sdf, QUAL_DATE_CANDIDATES)
        if qual_date_col is not None:
            qual_sdf = (
                qual_sdf
                .withColumn("qual_date", F.to_date(F.col(qual_date_col)))
                .where(F.col("qual_date").between(START_DATE, END_DATE))
            )
        failed_qual_call_ids_sdf = (
            qual_sdf
            .where(F.col("providerName").isin(FAILED_QUAL_PROVIDERS))
            .where(F.upper(F.trim(F.col("response"))) == F.lit("FAILURE"))
            .select("call_id")
            .where(F.col("call_id").isNotNull())
            .dropDuplicates()
        )
        failed_qual_flag_sdf = failed_qual_call_ids_sdf.withColumn("failed_qualification", F.lit(True))
        pitch_arcadia_target_sdf = (
            pitch_arcadia_target_sdf
            .join(failed_qual_flag_sdf, on="call_id", how="left")
            .withColumn("failed_qualification", F.coalesce(F.col("failed_qualification"), F.lit(False)))
        )

    # -----------------------------
    # 2) Call-level ordered pitches
    # -----------------------------

    with StepTimer(6, "Building call-level ordered pitches (step 2)"):
        pitches_ordered_sdf = (
            pitch_arcadia_target_sdf
            .select(
                "call_id", "call_date", "product_pitched", "canonical_key", "plan_category",
                "in_arcadia_target", "failed_qualification",
                F.col(pitch_order_col).alias("pitch_order")
            )
            .where(F.col("product_pitched").isNotNull())
            .groupBy("call_id", "call_date", "in_arcadia_target", "failed_qualification")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            F.col("pitch_order").alias("ord"),
                            F.col("product_pitched").alias("product_pitched"),
                            F.col("canonical_key").alias("canonical_key"),
                            F.col("plan_category").alias("plan_category"),
                        )
                    )
                ).alias("pitches_struct")
            )
            .withColumn("pitches_in_order",               F.expr("transform(pitches_struct, x -> x.product_pitched)"))
            .withColumn("pitches_canonical_in_order",     F.expr("transform(pitches_struct, x -> x.canonical_key)"))
            .withColumn("pitches_plan_category_in_order", F.expr("transform(pitches_struct, x -> x.plan_category)"))
            .withColumn("first_pitch",               F.element_at(F.col("pitches_in_order"), 1))
            .withColumn("first_pitch_canonical",     F.element_at(F.col("pitches_canonical_in_order"), 1))
            .withColumn("first_pitch_plan_category", F.element_at(F.col("pitches_plan_category_in_order"), 1))
            .drop("pitches_struct")
        )

    # -----------------------------
    # 3) Rank model outputs -> latest per call_id
    # -----------------------------

    with StepTimer(7, "Collecting rank model outputs from Spark (step 3)"):
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
        print(f"    {len(rank_pdf):,} deduplicated rank rows to parse")

    parsed_rank_rows = []
    for _, row in tqdm(rank_pdf.iterrows(), total=len(rank_pdf), desc="Parsing rank payloads", unit="row", dynamic_ncols=True):
        parsed = parse_rank_payload_for_etl(row["outputValueString"])
        parsed["call_id"]                             = row["correlationId"]
        parsed["recommended_4_in_order"]              = select_recommended_4(parsed)
        parsed["recommended_plan_types_in_order_raw"] = select_recommended_plan_types_in_order_raw(parsed)
        parsed["top_recommended_plan_type_raw"]       = parsed.get("product_category_1_plan_category")
        parsed_rank_rows.append(parsed)

    rank_flat_pdf = (
        pd.DataFrame(parsed_rank_rows)
        if parsed_rank_rows
        else pd.DataFrame(columns=[
            "call_id", "recommended_4_in_order", "recommended_plan_types_in_order_raw",
            "top_recommended_plan_type_raw", "raw_prob_fixed", "raw_prob_tiered",
            "raw_prob_bundled", "raw_prob_low", "expected_points_fixed", "expected_points_tiered",
            "expected_points_bundled", "expected_points_low",
            "expected_points_gap_1_2", "expected_points_gap_2_3",
        ])
    )

    rank_flat_sdf = spark.createDataFrame(rank_flat_pdf)

    STANDARDIZE_PLAN_TYPE_SQL = """
    case
        when {col} is null then null
        when lower(trim({col})) like '%fixed%' then 'Fixed'
        when lower(trim({col})) like '%tier%'  then 'Tiered'
        when lower(trim({col})) like '%bund%'  then 'Bundled'
        when lower(trim({col})) like '%low%'   then 'Low'
        else initcap(element_at(split(lower(trim({col})), '\\\\s+'), 1))
    end
    """

    rank_with_lists_sdf = (
        rank_flat_sdf
        .withColumn("recommended_in_order",
            F.expr("filter(recommended_4_in_order, x -> x is not null)"))
        .withColumn("recommended_canonical_in_order",
            F.expr("transform(recommended_in_order, x -> regexp_replace(lower(trim(x)), '[\\\\s\\\\-]+', ''))"))
        .withColumn("recommended_plan_types_in_order_raw",
            F.expr("filter(recommended_plan_types_in_order_raw, x -> x is not null)"))
        .withColumn("recommended_plan_types_in_order",
            F.expr(f"""
            transform(
                recommended_plan_types_in_order_raw,
                x -> {STANDARDIZE_PLAN_TYPE_SQL.format(col='x')}
            )
            """))
        .withColumn("top_recommended_plan_type",
            F.expr(STANDARDIZE_PLAN_TYPE_SQL.format(col="top_recommended_plan_type_raw")))
        .drop("recommended_4_in_order")
    )

    # -----------------------------
    # 3B) Element-view flags per call
    # -----------------------------

    with StepTimer(8, "Building element-view flags per call (step 3B)"):
        element_viewed_sdf = spark.read.table(ELEMENT_VIEWED_TABLE)
        element_date_col   = pick_first_existing_column(element_viewed_sdf, ELEMENT_DATE_CANDIDATES)
        if element_date_col is not None:
            element_viewed_sdf = (
                element_viewed_sdf
                .withColumn("element_date", F.to_date(F.col(element_date_col)))
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

    # -----------------------------
    # 4) Agent metadata, points, GCV, plan-points lookup, v_calls attrs
    # -----------------------------

    with StepTimer(9, "Building agent metadata, points, GCV & plan-points lookup (steps 4–4E)"):
        agent_sdf = (
            spark.read.table(RPT_AGENT_CALLS_TABLE).alias("rac")
            .join(vac_locations_sdf.alias("vac"), on="call_id", how="inner")
            .select(
                F.col("rac.call_id").alias("call_id"),
                F.col("vac.center_location").alias("center_location"),
                F.col("rac.order_count").alias("order_count"),
                F.col("rac.agent_tier").alias("agent_tier"),
                F.col("rac.agent_name").alias("agent_name"),
            )
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
            raise ValueError(
                f"Could not find gcv_v2 column in {V_ORDERS_TABLE}. "
                f"Tried: {GCV_V2_COL_CANDIDATES}. "
                f"Available columns: {orders_sdf.columns}"
            )

        gcv_by_call_sdf = (
            orders_sdf
            .select("call_id", F.col(gcv_v2_col).alias("gcv_v2"))
            .where(F.col("call_id").isNotNull())
            .where(F.col("gcv_v2").isNotNull())
            .withColumn("gcv_row", F.col("gcv_v2").cast("double"))
            .groupBy("call_id")
            .agg(F.sum("gcv_row").alias("gcv"))
        )

        masterlist_sdf = (
            spark.read.table(PLAN_MASTERLIST_TABLE)
            .select("plan_id", "plan_name", "supplier_name")
            .where(F.col("plan_id").isNotNull())
            .withColumn("plan_canonical_key",
                F.regexp_replace(
                    F.lower(F.trim(F.concat_ws("", F.col("supplier_name"), F.col("plan_name")))),
                    r"[\s\-]+", ""
                )
            )
        )

        sold_orders_sdf = (
            orders_sdf
            .select("call_id", "product_id")
            .where(F.col("call_id").isNotNull())
            .where(F.col("product_id").isNotNull())
            .dropDuplicates(["call_id", "product_id"])
        )

        sold_with_plan_sdf = (
            sold_orders_sdf
            .join(
                masterlist_sdf.select("plan_id", "plan_canonical_key"),
                F.col("product_id") == F.col("plan_id"),
                how="inner"
            )
            .select("call_id", "plan_canonical_key")
        )

        sold_with_points_sdf = (
            sold_with_plan_sdf
            .join(
                spark.read.table(ORDER_POINTS_TABLE)
                    .select("call_id", F.col("points").cast("double").alias("points"))
                    .where(F.col("call_id").isNotNull())
                    .where(F.col("points").isNotNull()),
                on="call_id",
                how="inner"
            )
        )

        w_plan = Window.partitionBy("plan_canonical_key").orderBy(F.col("call_id").desc())
        plan_points_lookup_sdf = (
            sold_with_points_sdf
            .withColumn("rn", F.row_number().over(w_plan))
            .where(F.col("rn") == 1)
            .select(
                "plan_canonical_key",
                F.col("points").alias("first_pitch_plan_points")
            )
        )

        first_pitch_plan_points_sdf = (
            pitches_ordered_sdf
            .select("call_id", "first_pitch_canonical")
            .where(F.col("first_pitch_canonical").isNotNull())
            .join(
                plan_points_lookup_sdf,
                F.col("first_pitch_canonical") == F.col("plan_canonical_key"),
                how="left"
            )
            .select("call_id", "first_pitch_plan_points")
        )

        v_calls_attrs_sdf = (
            spark.read.table(V_CALLS_TABLE)
            .select("call_id", "web_session_id", "ivr_split_name", "mover_switcher", "talk_time_minutes")
            .where(F.col("call_id").isNotNull())
            .dropDuplicates(["call_id"])
            .withColumn(
                "site_serp",
                F.when(F.col("web_session_id").isNull(), F.lit("SERP")).otherwise(F.lit("Site"))
            )
            .withColumn(
                "marketing_bucket",
                F.when(F.col("ivr_split_name").isin("natural_marketingbucket", "natural_marketingbucket_serp"), F.lit("Natural"))
                .when(F.col("ivr_split_name").isin("brandpartner_marketingbucket", "brandpartner_marketingbucket_serp"), F.lit("Brand-Partner"))
                .when(F.col("ivr_split_name").isin("generic_marketingbucket", "generic_marketingbucket_serp"), F.lit("Generic"))
                .when(F.col("ivr_split_name").isin("aggregator_marketingbucket", "aggregator_marketingbucket_serp"), F.lit("Aggregator"))
                .when(F.col("ivr_split_name").isin("competitor_marketingbucket", "competitor_marketingbucket_serp"), F.lit("Competitor"))
                .when(F.col("ivr_split_name").isin("dereg_utility_check", "dereg_utility_check_serp"), F.lit("Utility"))
                .when(F.col("ivr_split_name").isin("pmax_marketingbucket", "pmax_marketingbucket_serp"), F.lit("PMax"))
                .when(F.col("ivr_split_name").isin("nrg_bucket", "nrg_bucket_serp"), F.lit("NRG"))
                .otherwise(F.lit("Other Bucket"))
            )
            .select("call_id", "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes")
        )

    # -----------------------------
    # 4F) Sold product canonical (noterm) from v_orders
    #     Used for sale_type derivation without the enrichment table
    # -----------------------------

    with StepTimer(10, "Building sold-product canonical keys + rec noterm keys for sale_type (step 4F)"):

        order_supplier_col = pick_first_existing_column(orders_sdf, ORDER_SUPPLIER_COL_CANDIDATES)

        _sold_canon_select_cols = ["call_id", "product_id", "product_name"]
        if order_supplier_col:
            _sold_canon_select_cols.append(order_supplier_col)

        # Canonicalize sold product name from v_orders, strip term
        # One row per call_id — take first order consistent with existing logic
        sold_product_canon_sdf = (
            orders_sdf
            .select(*_sold_canon_select_cols)
            .where(F.col("call_id").isNotNull())
            .where(F.col("product_name").isNotNull())
            # Step 1: lowercase and remove non-alphanumeric (except spaces)
            .withColumn("product_canon",
                F.regexp_replace(F.lower(F.trim(F.col("product_name"))), r"[^a-z0-9\s]", ""))
            # Step 2: collapse whitespace
            .withColumn("product_canon",
                F.regexp_replace(F.col("product_canon"), r"\s+", " "))
            # Step 3: strip known suffixes that appear in rec names but not order names
            .withColumn("product_canon",
                F.regexp_replace(F.col("product_canon"), r"\s+prepay$", ""))
            .withColumn("product_canon",
                F.regexp_replace(F.col("product_canon"), r"\s+plan$", ""))
            # Step 4: strip trailing term number
            .withColumn("sold_product_canon_noterm",
                F.regexp_replace(F.col("product_canon"), r"\s+\d+$", ""))
            .withColumn("rn", F.row_number().over(Window.partitionBy("call_id").orderBy("call_id")))
            .where(F.col("rn") == 1)
        )

        if order_supplier_col:
            sold_product_canon_sdf = sold_product_canon_sdf.withColumn(
                "sold_partner_name",
                F.col(order_supplier_col),
            ).drop(order_supplier_col)
        else:
            # No supplier/partner column on v_orders — resolve supplier from plan masterlist
            # (same plan rows that define plan_canonical_key in step 9), keyed by product_id.
            sold_product_canon_sdf = sold_product_canon_sdf.join(
                masterlist_sdf.select(
                    F.col("plan_id"),
                    F.col("supplier_name").alias("sold_partner_name"),
                ),
                F.col("product_id") == F.col("plan_id"),
                how="left",
            ).drop("plan_id")

        sold_product_canon_sdf = sold_product_canon_sdf.select(
            "call_id",
            "sold_product_canon_noterm",
            F.col("product_name").alias("sold_plan_name"),
            "sold_partner_name",
        )

        # Points for the sold product — used for Silver threshold in sale_type
        # Join sold product back to plan_points_lookup via product_canon_noterm
        # Build a noterm -> points lookup from the masterlist + points table
        sold_product_points_sdf = (
            orders_sdf
            .select("call_id", "product_name")
            .where(F.col("call_id").isNotNull())
            .where(F.col("product_name").isNotNull())
            .withColumn("product_canon",
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.trim(F.col("product_name"))), r"[^a-z0-9\s]", ""),
                    r"\s+", " "
                )
            )
            .withColumn("rn", F.row_number().over(Window.partitionBy("call_id").orderBy("call_id")))
            .where(F.col("rn") == 1)
            .join(
                spark.read.table(ORDER_POINTS_TABLE)
                    .select("call_id", F.col("points").cast("double").alias("sold_product_points"))
                    .where(F.col("call_id").isNotNull())
                    .where(F.col("points").isNotNull()),
                on="call_id",
                how="left"
            )
            .select("call_id", "sold_product_points")
        )

        # Derive noterm canonical keys for each rec slot directly from the
        # recommendation display names (same normalization as sold product side)
        rec_noterm_sdf = (
            rank_with_lists_sdf
            .select("call_id", "recommended_in_order")
            .withColumn("rec1_noterm",
                F.when(F.size("recommended_in_order") >= 1,
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.regexp_replace(
                                    F.regexp_replace(F.lower(F.trim(F.element_at("recommended_in_order", 1))),
                                        r"[^a-z0-9\s]", ""),
                                    r"\s+", " "),
                                r"\s+prepay$", ""),
                            r"\s+plan$", ""),
                        r"\s+\d+$", "")
                )
            )
            .withColumn("rec2_noterm",
                F.when(F.size("recommended_in_order") >= 2,
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.regexp_replace(
                                    F.regexp_replace(F.lower(F.trim(F.element_at("recommended_in_order", 2))),
                                        r"[^a-z0-9\s]", ""),
                                    r"\s+", " "),
                                r"\s+prepay$", ""),
                            r"\s+plan$", ""),
                        r"\s+\d+$", "")
                )
            )
            .withColumn("rec3_noterm",
                F.when(F.size("recommended_in_order") >= 3,
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.regexp_replace(
                                    F.regexp_replace(F.lower(F.trim(F.element_at("recommended_in_order", 3))),
                                        r"[^a-z0-9\s]", ""),
                                    r"\s+", " "),
                                r"\s+prepay$", ""),
                            r"\s+plan$", ""),
                        r"\s+\d+$", "")
                )
            )
            .withColumn("rec4_noterm",
                F.when(F.size("recommended_in_order") >= 4,
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.regexp_replace(
                                    F.regexp_replace(F.lower(F.trim(F.element_at("recommended_in_order", 4))),
                                        r"[^a-z0-9\s]", ""),
                                    r"\s+", " "),
                                r"\s+prepay$", ""),
                            r"\s+plan$", ""),
                        r"\s+\d+$", "")
                )
            )
            .select("call_id", "rec1_noterm", "rec2_noterm", "rec3_noterm", "rec4_noterm")
        )

    # -----------------------------
    # 5) Final join + derived columns
    # -----------------------------

    print(f"\n[11/{StepTimer.TOTAL_STEPS}] ▶  Building final call-level DataFrame (step 5 — large join + derived cols) ...", flush=True)
    _step11_start = time.time()

    PITCH_TIER_SQL = """
    case
        when {pitch_key} is null then null
        when {rec1} is not null and instr({pitch_key}, {rec1}) > 0 then 'Diamond'
        when (  ({rec2} is not null and instr({pitch_key}, {rec2}) > 0)
            or ({rec3} is not null and instr({pitch_key}, {rec3}) > 0)
            or ({rec4} is not null and instr({pitch_key}, {rec4}) > 0)
            ) then 'Gold'
        when cast({points_col} as double) >= {silver_threshold} then 'Silver'
        else 'Bronze'
    end
    """

    final_call_level_sdf = (
        pitches_ordered_sdf
        .join(
            rank_with_lists_sdf.select(
                "call_id", "recommended_in_order", "recommended_canonical_in_order",
                "recommended_plan_types_in_order", "top_recommended_plan_type",
                "raw_prob_fixed", "raw_prob_tiered", "raw_prob_bundled", "raw_prob_low",
                "expected_points_fixed", "expected_points_tiered",
                "expected_points_bundled", "expected_points_low",
                "expected_points_gap_1_2", "expected_points_gap_2_3",
            ),
            on="call_id", how="left",
        )
        .where(F.col("recommended_in_order").isNotNull() & (F.size(F.col("recommended_in_order")) > 0))

        # Exclusion audit flags
        .withColumn("has_payless_pitch",
            F.expr("exists(pitches_canonical_in_order, x -> x is not null and x like '%payless%')")
        )
        .withColumn("has_low_rec",
            F.expr("""
                exists(
                    recommended_plan_types_in_order,
                    x -> x is not null and lower(trim(x)) = 'low'
                )
            """)
        )

        # Triplet cleaning (unchanged — still used for first_pitch_type and pitch_types_in_order)
        .withColumn("pitches_triplets", F.expr("""
            transform(
                sequence(1, size(pitches_in_order)),
                i -> struct(
                    element_at(pitches_in_order, i)               as product_pitched,
                    element_at(pitches_canonical_in_order, i)     as canonical_key,
                    element_at(pitches_plan_category_in_order, i) as plan_category
                )
            )
        """))
        .withColumn("pitches_triplets_clean", F.expr("""
            filter(
                pitches_triplets,
                x -> x.canonical_key is not null and lower(trim(x.canonical_key)) <> 'unknown'
            )
        """))
        .withColumn("pitches_in_order",               F.expr("transform(pitches_triplets_clean, x -> x.product_pitched)"))
        .withColumn("pitches_canonical_in_order",     F.expr("transform(pitches_triplets_clean, x -> x.canonical_key)"))
        .withColumn("pitches_plan_category_in_order", F.expr("transform(pitches_triplets_clean, x -> x.plan_category)"))
        .withColumn("first_pitch",
            F.when(F.size("pitches_in_order") >= 1, F.element_at("pitches_in_order", 1)))
        .withColumn("first_pitch_canonical",
            F.when(F.size("pitches_canonical_in_order") >= 1, F.element_at("pitches_canonical_in_order", 1)))
        .withColumn("first_pitch_plan_category",
            F.when(F.size("pitches_plan_category_in_order") >= 1, F.element_at("pitches_plan_category_in_order", 1)))
        .drop("pitches_triplets", "pitches_triplets_clean")
        .where(F.size(F.col("pitches_canonical_in_order")) > 0)

        .join(agent_sdf.select("call_id", "center_location", "agent_name", "agent_tier", "order_count", "order_rate"),
            on="call_id", how="left")
        .join(points_by_call_sdf,          on="call_id", how="left")
        .join(gcv_by_call_sdf,             on="call_id", how="left")
        .join(arcadia_target_attrs_sdf,    on="call_id", how="left")
        .join(element_flags_sdf,           on="call_id", how="left")
        .join(first_pitch_plan_points_sdf, on="call_id", how="left")
        .join(v_calls_attrs_sdf,           on="call_id", how="left")
        # New joins for sale_type from orders
        .join(sold_product_canon_sdf,      on="call_id", how="left")
        .join(sold_product_points_sdf,     on="call_id", how="left")
        .join(rec_noterm_sdf,              on="call_id", how="left")

        .withColumn("points", F.coalesce(F.col("points"), F.lit(0.0)))
        .withColumn("gcv",    F.coalesce(F.col("gcv"),    F.lit(0.0)))
        .withColumn("has_top_rec_pitch_view",    F.coalesce(F.col("has_top_rec_pitch_view"),    F.lit(False)))
        .withColumn("has_slide_recs_pitch_view", F.coalesce(F.col("has_slide_recs_pitch_view"), F.lit(False)))
        .withColumn("has_all_plans_pitch_view",  F.coalesce(F.col("has_all_plans_pitch_view"),  F.lit(False)))

        # rec1–rec4 canonical keys (unchanged — still used for pitch classification)
        .withColumn("rec1",
            F.when(F.size("recommended_canonical_in_order") >= 1, F.element_at("recommended_canonical_in_order", 1)))
        .withColumn("rec2",
            F.when(F.size("recommended_canonical_in_order") >= 2, F.element_at("recommended_canonical_in_order", 2)))
        .withColumn("rec3",
            F.when(F.size("recommended_canonical_in_order") >= 3, F.element_at("recommended_canonical_in_order", 3)))
        .withColumn("rec4",
            F.when(F.size("recommended_canonical_in_order") >= 4, F.element_at("recommended_canonical_in_order", 4)))

        .withColumn("product_type_adhered",
            F.when(
                F.col("first_pitch_plan_category").isNotNull() &
                F.col("top_recommended_plan_type").isNotNull() &
                (F.col("first_pitch_plan_category") == F.col("top_recommended_plan_type")),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn("pitched_top_rec_first",
            F.when(
                F.col("first_pitch_canonical").isNotNull() & F.col("rec1").isNotNull() &
                (F.instr(F.col("first_pitch_canonical"), F.col("rec1")) > 0),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn("pitched_slide_rec_first",
            F.when(
                F.col("first_pitch_canonical").isNotNull() & F.col("rec1").isNotNull() &
                (F.instr(F.col("first_pitch_canonical"), F.col("rec1")) <= 0) &
                (
                    (F.col("rec2").isNotNull() & (F.instr(F.col("first_pitch_canonical"), F.col("rec2")) > 0)) |
                    (F.col("rec3").isNotNull() & (F.instr(F.col("first_pitch_canonical"), F.col("rec3")) > 0)) |
                    (F.col("rec4").isNotNull() & (F.instr(F.col("first_pitch_canonical"), F.col("rec4")) > 0))
                ),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn("pitched_all_plans_first",
            F.when(
                F.col("first_pitch_canonical").isNotNull() &
                (F.col("rec1").isNull() | (F.instr(F.col("first_pitch_canonical"), F.col("rec1")) <= 0)) &
                (F.col("rec2").isNull() | (F.instr(F.col("first_pitch_canonical"), F.col("rec2")) <= 0)) &
                (F.col("rec3").isNull() | (F.instr(F.col("first_pitch_canonical"), F.col("rec3")) <= 0)) &
                (F.col("rec4").isNull() | (F.instr(F.col("first_pitch_canonical"), F.col("rec4")) <= 0)),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn("adhered_call",
            F.when(
                (F.col("pitched_top_rec_first") == F.lit(True)) &
                (F.col("has_top_rec_pitch_view") == F.lit(True)),
                F.lit(1.0)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("slide_call",
            F.when(
                (F.col("pitched_slide_rec_first") == F.lit(True)) &
                (F.col("has_slide_recs_pitch_view") == F.lit(True)),
                F.lit(1.0)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("all_plans_call",
            F.when(
                (F.col("has_all_plans_pitch_view") == F.lit(True)) &
                (F.col("adhered_call") == F.lit(0.0)) &
                (F.col("slide_call")   == F.lit(0.0)),
                F.lit(1.0)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("plan_adhered",               F.col("adhered_call") == F.lit(1.0))
        .withColumn("slide_first",                F.col("pitched_slide_rec_first"))
        .withColumn("all_plans_first",            F.col("pitched_all_plans_first"))
        .withColumn("all_plans_product_type_adhered", F.col("all_plans_call") == F.lit(1.0))
        .withColumn("classification_bucket",
            F.when(F.col("adhered_call")   == F.lit(1.0), F.lit("Adherence"))
            .when(F.col("slide_call")      == F.lit(1.0), F.lit("Slide"))
            .when(F.col("all_plans_call")  == F.lit(1.0), F.lit("All Plans"))
            .otherwise(F.lit("Unclassified"))
        )
        .withColumn("points_on_first_pitch",
            F.when(
                (F.col("order_count") > 0) & (F.size(F.col("pitches_in_order")) == 1),
                F.col("points")
            ).otherwise(F.lit(0.0))
        )
        .withColumn("gcv_on_first_pitch",
            F.when(
                (F.col("order_count") > 0) & (F.size(F.col("pitches_in_order")) == 1),
                F.col("gcv")
            ).otherwise(F.lit(0.0))
        )

        # first_pitch_type: unchanged — still uses enrichment table canonical keys
        # (retained as-is pending enrichment table fix)
        .withColumn(
            "first_pitch_type",
            F.expr(
                PITCH_TIER_SQL.format(
                    pitch_key="first_pitch_canonical",
                    points_col="first_pitch_plan_points",
                    rec1="rec1", rec2="rec2", rec3="rec3", rec4="rec4",
                    silver_threshold=SILVER_POINTS_THRESHOLD,
                )
            )
        )

        # pitch_types_in_order: unchanged — still uses enrichment table canonical keys
        # (retained as-is pending enrichment table fix)
        .withColumn(
            "pitch_types_in_order",
            F.expr(f"""
            transform(
                pitches_canonical_in_order,
                pitch_key ->
                case
                    when pitch_key is null then null
                    when rec1 is not null and instr(pitch_key, rec1) > 0 then 'Diamond'
                    when (   (rec2 is not null and instr(pitch_key, rec2) > 0)
                        or (rec3 is not null and instr(pitch_key, rec3) > 0)
                        or (rec4 is not null and instr(pitch_key, rec4) > 0)
                        ) then 'Gold'
                    when cast(first_pitch_plan_points as double) >= {SILVER_POINTS_THRESHOLD} then 'Silver'
                    else 'Bronze'
                end
            )
            """)
        )

        # ── sale_type: NEW derivation from v_orders product name
        #    Replaces the old approach that read pitch_types_in_order[-1] from the
        #    enrichment table. Now matches the sold product name (canonicalized,
        #    term-stripped) against the rec display names (same normalization).
        #    Diamond = sold product matches rec slot 1
        #    Gold    = sold product matches rec slots 2, 3, or 4
        #    Silver  = sold product points >= threshold
        #    Bronze  = all else
        .withColumn("sale_type",
            F.when(
                F.col("order_count").isNull() | (F.col("order_count") == 0),
                F.lit(None)
            )
            .when(F.col("sold_product_canon_noterm").isNull(), F.lit(None))
            .when(
                F.col("rec1_noterm").isNotNull() &
                (F.col("sold_product_canon_noterm") == F.col("rec1_noterm")),
                F.lit("Diamond")
            )
            .when(
                (F.col("rec2_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec2_noterm"))) |
                (F.col("rec3_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec3_noterm"))) |
                (F.col("rec4_noterm").isNotNull() & (F.col("sold_product_canon_noterm") == F.col("rec4_noterm"))),
                F.lit("Gold")
            )
            .when(
                F.col("sold_product_points").cast("double") >= F.lit(SILVER_POINTS_THRESHOLD),
                F.lit("Silver")
            )
            .otherwise(F.lit("Bronze"))
        )

        # happy_path flag (unchanged)
        .withColumn("happy_path",
            F.when(
                F.col("in_arcadia_target")      &
                ~F.col("failed_qualification")  &
                ~F.col("has_payless_pitch")      &
                ~F.col("has_low_rec"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    # Drop intermediate columns not needed in output
    final_call_level_sdf = final_call_level_sdf.drop(
        "sold_product_canon_noterm", "sold_product_points",
        "rec1_noterm", "rec2_noterm", "rec3_noterm", "rec4_noterm",
    )

    # ── First select (before agent perf join) ────────────────────────────────
    final_call_level_sdf = final_call_level_sdf.select(
        "call_id", "center_location", "agent_name", "agent_tier", "call_date",
        "order_count", "order_rate", "points", "points_on_first_pitch",
        "gcv", "gcv_on_first_pitch", "objection_reason",
        "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes",
        "pitches_in_order", "pitches_canonical_in_order", "pitches_plan_category_in_order",
        "first_pitch", "first_pitch_canonical", "first_pitch_plan_category",
        "recommended_in_order", "recommended_canonical_in_order",
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
        # first_pitch_type and pitch_types_in_order retained — rely on enrichment table
        # pending fix; flagged with comment for future removal/replacement
        "first_pitch_type",       # TODO: replace when enrichment table is reliable
        "pitch_types_in_order",   # TODO: replace when enrichment table is reliable
        "sale_type",              # now derived from v_orders, not enrichment table
        "sold_plan_name",
        "sold_partner_name",
        "first_pitch_plan_points",
        # Exclusion audit columns
        "in_arcadia_target",
        "failed_qualification",
        "has_payless_pitch",
        "has_low_rec",
        "happy_path",
    )

    print(f"[11/{StepTimer.TOTAL_STEPS}] ✔  Final call-level DataFrame plan built  ({time.time() - _step11_start:.1f}s)", flush=True)

    # -----------------------------
    # Agent-level performance
    # -----------------------------

    with StepTimer(12, "Computing agent-level performance quartiles"):
        agent_perf_sdf = (
            final_call_level_sdf
            .where(F.col("agent_name").isNotNull())
            .groupBy("agent_name")
            .agg(F.avg(F.col("points_on_first_pitch")).alias("avg_points_on_first_pitch"))
        )
        w = Window.orderBy(F.col("avg_points_on_first_pitch").desc_nulls_last())
        agent_perf_sdf = agent_perf_sdf.withColumn("performance_quartile", F.ntile(4).over(w))

        final_call_level_sdf = (
            final_call_level_sdf
            .join(
                agent_perf_sdf.select("agent_name", "avg_points_on_first_pitch", "performance_quartile"),
                on="agent_name", how="left"
            )
        )

        final_call_level_sdf = final_call_level_sdf.select(
            "call_id", "center_location", "agent_name", "agent_tier",
            "performance_quartile", "avg_points_on_first_pitch",
            "call_date", "order_count", "order_rate", "points", "points_on_first_pitch",
            "gcv", "gcv_on_first_pitch", "objection_reason",
            "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes",
            "pitches_in_order", "pitches_canonical_in_order", "pitches_plan_category_in_order",
            "first_pitch", "first_pitch_canonical", "first_pitch_plan_category",
            "recommended_in_order", "recommended_canonical_in_order",
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
            "first_pitch_type",       # TODO: replace when enrichment table is reliable
            "pitch_types_in_order",   # TODO: replace when enrichment table is reliable
            "sale_type",              # now derived from v_orders, not enrichment table
            "sold_plan_name",
            "sold_partner_name",
            "first_pitch_plan_points",
            # Exclusion audit columns
            "in_arcadia_target",
            "failed_qualification",
            "has_payless_pitch",
            "has_low_rec",
            "happy_path",
        )

        final_call_level_sdf.createOrReplaceTempView("CALL_LEVEL_PITCHES_AND_RECS")

    with StepTimer(13, "Collecting final DataFrame from Spark → pandas"):
        result_df = _spark_collect(final_call_level_sdf, "final toPandas")
        print(f"    Final shape: {result_df.shape[0]:,} rows × {result_df.shape[1]} cols")

    return result_df


def save_chunked_csv(df: pd.DataFrame, base_dir: str, base_filename: str, max_bytes: int = 9 * 1024 * 1024) -> list[str]:
    """
    Save a DataFrame to one or more CSV files, each under max_bytes (default 9 MB).
    """
    today_str = 'data'
    out_dir   = os.path.join(base_dir, today_str)
    os.makedirs(out_dir, exist_ok=True)

    # Clear any existing shards so stale files from prior runs don't persist
    existing = glob.glob(os.path.join(out_dir, f"{base_filename}_*.csv"))
    for f in existing:
        os.remove(f)
    if existing:
        print(f"  Cleared {len(existing)} existing shard(s) from {out_dir}", flush=True)

    sample_size  = min(500, len(df))
    sample_csv   = df.iloc[:sample_size].to_csv(index=False)
    header_bytes = len(sample_csv.encode("utf-8").split(b"\n", 1)[0]) + 1
    body_bytes   = len(sample_csv.encode("utf-8")) - header_bytes
    bytes_per_row = body_bytes / sample_size if sample_size else 200
    rows_per_chunk = max(1, int((max_bytes - header_bytes) / bytes_per_row))

    total_rows = len(df)
    n_chunks_est = max(1, -(-total_rows // rows_per_chunk))
    print(f"  Estimated {bytes_per_row:.0f} bytes/row → ~{rows_per_chunk:,} rows/chunk "
          f"(~{n_chunks_est} file{'s' if n_chunks_est != 1 else ''})", flush=True)

    written_files: list[str] = []
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
            tqdm.write(f"  ✔ {file_path}  ({actual_bytes / 1024 / 1024:.2f} MB, {len(chunk_df):,} rows)")
            written_files.append(file_path)
            file_index += 1

    starts = range(0, total_rows, rows_per_chunk)
    for start in tqdm(starts, desc="Writing chunked CSV", unit="chunk", dynamic_ncols=True):
        _write_chunk(df.iloc[start : start + rows_per_chunk])

    print(f"\nTotal files written: {len(written_files)}")
    return written_files


if __name__ == "__main__":
    df = get_data()

    save_chunked_csv(
        df=df,
        base_dir="/Workspace/Users/fnisbet@redventures.com/product-rec-dash",
        base_filename="call_level_data",
        max_bytes=9 * 1024 * 1024,
    )

    print("Done")