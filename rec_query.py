import json
from typing import Dict, Any, List, Optional
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import Window
import os
from datetime import date, timedelta
from databricks.connect import DatabricksSession

def get_data():

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

    TARGET_CENTER_LOCATIONS = ["Durban", "Jamaica", "Charlotte"]

    SILVER_POINTS_THRESHOLD = 25.0

    # -----------------------------
    # JSON parser helpers (rank model)
    # -----------------------------

    def _norm_plan_type_key(k: Optional[str]) -> Optional[str]:
        if not k:
            return None
        s = str(k).strip().lower()
        if "fixed" in s:
            return "Fixed"
        if "tier" in s:
            return "Tiered"
        if "bund" in s:
            return "Bundled"
        if "low" in s:
            return "Low"
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

    # -----------------------------
    # JSON parser (rank model) -> pandas
    # -----------------------------

    def parse_rank_payload_for_etl(payload: str) -> Dict[str, object]:
        out = {
            "product_category_1_plan_category": None, "product_category_1_product_name_1": None,
            "product_category_1_product_name_2": None, "product_category_2_plan_category": None,
            "product_category_2_product_name_1": None, "product_category_2_product_name_2": None,
            "product_category_3_plan_category": None, "product_category_3_product_name_1": None,
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
        out = []
        for p, t in zip(products, types):
            if p:
                out.append(t)
        return out

    # -----------------------------
    # Utility
    # -----------------------------

    def pick_first_existing_column(df, candidates):
        cols = set(df.columns)
        for c in candidates:
            if c in cols:
                return c
        return None

    # -----------------------------
    # 0) Read pitch enriched data
    # -----------------------------

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

    # -----------------------------
    # 0A) Center locations from v_agent_calls
    # -----------------------------

    vac_locations_sdf = (
        spark.read.table(V_AGENT_CALLS_TABLE)
        .select("call_id", "center_location")
        .where(F.col("call_id").isNotNull())
        .where(F.col("center_location").isin(TARGET_CENTER_LOCATIONS))
        .dropDuplicates(["call_id"])
    )

    # -----------------------------
    # 1) Arcadia call ids filtered by session_start_date + center_location
    #    -- no longer used as a semi-join filter; converted to a flag
    # -----------------------------

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

    # Flag: True if this call_id is in the arcadia target set
    in_arcadia_target_sdf = arcadia_target_call_ids_sdf.withColumn("in_arcadia_target", F.lit(True))

    # Left join instead of left_semi so we keep all calls and can flag non-target ones
    pitch_arcadia_target_sdf = (
        pitch_enriched_sdf
        .join(in_arcadia_target_sdf, on="call_id", how="left")
        .withColumn("in_arcadia_target", F.coalesce(F.col("in_arcadia_target"), F.lit(False)))
    )

    # -----------------------------
    # 1A) Arcadia call-level attrs incl. objection_reason
    # -----------------------------

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
    # 1B) Flag calls that FAIL qualification for TXU / TriEagle
    #     -- no longer used as a left_anti filter; converted to a flag
    # -----------------------------

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

    # Flag: True if this call_id failed qualification
    failed_qual_flag_sdf = failed_qual_call_ids_sdf.withColumn("failed_qualification", F.lit(True))

    # Left join instead of left_anti so we keep all calls and can flag failed ones
    pitch_arcadia_target_sdf = (
        pitch_arcadia_target_sdf
        .join(failed_qual_flag_sdf, on="call_id", how="left")
        .withColumn("failed_qualification", F.coalesce(F.col("failed_qualification"), F.lit(False)))
    )

    # -----------------------------
    # 2) Call-level ordered pitches
    #    -- carry in_arcadia_target and failed_qualification through the groupBy
    # -----------------------------

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
        .withColumn("first_pitch",                    F.element_at(F.col("pitches_in_order"), 1))
        .withColumn("first_pitch_canonical",          F.element_at(F.col("pitches_canonical_in_order"), 1))
        .withColumn("first_pitch_plan_category",      F.element_at(F.col("pitches_plan_category_in_order"), 1))
        .drop("pitches_struct")
    )

    # -----------------------------
    # 3) Rank model outputs -> latest per call_id
    # -----------------------------

    rank_sdf = (
        spark.read.table(RAW_MODEL_EVALUATED_TABLE)
        .where(F.col("modelFieldName").ilike("agent-assist-product-rank"))
        .select("correlationId", "_timeStamp", "outputValueString")
        .dropna(subset=["correlationId", "outputValueString"])
    )

    rank_pdf = rank_sdf.toPandas()

    if not rank_pdf.empty:
        rank_pdf = (
            rank_pdf.sort_values(["correlationId", "_timeStamp"], ascending=[True, False])
            .drop_duplicates(subset=["correlationId"], keep="first")
            .reset_index(drop=True)
        )

    parsed_rank_rows = []
    for _, row in rank_pdf.iterrows():
        parsed = parse_rank_payload_for_etl(row["outputValueString"])
        parsed["call_id"] = row["correlationId"]

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
        # NOTE: the 'Low' recommendation filter has been removed here and replaced
        # with has_low_rec flag column downstream
        .drop("recommended_4_in_order")
    )

    # -----------------------------
    # 3B) Element-view flags per call
    # -----------------------------

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
    # 4) Agent metadata from v_agent_calls / rpt_agent_calls
    # -----------------------------

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

    # -----------------------------
    # 4B) Points per call
    # -----------------------------

    points_by_call_sdf = (
        spark.read.table(ORDER_POINTS_TABLE)
        .select("call_id", "points")
        .where(F.col("call_id").isNotNull())
        .where(F.col("points").isNotNull())
        .groupBy("call_id")
        .agg(F.sum(F.col("points").cast("double")).alias("points"))
    )

    # -----------------------------
    # 4C) GCV per call from v_orders — using gcv_v2 directly
    # -----------------------------

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
        .select(
            "call_id",
            F.col(gcv_v2_col).alias("gcv_v2"),
        )
        .where(F.col("call_id").isNotNull())
        .where(F.col("gcv_v2").isNotNull())
        .withColumn("gcv_row", F.col("gcv_v2").cast("double"))
        .groupBy("call_id")
        .agg(F.sum("gcv_row").alias("gcv"))
    )

    # -----------------------------
    # 4D) First-pitch plan points lookup
    # -----------------------------

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

    # -----------------------------
    # 4E) v_calls attrs: site_serp, marketing_bucket, mover_switcher, talk_time_minutes
    # -----------------------------

    v_calls_attrs_sdf = (
        spark.read.table(V_CALLS_TABLE)
        .select("call_id", "web_session_id", "ivr_split_name", "mover_switcher", "talk_time_minutes")
        .where(F.col("call_id").isNotNull())
        .dropDuplicates(["call_id"])
        .withColumn(
            "site_serp",
            F.when(F.col("web_session_id").isNull(), F.lit("SERP"))
            .otherwise(F.lit("Site"))
        )
        .withColumn(
            "marketing_bucket",
            F.when(
                F.col("ivr_split_name").isin("natural_marketingbucket", "natural_marketingbucket_serp"),
                F.lit("Natural")
            ).when(
                F.col("ivr_split_name").isin("brandpartner_marketingbucket", "brandpartner_marketingbucket_serp"),
                F.lit("Brand-Partner")
            ).when(
                F.col("ivr_split_name").isin("generic_marketingbucket", "generic_marketingbucket_serp"),
                F.lit("Generic")
            ).when(
                F.col("ivr_split_name").isin("aggregator_marketingbucket", "aggregator_marketingbucket_serp"),
                F.lit("Aggregator")
            ).when(
                F.col("ivr_split_name").isin("competitor_marketingbucket", "competitor_marketingbucket_serp"),
                F.lit("Competitor")
            ).when(
                F.col("ivr_split_name").isin("dereg_utility_check", "dereg_utility_check_serp"),
                F.lit("Utility")
            ).when(
                F.col("ivr_split_name").isin("pmax_marketingbucket", "pmax_marketingbucket_serp"),
                F.lit("PMax")
            ).when(
                F.col("ivr_split_name").isin("nrg_bucket", "nrg_bucket_serp"),
                F.lit("NRG")
            ).otherwise(F.lit("Other Bucket"))
        )
        .select("call_id", "site_serp", "marketing_bucket", "mover_switcher", "talk_time_minutes")
    )

    # -----------------------------
    # 5) Final join + derived columns
    # -----------------------------

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
        # Keep only calls that have a recommendation — this filter is intentionally retained
        .where(F.col("recommended_in_order").isNotNull() & (F.size(F.col("recommended_in_order")) > 0))

        # ── Exclusion audit flag: payless pitch ───────────────────────────────────
        # Previously excluded via: .where(~F.expr("exists(pitches_canonical_in_order, ...)"))
        .withColumn("has_payless_pitch",
            F.expr("exists(pitches_canonical_in_order, x -> x is not null and x like '%payless%')")
        )

        # ── Exclusion audit flag: low deposit recommendation ──────────────────────
        # Previously excluded via: .where() filter in rank_with_lists_sdf
        .withColumn("has_low_rec",
            F.expr("""
                exists(
                    recommended_plan_types_in_order,
                    x -> x is not null and lower(trim(x)) = 'low'
                )
            """)
        )

        # in_arcadia_target and failed_qualification were joined in during sections 1 and 1B

        # ── Triplet cleaning (unchanged) ─────────────────────────────────────────
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

        .withColumn("points", F.coalesce(F.col("points"), F.lit(0.0)))
        .withColumn("gcv",    F.coalesce(F.col("gcv"),    F.lit(0.0)))
        .withColumn("has_top_rec_pitch_view",    F.coalesce(F.col("has_top_rec_pitch_view"),    F.lit(False)))
        .withColumn("has_slide_recs_pitch_view", F.coalesce(F.col("has_slide_recs_pitch_view"), F.lit(False)))
        .withColumn("has_all_plans_pitch_view",  F.coalesce(F.col("has_all_plans_pitch_view"),  F.lit(False)))

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
        .withColumn(
            "sale_type",
            F.when(
                (F.col("order_count") > 0) & (F.size(F.col("pitch_types_in_order")) > 0),
                F.element_at(F.col("pitch_types_in_order"), -1)
            ).otherwise(F.lit(None))
        )

        # ── happy_path: 1 if call passed ALL exclusion criteria, 0 otherwise ─────
        # Conditions that set happy_path = 0:
        #   in_arcadia_target = False  -> call not in target center/date window
        #   failed_qualification = True -> failed qual for TXU Energy or TriEagle Energy
        #   has_payless_pitch = True   -> pitched a Payless product
        #   has_low_rec = True         -> model recommended a 'Low' deposit plan type
        .withColumn("happy_path",
            F.when(
                F.col("in_arcadia_target")      &
                ~F.col("failed_qualification")  &
                ~F.col("has_payless_pitch")     &
                ~F.col("has_low_rec"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    # ── First select (before agent perf join) ─────────────────────────────────────
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
        "classification_bucket", "first_pitch_type", "pitch_types_in_order", "sale_type",
        "first_pitch_plan_points",
        # ── exclusion audit columns ───────────────────────────────────────────────
        "in_arcadia_target",      # False = not in target center location or date window
        "failed_qualification",   # True  = failed qual for TXU Energy or TriEagle Energy
        "has_payless_pitch",      # True  = had a Payless product in the pitch list
        "has_low_rec",            # True  = model recommended a 'Low' deposit plan type
        "happy_path",             # 1 = passed all exclusions; 0 = excluded by >=1 reason
    )

    # -----------------------------
    # Agent-level performance
    # -----------------------------

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

    # ── Final select (after agent perf join) ──────────────────────────────────────
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
        "classification_bucket", "first_pitch_type", "pitch_types_in_order", "sale_type",
        "first_pitch_plan_points",
        # ── exclusion audit columns ───────────────────────────────────────────────
        "in_arcadia_target",      # False = not in target center location or date window
        "failed_qualification",   # True  = failed qual for TXU Energy or TriEagle Energy
        "has_payless_pitch",      # True  = had a Payless product in the pitch list
        "has_low_rec",            # True  = model recommended a 'Low' deposit plan type
        "happy_path",             # 1 = passed all exclusions; 0 = excluded by >=1 reason
    )

    final_call_level_sdf.createOrReplaceTempView("CALL_LEVEL_PITCHES_AND_RECS")

    return final_call_level_sdf.toPandas()

if __name__ == "__main__":
    df = get_data()
    df.to_csv("/Workspace/Users/fnisbet@redventures.com/product-rec-dash/call_level_data.csv", index=False)
    print("Done")


