from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, List
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, StructType, StructField

from pipeline.config import SMALL_MAX_EMPLOYEES, MEDIUM_MAX_EMPLOYEES, COMPANY_METADATA_JSON, ARTICLES_CSV, OUTPUT_DIR
from pipeline.cleaning.revenue import with_cleaned_revenue
from pipeline.cleaning.dates import with_cleaned_date
from pipeline.cleaning.categories import with_standardized_category
from pipeline.enrichment.company_matcher import match_company

def load_company_metadata_dict(path: Path = COMPANY_METADATA_JSON) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def load_bronze_articles(
    spark: SparkSession, path: Path = ARTICLES_CSV, batch_id: Optional[str] = None
) -> DataFrame:
    batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df = (
        spark.read.option("header", "true")
        .option("multiline", "true")
        .option("escape", '"')
        .csv(str(path))
    )
    return (
        df.withColumn("batch_id", F.lit(batch_id))
          .withColumn("source_file", F.lit(path.name))
          .withColumn("ingested_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )

def build_dim_company(spark: SparkSession, company_metadata: dict) -> DataFrame:
    rows = [
        {
            "company_key": name,
            "company_name": name,
            "industry": meta.get("industry"),
            "founded_year": meta.get("founded_year"),
            "headquarters": meta.get("headquarters"),
            "employee_count": meta.get("employee_count"),
            "is_public": meta.get("is_public"),
            "stock_ticker": meta.get("stock_ticker"),
        }
        for name, meta in company_metadata.items()
    ]
    df = spark.createDataFrame(rows)
    return with_company_size_category(df, "employee_count")

def build_silver_articles(spark: SparkSession, bronze_df: DataFrame, company_metadata: dict) -> DataFrame:
    canonical_names = list(company_metadata.keys())
    df = with_cleaned_revenue(bronze_df, "revenue")
    df = with_cleaned_date(df, "published_date")
    df = with_standardized_category(df, "category")
    df = with_resolved_company(df, canonical_names, spark, "company_name")

    return df.select(
        "article_id", "title", "company_name_raw", "company_name_resolved",
        "company_match_method", "company_match_score", "is_company_matched",
        "published_date_raw", "published_date", "year", "quarter", "month", "date_parse_status",
        "category_raw", "category_standardized",
        "revenue_raw", "arr_usd", "currency_detected", "arr_parse_status", "is_valid_arr",
        "summary", "url", "author", "word_count",
        "batch_id", "source_file", "ingested_at"
    )

def build_unmatched_companies(silver_df: DataFrame) -> DataFrame:
    return (silver_df.filter(~F.col("is_company_matched"))
        .groupBy("company_name_raw")
        .agg(
            F.count("*").alias("article_count"),
            F.concat_ws(",", F.slice(F.collect_list("article_id"), 1, 5)).alias("sample_article_ids"),
        )
        .orderBy(F.desc("article_count")))

def build_fact_arr_observation(silver_df: DataFrame, dim_company_df: DataFrame) -> DataFrame:
    valid = silver_df.filter(F.col("is_valid_arr"))
    joined = valid.join(
        F.broadcast(dim_company_df.select("company_key","founded_year","company_size_category")),
        valid["company_name_resolved"] == F.col("company_key"), "left"
    )
    joined = with_company_age(joined, "founded_year","published_date","company_age_at_observation")
    joined = joined.withColumnRenamed("company_size_category","company_size_category_at_observation")
    return joined.select(
        F.col("article_id").alias("arr_observation_id"),
        "article_id","company_name_resolved","company_name_raw",
        F.col("published_date").alias("observation_date"),
        "year","quarter","month","arr_usd",
        F.col("currency_detected").alias("currency_original"),
        F.col("revenue_usd").alias("revenue_usd_original"),
    )

def build_ai_articles_enriched(silver_df: DataFrame, dim_company_df: DataFrame) -> DataFrame:
     dim = dim_company_df.drop("company_name")
     merged = silver_df.join(F.broadcast(dim),
         silver_df["company_name_resolved"] == dim["company_key"], "left")
     merged = with_company_age(merged,"founded_year","published_date","company_age")
     industry_is_ai = F.lower(F.coalesce(F.col("industry"),F.lit(""))).isin(list(config.AI_ML_INDUSTRY_VALUES))
     category_is_ai = F.col("category_standardized") == "AI_ML"
     in_year_range = F.col("year").between(config.AI_EXPORT_MIN_YEAR, config.AI_EXPORT_MAX_YEAR)
     has_valid_arr = F.col("is_valid_arr") & (F.col("arr_usd") > config.AI_EXPORT_MIN_ARR_USD)
     filtered = merged.filter((category_is_ai | industry_is_ai) & in_year_range & has_valid_arr)
     return filtered.select(
         "article_id","title",F.col("company_name_resolved").alias("company_name"),
         "published_date",F.col("category_standardized").alias("category"),
         "arr_usd","summary","url","industry","founded_year","headquarters",
         "employee_count","is_public","stock_ticker","company_age","company_size_category"
     )

def run_warehouse_build(spark: SparkSession, output_dir: Path = config.OUTPUT_DIR) -> dict:
    """End-to-end bronze -> silver -> gold build. Returns {table_name: DataFrame}."""

    # --- Load metadata + bronze layer ---
    company_metadata = load_company_metadata_dict()
    bronze_df = load_bronze_articles(spark)

    # --- Silver layer ---
    silver_df = build_silver_articles(spark, bronze_df, company_metadata).cache()

    # --- Gold layer ---
    dim_company_df = build_dim_company(spark, company_metadata).cache()
    unmatched_df = build_unmatched_companies(silver_df, dim_company_df)
    fact_df = build_fact_ar_observation(silver_df, dim_company_df)
    ai_articles_df = build_ai_articles_enriched(silver_df, dim_company_df)

    # --- Write outputs ---
    write_table(spark, bronze_df, "bronze_articles", output_dir, key_cols=["article_id"])
    write_table(spark, silver_df, "silver_articles", output_dir, key_cols=["article_id"])
    write_table(spark, dim_company_df, "dim_company", output_dir, key_cols=["company_key"])
    write_table(spark, unmatched_df, "unmatched_companies", output_dir)
    write_table(spark, fact_df, "fact_ar_observation", output_dir, key_cols=["ar_observation_id"])
    # ai_articles_enriched.csv is written later by run_pipeline.py, after embeddings are attached.

    return {
        "bronze_articles": bronze_df,
        "silver_articles": silver_df,
        "dim_company": dim_company_df,
        "unmatched_companies": unmatched_df,
        "fact_ar_observation": fact_df,
        "ai_articles_enriched": ai_articles_df,
    }
