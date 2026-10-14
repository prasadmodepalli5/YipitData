from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from pipeline.enrichment.company_matcher import match_company

# Schema for the match result
MATCH_STRUCT_SCHEMA = StructType([
    StructField("company_name_resolved", StringType(), True),
    StructField("company_match_method", StringType(), True),
    StructField("company_match_score", DoubleType(), True)
])

def with_resolved_company(
    df: DataFrame,
    canonical_names: list[str],
    spark: SparkSession,
    raw_col: str = "company_name"
) -> DataFrame:
    """
    Resolve `raw_col` to a canonical company name using exact/alias/fuzzy matching.
    `canonical_names` is broadcast once and reused by every executor/task.
    """
    broadcast_names = spark.sparkContext.broadcast(canonical_names)

    def _match_udf(raw_name):
        result = match_company(raw_name, broadcast_names.value)
        return (result.resolved_name, result.match_method, result.match_score)

    match_udf = F.udf(_match_udf, MATCH_STRUCT_SCHEMA)

    return (
        df.withColumnRenamed(raw_col, "company_name_raw")
          .withColumn("_match", match_udf(F.col("company_name_raw")))
          .select(
              "*",
              F.col("_match.company_name_resolved").alias("company_name_resolved"),
              F.col("_match.company_match_method").alias("company_match_method"),
              F.col("_match.company_match_score").alias("company_match_score"),
          )
          .drop("_match")
          .withColumn("is_company_matched", F.col("company_name_resolved").isNotNull())
    )
