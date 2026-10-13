""" Revenue/ARR cleaning as a Spark UDF"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

REVENUE_STRUCT_SCHEMA =StructType (
	[
		StructField("arr_usd", IntegerType(), True),
		StructField("currency_detected", StringType(), True),
		StructField("is_valid_arr", BooleanType(), True),
		StructField("arr_parse_status", StringType(), True),
	]
)

def _clean_revenue_udf(raw_value):
	result= clean_revenue(raw_value)
	return (result.arr_usd, result.currency_detected, result.is_valid_arr, result.arr_parse_status)

clean_revenue_udf = F.udf(_clean_revenue_udf, REVENUE_STRUCT_SCHEMA)

def with_cleaned_revenue(df, revenue_col: str ="revenue") -> "DataFrame":
    """Adding parsed date-part columns to a DataFrame containing `revenue_col_raw`"""

    raw_col = f"{revenue_col}_raw"
    renamed = df.withColumnRenamed(revenue_col, raw_col)
    parsed = clean_revenue_udf(F.col(raw_col))
    return (
        renamed.withColumn("_parsed_revenue", parsed)
        .select(
            "*",
            F.col("_parsed_revenue.arr_usd").alias("arr_usd"),
            F.col("_parsed_revenue.currency_detected").alias("currency_detected"),
            F.col("_parsed_revenue.is_valid_arr").alias("is_valid_arr"),
            F.col("_parsed_revenue.arr_parse_status").alias("arr_parse_status"),
        )
        .drop("_parsed_revenue")

    )