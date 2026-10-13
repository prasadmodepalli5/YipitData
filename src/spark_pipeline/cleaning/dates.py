""" Published-date cleaning as a Spark UDF"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

DATE_STRUCT_SCHEMA =StructType (
	[
		StructField("published_date", StringType(), True),
		StructField("year", IntegerType(), True),
		StructField("quarter", IntegerType(), True),
		StructField("month", IntegerType(), True),
		StructField("date_parse_status", StringType(), True),
	]
)

def _parse_date_udf(raw_value):
	result= parse_published_date(raw_value)
	date_str= result.published_date.date().isoformat() if result.published_date else None
	return (date_str, result.year, result.quarter, result.month, result.parse_status)

def with_cleaned_date(df, date_col: str ="published_date"):
    """Adding parsed date-part columns to a DataFrame containing `datecol`"""

    raw_col = f"{date_col}_raw"
    renamed = df.withColumnRenamed(date_col, raw_col)
    parsed = parse_date_udf(F.col(raw_col))
    return (
        renamed.withColumn("_parsed_date", parsed)
        .select(
            "*",
            F.col("_parsed_date.published_date").alias(date_col),
            F.col("_parsed_date.year").alias("year"),
            F.col("_parsed_date.quarter").alias("quarter"),
            F.col("_parsed_date.month").alias("month"),
            F.col("_parsed_date.date_parse_status").alias("date_parse_status"),
        )
        .drop("_parsed_date")
    )