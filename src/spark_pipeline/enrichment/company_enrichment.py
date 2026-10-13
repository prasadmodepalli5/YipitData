from __future__ import annotations
from pyspark.sql import DataFrame, functions as F

from pipeline.config import SMALL_MAX_EMPLOYEES, MEDIUM_MAX_EMPLOYEES


def with_company_size_category(
    df: DataFrame, employee_count_col: str = "employee_count"
) -> DataFrame:
    """Add company size category (SMALL, MEDIUM, LARGE) based on employee count."""
    return df.withColumn(
        "company_size_category",
        F.when(F.col(employee_count_col).isNull(), None)
         .when(F.col(employee_count_col) < F.lit(SMALL_MAX_EMPLOYEES), F.lit("SMALL"))
         .when(F.col(employee_count_col) < F.lit(MEDIUM_MAX_EMPLOYEES), F.lit("MEDIUM"))
         .otherwise(F.lit("LARGE"))
    )

def with_company_age(
    df: DataFrame,
    founded_year_col: str = "founded_year",
    observation_date_col: str = "published_date",
    out_col: str = "company_age"
) -> DataFrame:
    """Compute company age in years based on founded year and observation date."""
    age = F.year(F.to_date(F.col(observation_date_col))) - F.col(founded_year_col)
    return df.withColumn(
        out_col,
        F.when(F.col(founded_year_col).isNull() | F.col(observation_date_col).isNull(), None)
         .when(age < 0, None)
         .otherwise(age)
    )
