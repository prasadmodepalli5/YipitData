"""
SparkSession factory for the pipeline.
Centralizing session creation here means every module/test gets a
consistently-configured local Spark session, and makes it trivial to show
counterparts how to point this at a real cluster later (just change
...master()).
"""

from __future__ import annotations
from pyspark.sql import SparkSession

def get_spark(app_name: str = "vdpdatas-tech-news-anr-pipeline") -> SparkSession:
    """Create (or fetch) a local SparkSession.

    `local[*]` uses all available cores on this machine — for a 750-row
    dataset this is purely for demonstration; the same code runs unchanged
    against `spark://...` or a YARN/Kubernetes master in production.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small local dataset -> few shuffle partitions
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
