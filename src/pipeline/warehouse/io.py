from __future__ import annotations
import glob, os, shutil
from pathlib import Path
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

# -----------------------------
# Utility: write DataFrame to single CSV
# -----------------------------
def write_single_csv(df: DataFrame, output_path: Path) -> Path:
    tmp_dir = output_path.parent / f"tmp_{output_path.stem}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    df.write.option("header", "true").csv(str(tmp_dir))

    part_files = list(tmp_dir.glob("part-*"))
    if not part_files:
        raise RuntimeError("No part files written")

    if output_path.exists():
        output_path.unlink()
    shutil.move(str(part_files[0]), str(output_path))
    shutil.rmtree(tmp_dir)
    return output_path

# -----------------------------
# Merge logic: upsert by key
# -----------------------------
def merge_on_key(
    spark: SparkSession,
    new_df: DataFrame,
    existing_csv_path: Path,
    key_cols: List[str],
    order_col: str = "ingested_at",
) -> DataFrame:
    """Upsert new_df into existing CSV keyed by key_cols.
    Keeps latest row per key using order_col.
    """
    if order_col not in new_df.columns:
        new_df = new_df.withColumn(order_col, F.current_timestamp().cast("string"))

    if existing_csv_path.exists():
        existing_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(str(existing_csv_path))
        )
        # Align schema: cast existing CSV columns to new_df types
        existing_df = existing_df.select(
            *[F.col(c).cast(new_df.schema[c].dataType) for c in new_df.columns]
        )
        combined = existing_df.unionByName(new_df)
    else:
        combined = new_df

    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    deduped = (
        combined.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return deduped

# -----------------------------
# Write table with optional merge
# -----------------------------
def write_table(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    output_dir: Path,
    key_cols: Optional[List[str]] = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"
    if key_cols is not None:
        df = merge_on_key(spark, df, path, key_cols)
    return write_single_csv(df, path)
