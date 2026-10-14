"""End-to-end Spark pipeline entrypoint.

Usage:
    python -m spark_pipeline.run_pipeline
    python -m spark_pipeline.run_pipeline --no-embeddings
    python -m spark_pipeline.run_pipeline --persist-duckdb
"""

from __future__ import annotations
import argparse, logging
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, functions as F
# from pipeline import config
# from pipeline.embeddings.duckdb_loader import build_duckdb_connection
# from pipeline.embeddings.embedder import save_embeddings
from pipeline.embeddings.embedder import (
    embeddings_df_to_numpy,
    generate_embeddings_spark,
    with_article_text,
)

from pipeline.embeddings.embedder import build_duckdb_connection


from spark_pipeline.embeddings.similarity import compute_top_similar_per_article
from spark_pipeline.spark_session import get_spark
from spark_pipeline.warehouse.build import run_warehouse_build
from spark_pipeline.warehouse.io import write_table

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("spark_pipeline.run_pipeline")

def main(generate_embeddings_flag: bool = True, persist_duckdb: bool = False) -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Building bronze/silver/gold warehouse tables with Spark...")
    tables = run_warehouse_build(spark)
    silver_df = tables["silver_articles"]
    ai_articles_df = tables["ai_articles_enriched"]

    # --- Counts for logging ---
    silver_count = silver_df.count()
    valid_arr_count = silver_df.filter(F.col("is_valid_arr")).count()
    matched_count = silver_df.filter(F.col("is_company_matched")).count()
    ai_count = ai_articles_df.count()
    logger.info(
        "silver_articles=%d, valid_arr=%d, matched_companies=%d, ai_articles_enriched=%d",
        silver_count, valid_arr_count, matched_count, ai_count,
    )

    # --- Embeddings + similarity ---
    if generate_embeddings_flag:
        logger.info("Generating embeddings for article + summary...")
        text_df = with_article_text(silver_df.select("article_id", "title", "summary"))
        embeddings_df, method = generate_embeddings_spark(spark, text_df)
        embeddings_df = embeddings_df.cache()
        logger.info("Generated embeddings using method=%s", method)

        top_similar_df = compute_top_similar_per_article(
            embeddings_df, top_n=config.TOP_SIMILAR_N
        )
        write_table(
            spark,
            top_similar_df.withColumn(
                "top_similar_articles", F.concat_ws(",", "top_similar_articles")
            ),
            "article_top_similar",
            config.OUTPUT_DIR,
            key_cols=["article_id"],
        )

        # Attach embeddings + similarity
        ai_with_embeddings = (
            ai_articles_df.join(embeddings_df, on="article_id", how="left")
            .withColumn("embedding", F.to_json("embedding"))
            .join(top_similar_df, on="article_id", how="left")
            .withColumn(
                "top_similar_articles", F.coalesce(F.col("top_similar_articles"), F.array())
            )
        )

        # Collect embedding matrix for DuckDB/NumPy search
        article_ids, matrix = embeddings_df_to_numpy(ai_with_embeddings)
        save_embeddings(matrix, article_ids)
    else:
        ai_with_embeddings = (
            ai_articles_df.withColumn("embedding", F.lit(None).cast("string"))
            .withColumn("top_similar_articles", F.lit(None).cast("string"))
        )

    write_table(
        spark, ai_with_embeddings, "ai_articles_enriched",
        config.OUTPUT_DIR, key_cols=["article_id"]
    )
    logger.info(f"Wrote ai_articles_enriched.csv with {ai_with_embeddings.count()} rows")

    # --- Persist DuckDB warehouse ---
    if persist_duckdb:
        logger.info("Persisting DuckDB warehouse to %s", config.DUCKDB_PATH)
        config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        pandas_tables = {
            "bronze_articles": tables["bronze_articles"].toPandas(),
            "silver_articles": silver_df.toPandas(),
            "dim_company": tables["dim_company"].toPandas(),
            "unmatched_companies": tables["unmatched_companies"].toPandas(),
            "fact_arr_observation": tables["fact_arr_observation"].toPandas(),
            "ai_articles_enriched": ai_with_embeddings.toPandas(),
        }
        embeddings_for_duckdb = None
        ids_for_duckdb = None
        if generate_embeddings_flag:
            embeddings_for_duckdb = matrix
            ids_for_duckdb = article_ids

        conn = build_duckdb_connection(
            pandas_tables,
            embeddings=embeddings_for_duckdb,
            article_ids=ids_for_duckdb,
            db_path=config.DUCKDB_PATH,
        )
        conn.close()


    logger.info("Pipeline complete. Outputs written to %s", config.OUTPUT_DIR)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI tech-news ARR pipeline (PySpark)")
    parser.add_argument("--no-embeddings", action="store_true", help="Skip embedding generation")
    parser.add_argument("--persist-duckdb", action="store_true", help="Write a warehouse.duckdb file")
    args = parser.parse_args()
    main(generate_embeddings_flag=not args.no_embeddings, persist_duckdb=args.persist_duckdb)
