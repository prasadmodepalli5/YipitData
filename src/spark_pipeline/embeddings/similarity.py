"""Spark-native cosine similarity (self join + higher -order array functions)"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Expressions for dot product and norm
DOT_PRODUCT_EXPR = "aggregate(zip_with(e1, e2, (x, y) -> x * y), 0D, (acc, x) -> acc + x)"
NORM_EXPR = "sqrt(aggregate(transform({col}, x -> x * x), 0D, (acc, x) -> acc + x))"

def compute_pairwise_similarity(
    embeddings_df: DataFrame, id_col: str = "article_id", embedding_col: str = "embedding"
) -> DataFrame:
    """Self-join embeddings_df and compute cosine similarity for every pair (a != b)."""
    left = embeddings_df.select(
        F.col(id_col).alias("article_id_a"), F.col(embedding_col).alias("e1")
    )
    right = embeddings_df.select(
        F.col(id_col).alias("article_id_b"), F.col(embedding_col).alias("e2")
    )
    pairs = left.crossJoin(right).filter(F.col("article_id_a") != F.col("article_id_b"))

    return (
        pairs.withColumn("_dot", F.expr(DOT_PRODUCT_EXPR))
        .withColumn("_norm1", F.expr(NORM_EXPR.format(col="e1")))
        .withColumn("_norm2", F.expr(NORM_EXPR.format(col="e2")))
        .withColumn(
            "similarity",
            F.col("_dot") / (F.col("_norm1") * F.col("_norm2") + F.lit(1e-10))
        )
        .select("article_id_a", "article_id_b", "similarity")
    )

def compute_top_similar_per_article(
    embeddings_df: DataFrame, top_n: int = 3
) -> DataFrame:
    """For every article, find the top_n most similar *other* articles -- entirely in Spark."""
    pairwise = compute_pairwise_similarity(embeddings_df)
    window = Window.partitionBy("article_id_a").orderBy(F.desc("similarity"))
    ranked = pairwise.withColumn("_rank", F.row_number().over(window)).filter(F.col("_rank") <= top_n)
    return (
        ranked.groupBy("article_id_a")
        .agg(F.collect_list("article_id_b").alias("top_similar_articles"))
        .withColumnRenamed("article_id_a", "article_id")
    )
