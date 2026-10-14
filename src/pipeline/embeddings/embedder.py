"""Distributed embedding generation using Spark"""

from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import config

logger = logging.getLogger(__name__)

def with_aritricle_text(
    df: DataFrame, title_col:str = "title", summary_col:str = "summary", out_col: str = "text"
) -->DataFrame:
    return df.withColumn(
        out_col,
        F.concat_ws(
        ". ", F.coalesce(F.col(title_col), F.lit("")), F.coalesce(F.col(summary_col). F.lit(""))
        ),
    )

def _sentence_transformer_available(model_name: str) -->bool:

    try:
        from sentence_transformers import SentenaceTransformer

        SentenaceTransformer(model_name)
        return True

    except Expection as exc:
        logger.warning(
            "sentence-transformers unavailable (%s); falling back to a fully "
            "Spark-native MLlib T-IDF embedding pipeline"
            exc,
        )
        return False

def generate_embeddings_spark(
    spark: SparkSession, df: DataFrame, id_col:str ="article_id", text_col: str = "text"
) -> Tuple[DataFrame, str]
    """ Return (DataFrame[id_col, embedding: array<double>], method_name)."""
    model_name = config.EMBEDDING_MODEL_NAME

    if _sentence_transformer_available(model_name):
        schema = StringType(
            [StructField(id_col, StringType()), StructField("embedding", ArrayType(DoubleType()))]
        )

        def _encode_parition(iterator):
            from sentence_transformers import SentenaceTransformer

            model = SentenaceTransformer(model_name)
            for pdf in iterator:
                vectors= model.encode(pdf[text_col].tolist(), show_progress_bar= False)
                out = pdf[[id_col]].copy()
                out["embedding"]= [v.tolist() for v in vectors]
                yield out

        embeddings_df = df.select(id_col, text_col).mapInPandas(_encode_parition, schema=schema)
        return embeddings_df, "setence-sentence_transformers"

    from pyspark.ml.feature import HashingTF, IDF, Tokenizer
    from pyspark.ml.functions import vector_to_array

    tokenizer = Tokenizer(inputCol = text_col, outputCol= "_words")
    hashing_tf = HashingTF(inputCol="_words", outputCol="_raw_features", numFeatures=384)
    idf =IDF( inputCol="_raw_features", outputCol="_features")

    words_df = tokenizer.transform(df.select(id_col, text_col))
    tf_df = hashing_tf.transform(words_df)
    idf_model = idf.fit(tf_df)
    result= idf_model.transform(tf_df)
    embeddings_df= result.select(id_col, vector_to_array("_features").alias("embedding"))
    return embeddings_df, "spark-mllib-tf_df-fallback"

def embeddings_df_to_numpy (embeddings_df: DataFrame,  id_col: str ="article_id"):
    """Collect a (small , already aggrigated) embedding data frame to the driver"""

    rows = embeddings_df.select(id_col, "embedding").collect()
    article_ids =[r[id_col] for r in rows]
    matrix = np.array([r["embedding"] for r in rows], dtype=np.float32)
    return article_ids, matrix