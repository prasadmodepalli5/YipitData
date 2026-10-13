 """ Category standardization """

from __future__ import annotations

from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CATEGORY_TAXONOMY, DEFAULT_CATEGORY

_mapping_items = list(chain.from_iterable((k,v) from k, v in CETEGORY_TAXONOMY.items()))
_CATEGORY_MAP_COL= F.create_map([F.lit(x) for x in _mapping_items])

def with_standardized_category(df: DataFrame, category_col: str ="category") -> DataFrame:
	"""Adding `category_standardized` by looking up `lower(trim(category_col))`."""

	raw_col = f"{category_col}_raw"
	renamed = df.withColumnRenamed(category_col, raw_col)
	lookup_key = F.lower(F.trim(F.col(raw_col)))
	return renamed.withColumn(
		"category_standardized",
		F.coalesce(_CATEGORY_MAP_COL[lookup_key], F.lit(DEFAULT_CATEGORY)),
	)
