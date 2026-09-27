"""Builds the required ai_articles_enriched.csv.

That CSV must include an `embedding` column, so for each article we turn its
title + summary into a vector of numbers using a small text model. That's the
only reason vectors appear here. (Full semantic search was the optional bonus
and is not implemented.)
"""

import json
import os

import numpy as np
from sentence_transformers import SentenceTransformer

OUT_DIR = os.environ.get("OUT_DIR", "output")

AI_COLUMNS = [
    "article_id", "title", "company_name", "published_date", "category",
    "arr_usd", "summary", "url", "industry", "founded_year", "headquarters",
    "employee_count", "is_public", "stock_ticker", "company_age",
    "company_size_category", "embedding",
]


def add_embeddings(df):
    """Add an `embedding` column (from title + summary) for each article."""
    df = df.sort_values("article_id").reset_index(drop=True)
    text = (df["title"].fillna("") + ". " + df["summary"].fillna("")).tolist()

    model = SentenceTransformer("all-MiniLM-L6-v2")
    vecs = model.encode(text, normalize_embeddings=True)

    df["embedding"] = [json.dumps(np.round(v, 6).tolist()) for v in vecs]
    return df


def ai_export(df):
    """AI articles, 2022-2024, with a real ARR above $50M."""
    m = (df["is_ai"].fillna(False)
         & (df["arr_status"] == "parsed")
         & df["matched_company"].notna()
         & df["arr_usd"].notna()
         & (df["arr_usd"] > 50_000_000)
         & df["year"].between(2022, 2024))
    out = df[m].copy()
    out["company_name"] = out["matched_company"]
    out["category"] = out["category_clean"]
    out["published_date"] = out["published_date_clean"]
    return out[AI_COLUMNS].sort_values("article_id").reset_index(drop=True)
