# Central configuration: paths, FX rates, taxonomies, thresholds.

from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
ARTICLES_CSV = ROOT_DIR / "tech_news.csv"
COMPANY_METADATA_JSON = ROOT_DIR / "company_metadata.json"
OUTPUT_DIR = ROOT_DIR / "output"
DUCKDB_PATH = OUTPUT_DIR / "warehouse.duckdb"
EMBEDDINGS_NPY = OUTPUT_DIR / "article_embeddings.npy"
EMBEDDINGS_IDS_NPY = OUTPUT_DIR / "article_embeddings_ids.npy"

# -----------------------------
# Currency conversion (fixed rates)
# -----------------------------
FX_TO_USD = {
    "USD": 1.0,
    "EUR": 1.1,
    "GBP": 1.27,
    "JPY": 1 / 150,
}

# -----------------------------
# NULL / missing revenue tokens
# -----------------------------
NULL_REVENUE_TOKENS = {
    "", "n/a", "na", "none", "null", "not disclosed", "undisclosed", "tbd"
}

# -----------------------------
# Category taxonomy
# -----------------------------
CATEGORY_TAXONOMY = {
    # AI / ML
    "artificial_intelligence": "AI_ML",
    "ai/ml": "AI_ML",
    "ai ml": "AI_ML",
    "machine learning": "AI_ML",
    # Cloud computing
    "cloud": "CLOUD",
    "cloud services": "CLOUD",
    # Data / Analytics
    "analytics": "DATA_ANALYTICS",
    "data analytics": "DATA_ANALYTICS",
    "big data": "DATA_ANALYTICS",
    # Fintech / Finance
    "financial technology": "FINTECH",
    "fintech": "FINTECH",
    "finance": "FINTECH",
    # Security
    "cybersecurity": "CYBERSECURITY",
    "security": "CYBERSECURITY",
    "infosec": "CYBERSECURITY",
    # Software
    "software": "SOFTWARE",
    "enterprise software": "SOFTWARE",
    "saas": "SOFTWARE",
}
DEFAULT_CATEGORY = "OTHER"

AI_ML_INDUSTRY_VALUES = {
    "ai/ml", "ai ml", "artificial intelligence", "machine learning"
}

# -----------------------------
# Company size thresholds
# -----------------------------
SMALL_MAX_EMPLOYEES = 10_000
MEDIUM_MAX_EMPLOYEES = 30_000
# 0 - 10,000 -> SMALL
# 10,000 - 30,000 -> MEDIUM
# >30,000 -> LARGE

# -----------------------------
# Company alias map
# -----------------------------
COMPANY_ALIASES = {
    "amazon": "Amazon Web Services",
    "amazon web services (aws)": "Amazon Web Services",
    "microsoft azure": "Microsoft",
    "azure": "Microsoft",
    "google deepmind": "Google DeepMind",
    "meta ai research": "Meta AI",
    "facebook ai research": "Meta AI",
    "meta ai": "Meta AI",
    "nvidia": "NVIDIA",
    "nvidia corporation": "NVIDIA",
    "openai": "OpenAI",
    "openai inc.": "OpenAI",
    "deepmind": "DeepMind",
    "databricks inc.": "Databricks",
    "snowflake inc.": "Snowflake",
    "cloudflare": "Cloudflare",
    "cloudflare ltd.": "Cloudflare",
    "data robot": "DataRobot",
    "mongo db": "MongoDB",
    "palantir technologies": "Palantir",
    "the boring company / spacex": "SpaceX",
    "stripe inc.": "Stripe",
}

# -----------------------------
# Fuzzy-match threshold
# -----------------------------
FUZZY_MATCH_THRESHOLD = 0.82

# -----------------------------
# AI article export filters
# -----------------------------
AI_EXPORT_MIN_YEAR = 2022
AI_EXPORT_MAX_YEAR = 2024
AI_EXPORT_MIN_ARR_USD = 50_000_000

# -----------------------------
# Embeddings
# -----------------------------
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
TOP_SIMILAR_N = 3
