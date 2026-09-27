import json
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType

from src.clean import parse_revenue, parse_date, clean_category
from src.companies import match_company

DATA_DIR = os.environ.get("DATA_DIR", ".")
OUT_DIR = os.environ.get("OUT_DIR", "output")


def size_bucket(n):
    if n is None:
        return None
    if n < 10000:
        return "Small"
    if n <= 30000:
        return "Medium"
    return "Large"


def build_tables(spark):
    with open(os.path.join(DATA_DIR, "company_metadata.json"), encoding="utf-8") as f:
        meta = json.load(f)
    known = list(meta.keys())
    comp_rows = [
        (name, m["industry"], m["founded_year"], m["headquarters"],
         m["employee_count"], bool(m["is_public"]), m["stock_ticker"],
         size_bucket(m["employee_count"]))
        for name, m in meta.items()
    ]
    companies = spark.createDataFrame(
        comp_rows,
        ["company_name", "industry", "founded_year", "headquarters",
         "employee_count", "is_public", "stock_ticker", "company_size_category"],
    )


    arr_usd = F.udf(lambda r: parse_revenue(r)[0], LongType())
    arr_status = F.udf(lambda r: parse_revenue(r)[2], StringType())
    date_val = F.udf(lambda d: parse_date(d)[0], StringType())
    date_status = F.udf(lambda d: parse_date(d)[1], StringType())
    category = F.udf(clean_category, StringType())
    resolve = F.udf(lambda c: match_company(c, known)[0], StringType())
    resolve_how = F.udf(lambda c: match_company(c, known)[1], StringType())

    raw = (spark.read.option("header", True).option("multiLine", True)
           .csv(os.path.join(DATA_DIR, "tech_news.csv")))

    clean = (
        raw.dropDuplicates(["article_id"])        
        .withColumn("arr_usd", arr_usd("revenue"))
        .withColumn("arr_status", arr_status("revenue"))
        .withColumn("published_date_clean", F.to_date(date_val("published_date")))
        .withColumn("date_status", date_status("published_date"))
        .withColumn("category_clean", category("category"))
        .withColumn("matched_company", resolve("company_name"))
        .withColumn("match_type", resolve_how("company_name"))
        .withColumn("year", F.year("published_date_clean"))
        .withColumn("quarter", F.quarter("published_date_clean"))
        .withColumn("month", F.month("published_date_clean"))
    )


    clean = clean.join(companies, clean.matched_company == companies.company_name, "left")
    clean = (clean.drop(companies.company_name)
             .withColumn("company_age",
                         F.when(F.col("founded_year").isNotNull(),
                                F.col("year") - F.col("founded_year")))
             .withColumn("is_ai",
                         (F.col("category_clean") == "AI_ML") | (F.col("industry") == "AI/ML")))


    arr = (clean.filter((F.col("arr_status") == "parsed") & F.col("matched_company").isNotNull())
           .select(
               F.col("article_id").alias("observation_id"),
               F.col("article_id").alias("source_article_id"),
               F.col("matched_company").alias("company_name"),
               F.col("published_date_clean").alias("observation_date"),
               "year", "quarter", "arr_usd",
               F.col("revenue").alias("source_revenue_raw")))

    reason = (F.when(F.col("matched_company").isNull(), "unknown_company")
              .when(F.col("arr_status") != "parsed", F.concat(F.lit("revenue_"), F.col("arr_status")))
              .otherwise("ok"))
    rejects = (clean.withColumn("reason", reason).filter(F.col("reason") != "ok")
               .select("article_id", "company_name", "matched_company", "match_type",
                       F.col("revenue").alias("source_revenue_raw"), "arr_status",
                       "published_date", "date_status", "reason"))

    return companies, clean, arr, rejects


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    spark = (SparkSession.builder.master("local[*]").appName("yipit-arr")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    companies, clean, arr, rejects = build_tables(spark)

    companies_pd = companies.toPandas()
    clean_pd = clean.toPandas()
    arr_pd = arr.toPandas()
    rejects_pd = rejects.toPandas()

    from src.export import add_embeddings, ai_export
    clean_pd = add_embeddings(clean_pd)
    ai_pd = ai_export(clean_pd)

    articles_pd = clean_pd.drop(columns=["embedding"])

    for df, name in [(companies_pd, "companies"), (articles_pd, "articles_clean"),
                     (arr_pd, "arr_observations"), (rejects_pd, "rejects"),
                     (ai_pd, "ai_articles_enriched")]:
        path = os.path.join(OUT_DIR, name + ".csv")
        df.to_csv(path, index=False)
        print(f"wrote {name}.csv  ({len(df)} rows)")

    spark.stop()


if __name__ == "__main__":
    main()
