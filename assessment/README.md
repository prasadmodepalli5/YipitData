# Tech News ARR Pipeline

Cleans messy tech-news articles into tidy tables of company revenue (ARR) over
time.

## What you need

- Python 3.11
- Java

## Install


python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt


## Run it


python -m src.pipeline


Keep the two source files (tech_news.csv and company_metadata.json") in the
project root. The pipeline reads them from there and writes five CSVs into 'output/'

companies.csv : one row per company
articles_clean.csv : all 750 articles, cleaned up
arr_observations.csv : the good revenue numbers, linked back to their article
rejects.csv : rows we couldn't use, with the reason why
ai_articles_enriched.csv : AI articles from 2022–24 with ARR over $50M
