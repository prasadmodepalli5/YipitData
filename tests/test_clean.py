from src.clean import parse_revenue, parse_date, clean_category
from src.companies import match_company

KNOWN = ["OpenAI", "Amazon Web Services", "Microsoft", "Snowflake", "DataRobot", "NVIDIA"]


def test_revenue_formats():
    assert parse_revenue("5.2B")[0] == 5_200_000_000
    assert parse_revenue("$5,200,000,000")[0] == 5_200_000_000
    assert parse_revenue("5.2 billion")[0] == 5_200_000_000
    assert parse_revenue("500M USD")[0] == 500_000_000


def test_revenue_currency():
    assert parse_revenue("€2B")[0] == int(2e9 * 1.1)
    assert parse_revenue("£1B")[0] == int(1e9 * 1.27)
    assert parse_revenue("¥15000000000")[0] == int(15e9 / 150)


def test_revenue_range_midpoint():
    assert parse_revenue("$10M - $20M")[0] == 15_000_000


def test_revenue_missing_is_none():
    for v in ["", "N/A", "Not disclosed", None]:
        assert parse_revenue(v) == (None, None, "missing")


def test_dates():
    assert parse_date("2023-12-13")[0] == "2023-12-13"
    assert parse_date("2023-12-13T00:00:00Z")[0] == "2023-12-13"
    assert parse_date("02/23/2023")[0] == "2023-02-23"        # US month-first
    assert parse_date("23-08-2023")[0] == "2023-08-23"        # day-first
    assert parse_date("06-21-2024")[0] == "2024-06-21"        # month-first (21 can't be a month)
    assert parse_date("21 Feb 2020")[0] == "2020-02-21"
    assert parse_date("October 28, 2024")[0] == "2024-10-28"


def test_dates_bad():
    assert parse_date("")[1] == "missing"
    assert parse_date("2023-13-40")[1] == "bad"


def test_categories():
    assert clean_category("Artificial Intelligence") == "AI_ML"
    assert clean_category("AI & ML") == "AI_ML"
    assert clean_category("Cloud Services") == "CLOUD"
    assert clean_category("InfoSec") == "SECURITY"


def test_company_match():
    assert match_company("OpenAI", KNOWN) == ("OpenAI", "exact")
    assert match_company("AWS", KNOWN) == ("Amazon Web Services", "alias")
    assert match_company("Snowflake Inc.", KNOWN)[0] == "Snowflake"
    assert match_company("Cohere", KNOWN) == (None, "unmatched")
