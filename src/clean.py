import re
from datetime import date, datetime


FX = {"USD": 1.0, "EUR": 1.1, "GBP": 1.27, "JPY": 1 / 150}
SYMBOL = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"}

SUFFIX = {
    "k": 1e3, "thousand": 1e3,
    "m": 1e6, "mn": 1e6, "million": 1e6,
    "b": 1e9, "bn": 1e9, "billion": 1e9,
    "t": 1e12, "trillion": 1e12,
}

MISSING_REVENUE = {"", "n/a", "na", "none", "null", "not disclosed", "undisclosed", "-"}


CATEGORY = {
    "ai/ml": "AI_ML", "ai & ml": "AI_ML", "artificial intelligence": "AI_ML",
    "machine learning": "AI_ML",
    "cloud computing": "CLOUD", "cloud": "CLOUD", "cloud services": "CLOUD",
    "financial technology": "FINTECH", "fintech": "FINTECH", "finance": "FINTECH",
    "big data": "DATA", "analytics": "DATA", "data analytics": "DATA",
    "software": "SOFTWARE", "enterprise software": "SOFTWARE", "saas": "SOFTWARE",
    "cybersecurity": "SECURITY", "infosec": "SECURITY", "security": "SECURITY",
}

_num = re.compile(r"(\d[\d,]*\.?\d*)\s*(trillion|billion|million|thousand|bn|mn|[ktmb])?", re.I)


def parse_revenue(raw):
    """Return (arr_usd, currency, status). status is parsed / missing / bad."""
    if raw is None:
        return None, None, "missing"
    s = str(raw).strip()
    if s.lower() in MISSING_REVENUE:
        return None, None, "missing"

    currency = "USD"
    for sym, code in SYMBOL.items():
        if sym in s:
            currency = code
            break
    else:
        for code in ("EUR", "GBP", "JPY"):
            if re.search(rf"\b{code}\b", s, re.I):
                currency = code
                break


    parts = [p for p in re.split(r"\s*-\s*", s) if re.search(r"\d", p)]
    values = []
    for p in parts:
        m = _num.search(p)
        if not m:
            return None, None, "bad"
        n = float(m.group(1).replace(",", ""))
        if m.group(2):
            n *= SUFFIX[m.group(2).lower()]
        values.append(n)
    if not values:
        return None, None, "bad"

    usd = int(round(sum(values) / len(values) * FX[currency]))
    return usd, currency, "parsed"


def parse_date(raw):
    """Return (iso_date, status). Handles ISO, US, EU and written formats."""
    if raw is None or str(raw).strip() == "":
        return None, "missing"
    s = str(raw).strip()


# dash date valudation
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)  
    if m:
        return _safe(int(m.group(1)), int(m.group(2)), int(m.group(3)))

# slash date validation
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        return _safe(int(m.group(3)), int(m.group(1)), int(m.group(2)))


    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if b > 12 and a <= 12:    
            d, mo = b, a
        else:                     
            d, mo = a, b
        return _safe(y, mo, d)


    for fmt in ("%d %b %Y", "%B %d, %Y", "%b %d, %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat(), "parsed"
        except ValueError:
            pass
    return None, "bad"


def _safe(y, mo, d):
    try:
        return date(y, mo, d).isoformat(), "parsed"
    except ValueError:
        return None, "bad"


def clean_category(raw):
    if raw is None:
        return "OTHER"
    return CATEGORY.get(re.sub(r"\s+", " ", str(raw).strip().lower()), "OTHER")
