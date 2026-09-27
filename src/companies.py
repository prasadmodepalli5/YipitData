import re
from rapidfuzz import fuzz, process


ALIASES = {
    "aws": "Amazon Web Services",
    "amazon web services aws": "Amazon Web Services",
    "azure": "Microsoft",
    "microsoft azure": "Microsoft",
    "open ai": "OpenAI",
    "deepmind": "Google DeepMind",
    "google deepmind": "Google DeepMind",
    "meta ai research": "Meta AI",
    "facebook ai research": "Meta AI",
    "data robot": "DataRobot",
    "mongo db": "MongoDB",
    "the boring company spacex": "SpaceX",
}


def _norm(name):
    s = (name or "").lower()
    s = re.sub(r"\b(inc|corp|corporation|technologies|ltd|llc)\b", " ", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def match_company(name, known_names):
    """Return (canonical_name_or_None, how). Tries exact, alias, then fuzzy."""
    n = _norm(name)
    if not n:
        return None, "unmatched"

    exact = {_norm(k): k for k in known_names}
    if n in exact:
        return exact[n], "exact"
    if n in ALIASES:
        return ALIASES[n], "alias"

    best, score, _ = process.extractOne(n, exact.keys(), scorer=fuzz.token_sort_ratio)
    if score >= 90:
        return exact[best], "fuzzy"
    return None, "unmatched"
