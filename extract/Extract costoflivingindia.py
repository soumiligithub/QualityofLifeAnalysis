"""
extract_costoflivingindia.py
-----------------------------
Scrapes ALL 9 expenditure categories from costoflivingindia.com
for your 20 QoL project cities.

Categories scraped:
  1. Restaurants & Dining     → col_restaurants.csv
  2. Groceries                → col_groceries.csv
  3. Transportation           → col_transport.csv
  4. Utilities (Monthly)      → col_utilities.csv
  5. Accommodation - Rent     → col_rent.csv
  6. PG / Shared Accommodation→ col_pg.csv
  7. Household Help & Misc    → col_household.csv
  8. Shopping & Online        → col_shopping.csv
  9. Lifestyle & Entertainment→ col_lifestyle.csv
  ALL merged                  → col_all.csv

Run:
  pip install requests beautifulsoup4 pandas
  py extract_costoflivingindia.py
"""

import re
import time
import logging
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── 20 cities → URL slugs ──────────────────────────────────────────────────────
CITIES = {
    "Mumbai":        "mumbai",
    "Delhi":         "delhi",
    "Bangalore":     "bangalore",
    "Hyderabad":     "hyderabad",
    "Chennai":       "chennai",
    "Kolkata":       "kolkata",
    "Pune":          "pune",
    "Ahmedabad":     "ahmedabad",
    "Jaipur":        "jaipur",
    "Surat":         "surat",
    "Lucknow":       "lucknow",
    "Kanpur":        "kanpur",
    "Nagpur":        "nagpur",
    "Indore":        "indore",
    "Bhopal":        "bhopal",
    "Visakhapatnam": "visakhapatnam",
    "Patna":         "patna",
    "Vadodara":      "vadodara",
    "Ludhiana":      "ludhiana",
    "Coimbatore":    "coimbatore",
}

# Category heading (partial match) → short key used as col prefix & filename
CATEGORY_MAP = {
    "Restaurants & Dining":       "restaurants",
    "Groceries":                  "groceries",
    "Transportation":             "transport",
    "Utilities":                  "utilities",
    "Accommodation - Rent":       "rent",
    "PG / Shared Accommodation":  "pg",
    "Household Help":             "household",
    "Shopping & Online":          "shopping",
    "Lifestyle & Entertainment":  "lifestyle",
}

# Column prefix per category key
CAT_PREFIX = {
    "restaurants": "rest",
    "groceries":   "groc",
    "transport":   "trans",
    "utilities":   "util",
    "rent":        "rent",
    "pg":          "pg",
    "household":   "hh",
    "shopping":    "shop",
    "lifestyle":   "life",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

BASE_URL = "https://costoflivingindia.com/cost-of-living/{slug}/prices"


# ── Helpers ────────────────────────────────────────────────────────────────────
def parse_price(text: str) -> float | None:
    """
    Handles:
      ₹1,500      → 1500.0
      ₹1.1L       → 110000.0   (lakh)
      ₹2.5Cr      → 25000000.0 (crore)
      105/L       → 105.0
    """
    text = text.strip().replace(",", "")
    text = re.sub(r"[₹\u20b9]", "", text)
    text = re.sub(
        r"/(L|litre|kg|month|ride|piece|pcs|cut|pint|bottle|ticket|cylinder|pair|meal|cup|plate)",
        "", text, flags=re.IGNORECASE
    )

    lakh = re.match(r"^([\d.]+)[Ll]$", text.strip())
    if lakh:
        return float(lakh.group(1)) * 100_000

    crore = re.match(r"^([\d.]+)[Cc]r$", text.strip())
    if crore:
        return float(crore.group(1)) * 10_000_000

    match = re.search(r"\d+\.?\d*", text)
    return float(match.group()) if match else None


def to_col(item_name: str, prefix: str) -> str:
    """'Veg Thali (local restaurant)', 'rest' → 'rest_veg_thali_local_restaurant'"""
    clean = re.sub(r"[^a-z0-9 ]", "", item_name.lower())
    clean = re.sub(r"\s+", "_", clean.strip())
    return f"{prefix}_{clean}"


# ── Scraper ────────────────────────────────────────────────────────────────────
def scrape_city(city: str, slug: str) -> dict:
    """Fetch /prices page and extract all category tables. Returns flat dict."""
    url = BASE_URL.format(slug=slug)
    result = {"city": city}

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"[{city}] Failed: {e}")
        return result

    soup = BeautifulSoup(resp.text, "html.parser")

    for h2 in soup.find_all("h2"):
        heading = h2.get_text(strip=True)

        cat_key = None
        for cat_heading, key in CATEGORY_MAP.items():
            if cat_heading.lower() in heading.lower():
                cat_key = key
                break
        if not cat_key:
            continue

        prefix = CAT_PREFIX[cat_key]
        table = h2.find_next("table")
        if not table:
            continue

        scraped = 0
        for row in table.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) < 2:
                continue

            # Item name — first line only (strips unit label after newline)
            raw_name = tds[0].get_text(separator="\n", strip=True)
            item_name = raw_name.split("\n")[0].strip()

            price = parse_price(tds[1].get_text(strip=True))
            if item_name:
                result[to_col(item_name, prefix)] = price
                scraped += 1

        log.debug(f"  [{city}] {cat_key}: {scraped} items")

    log.info(f"[{city}] {len(result) - 1} total data points scraped")
    return result


# ── Output ─────────────────────────────────────────────────────────────────────
def save_outputs(all_rows: list[dict]) -> None:
    os.makedirs("data/raw", exist_ok=True)
    df = pd.DataFrame(all_rows)
    df["scraped_date"] = pd.Timestamp.today().date()

    # Per-category files
    for cat_key, prefix in CAT_PREFIX.items():
        cat_cols = ["city"] + [c for c in df.columns if c.startswith(f"{prefix}_")]
        if len(cat_cols) <= 1:
            log.warning(f"No columns found for category: {cat_key}")
            continue
        out = f"data/raw/col_{cat_key}.csv"
        df[cat_cols + ["scraped_date"]].to_csv(out, index=False)
        log.info(f"Saved {out}  ({len(cat_cols)-1} items)")

    # Wide merged file
    df.to_csv("data/raw/col_all.csv", index=False)
    log.info(f"Saved data/raw/col_all.csv  ({df.shape[0]} rows × {df.shape[1]} cols)")

    # Summary
    print("\n=== Category summary ===")
    for cat_key, prefix in CAT_PREFIX.items():
        n = sum(1 for c in df.columns if c.startswith(f"{prefix}_"))
        print(f"  {cat_key:<20s}: {n} items")

    print(f"\n=== Sample (first 3 rows, first 6 cols) ===")
    preview = ["city"] + [c for c in df.columns if c != "city"][:5]
    print(df[preview].head(3).to_string(index=False))


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    all_rows = []
    for i, (city, slug) in enumerate(CITIES.items(), 1):
        log.info(f"[{i}/{len(CITIES)}] {city}  →  {BASE_URL.format(slug=slug)}")
        all_rows.append(scrape_city(city, slug))
        time.sleep(2)

    save_outputs(all_rows)


if __name__ == "__main__":
    main()