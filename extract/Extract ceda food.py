"""
extract_ceda_food.py
---------------------
Downloads official government food price data from CEDA / DoCA (Ashoka University).

Source:      https://dca.ceda.ashoka.edu.in/index.php/home/download
Data:        22 essential commodities, daily retail prices
Coverage:    Centre-level 2009–2023, State-level 2014–present
Outputs:
  data/raw/ceda_food_raw.csv        long format, all daily rows
  data/raw/ceda_food_prices.csv     wide format, 3-month avg per city

Run:
  pip install requests pandas
  py extract_ceda_food.py

NOTE: If no data downloads, CEDA may have changed their form parameters.
      Run the script with DEBUG_MODE = True to print the raw response
      for one city+commodity so you can inspect what's coming back.
"""

import os
import time
import logging
import requests
import pandas as pd
from io import StringIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DEBUG_MODE = False   # Set True to print raw server response for first request

# ── Config ─────────────────────────────────────────────────────────────────────
DOWNLOAD_URL = "https://dca.ceda.ashoka.edu.in/index.php/home/download"

CITY_CENTRE_MAP = {
    "Mumbai":        "Mumbai",
    "Delhi":         "Delhi",
    "Bangalore":     "Bengaluru",
    "Hyderabad":     "Hyderabad",
    "Chennai":       "Chennai",
    "Kolkata":       "Kolkata",
    "Pune":          "Pune",
    "Ahmedabad":     "Ahmedabad",
    "Jaipur":        "Jaipur",
    "Surat":         "Surat",
    "Lucknow":       "Lucknow",
    "Kanpur":        "Kanpur",
    "Nagpur":        "Nagpur",
    "Indore":        "Indore",
    "Bhopal":        "Bhopal",
    "Visakhapatnam": "Visakhapatnam",
    "Patna":         "Patna",
    "Vadodara":      "Vadodara",
    "Ludhiana":      "Ludhiana",
    "Coimbatore":    "Coimbatore",
}

COMMODITIES = [
    "Rice", "Wheat", "Atta (Wheat)", "Sugar", "Milk",
    "Toor/Arhar Dal", "Urad Dal", "Moong Dal", "Gram Dal", "Masoor Dal",
    "Onion", "Tomato", "Potato",
    "Mustard Oil (Packed)", "Groundnut Oil (Packed)", "Palm Oil (Packed)",
    "Soya Oil (Packed)", "Sunflower Oil (Packed)", "Vanaspati (Packed)",
    "Gur", "Tea Loose", "Salt Pack (Iodised)",
]

# Try most recent first; centre-level data ends Sept 2023
YEARS = ["2023", "2022", "2021"]

HEADERS = {
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0",
    "Referer":      "https://dca.ceda.ashoka.edu.in/index.php/home/download",
    "Content-Type": "application/x-www-form-urlencoded",
}

_debug_printed = False


def download_one(city: str, centre: str, commodity: str, year: str) -> pd.DataFrame | None:
    global _debug_printed
    payload = {
        "type":      "retail",
        "commodity": commodity,
        "centre":    centre,
        "year":      year,
        "submit":    "Download",
    }
    try:
        resp = requests.post(DOWNLOAD_URL, data=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        content = resp.text.strip()

        if DEBUG_MODE and not _debug_printed:
            print(f"\n--- DEBUG: first response ({city}, {commodity}, {year}) ---")
            print(content[:500])
            print("---")
            _debug_printed = True

        if not content or len(content) < 30 or "No data" in content:
            return None

        df = pd.read_csv(StringIO(content))
        df["city"] = city
        df["commodity"] = commodity
        return df

    except Exception as e:
        log.debug(f"  [{city}] {commodity} {year}: {e}")
        return None


def download_city(city: str, centre: str) -> pd.DataFrame:
    frames = []
    for commodity in COMMODITIES:
        for year in YEARS:
            df = download_one(city, centre, commodity, year)
            if df is not None and len(df) > 0:
                frames.append(df)
                break
            time.sleep(0.4)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def make_wide(raw: pd.DataFrame) -> pd.DataFrame:
    """Pivot raw long data → monthly avg per city, one col per commodity."""
    date_col  = next((c for c in raw.columns if "date" in c.lower()), None)
    price_col = next((c for c in raw.columns if any(k in c.lower() for k in ["retail", "price", "modal"])), None)

    if not date_col or not price_col:
        log.warning(f"Cannot find date/price columns. Columns seen: {list(raw.columns)}")
        return raw

    raw[date_col]  = pd.to_datetime(raw[date_col], errors="coerce")
    raw[price_col] = pd.to_numeric(raw[price_col], errors="coerce")
    raw = raw.dropna(subset=[date_col, price_col])

    cutoff = raw[date_col].max() - pd.DateOffset(months=3)
    recent = raw[raw[date_col] >= cutoff] if not raw.empty else raw

    agg = (
        recent.groupby(["city", "commodity"])[price_col]
        .mean().round(2).reset_index()
    )
    agg.columns = ["city", "commodity", "avg_retail_inr"]

    pivot = agg.pivot(index="city", columns="commodity", values="avg_retail_inr")
    pivot.columns = [
        "ceda_" + c.lower().replace(" ", "_").replace("/", "_")
                         .replace("(", "").replace(")", "")
        for c in pivot.columns
    ]
    return pivot.reset_index()


def main():
    os.makedirs("data/raw", exist_ok=True)
    all_raw = []

    for i, (city, centre) in enumerate(CITY_CENTRE_MAP.items(), 1):
        log.info(f"[{i}/{len(CITY_CENTRE_MAP)}] {city}")
        df = download_city(city, centre)
        if not df.empty:
            all_raw.append(df)
            log.info(f"  → {len(df)} rows")
        else:
            log.warning(f"  → no data (set DEBUG_MODE=True to inspect raw response)")
        time.sleep(1)

    if not all_raw:
        log.error(
            "No data at all. CEDA may use AJAX/JS to serve downloads.\n"
            "Fix: open https://dca.ceda.ashoka.edu.in/index.php/home/download in Chrome,\n"
            "      open DevTools → Network, submit form, find the POST/XHR request,\n"
            "      copy exact form field names and update the payload dict above."
        )
        return

    raw_df = pd.concat(all_raw, ignore_index=True)
    raw_df.to_csv("data/raw/ceda_food_raw.csv", index=False)
    log.info(f"Saved ceda_food_raw.csv  {raw_df.shape}")

    wide = make_wide(raw_df)
    wide["scraped_date"] = pd.Timestamp.today().date()
    wide.to_csv("data/raw/ceda_food_prices.csv", index=False)
    log.info(f"Saved ceda_food_prices.csv  {wide.shape}")
    print(wide.head(3).to_string())


if __name__ == "__main__":
    main()
    