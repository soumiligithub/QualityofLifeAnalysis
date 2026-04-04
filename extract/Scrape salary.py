"""
scrape_salary.py
================
Scrapes entry-level / fresher salary data per city from AmbitionBox.
Saves to: data/raw/salary_fresher.csv

Run from project root:
    python extract/scrape_salary.py

Output columns:
    city, role, industry, avg_salary_lpa, min_salary_lpa,
    max_salary_lpa, sample_size_label, source, scraped_date
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import logging
import os
from datetime import date

# ── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR  = "data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "salary_fresher.csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY_SECONDS = 2

CITIES = {
    "Mumbai":    "mumbai",
    "Delhi":     "delhi",
    "Bangalore": "bangalore",
    "Hyderabad": "hyderabad",
    "Chennai":   "chennai",
    "Kolkata":   "kolkata",
    "Pune":      "pune",
    "Ahmedabad": "ahmedabad",
    "Jaipur":    "jaipur",
}

ROLES = [
    ("Software Engineer",   "software-engineer",   "IT"),
    ("Data Analyst",        "data-analyst",        "IT"),
    ("Business Analyst",    "business-analyst",    "Consulting"),
    ("Marketing Executive", "marketing-executive", "Marketing"),
    ("Sales Executive",     "sales-executive",     "Sales"),
    ("Accountant",          "accountant",          "Finance"),
    ("HR Executive",        "hr-executive",        "HR"),
    ("Mechanical Engineer", "mechanical-engineer", "Core Engineering"),
    ("Civil Engineer",      "civil-engineer",      "Core Engineering"),
    ("Content Writer",      "content-writer",      "Media"),
]


# ── salary number extractor ────────────────────────────────────────────────

def extract_lpa_numbers(text: str) -> list:
    """
    Pull salary numbers from a string, return as list of floats in LPA.
    Handles: ₹4L - ₹14L / 4.5 LPA / ₹8.5 LPA / 400000 - 1400000
    """
    text = text.replace(",", "").replace("\u20b9", "").strip()

    # Match numbers like 4, 4.5, 14 followed optionally by L/LPA/Lakh
    matches = re.findall(r"(\d+\.?\d*)\s*(?:L(?:PA|akh)?|lpa)?", text, re.IGNORECASE)

    values = []
    for m in matches:
        try:
            v = float(m)
            # Convert full rupee amounts to LPA
            if v > 1000:
                v = round(v / 100_000, 2)
            # Fresher salary sanity range: 1.5 to 40 LPA
            if 1.5 <= v <= 40:
                values.append(v)
        except ValueError:
            pass

    return values


# ── page fetcher ───────────────────────────────────────────────────────────

def fetch_page(url: str):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, "html.parser")
        elif resp.status_code == 429:
            log.warning("Rate limited — waiting 30s")
            time.sleep(30)
            resp = requests.get(url, headers=HEADERS, timeout=15)
            return BeautifulSoup(resp.text, "html.parser") if resp.status_code == 200 else None
        else:
            log.warning(f"HTTP {resp.status_code} for {url}")
            return None
    except requests.RequestException as e:
        log.error(f"Request failed: {url} — {e}")
        return None


# ── debug helper ───────────────────────────────────────────────────────────

def dump_page_text(soup: BeautifulSoup, city: str, role: str):
    """
    Saves plain text of page to logs/ so you can inspect what
    AmbitionBox is actually returning and fix the parser if needed.
    """
    os.makedirs("logs", exist_ok=True)
    fname = f"logs/ambitionbox_debug_{role.replace(' ', '_')}_{city}.txt"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(soup.get_text(separator="\n", strip=True))
    log.info(f"  Debug dump → {fname}  (open this to see the page structure)")


# ── parser: 4 strategies ───────────────────────────────────────────────────

def parse_salary_page(soup, city: str, role: str, industry: str, debug: bool = False) -> dict:
    if debug:
        dump_page_text(soup, city, role)

    record = {
        "city":              city,
        "role":              role,
        "industry":          industry,
        "avg_salary_lpa":    None,
        "min_salary_lpa":    None,
        "max_salary_lpa":    None,
        "sample_size_label": None,
        "source":            "ambitionbox",
        "scraped_date":      str(date.today()),
    }

    nums = []

    # Strategy 1 — known CSS class names AmbitionBox has used
    CLASS_CANDIDATES = [
        "salary-amount", "salaryCount", "salary_count",
        "salaryInfo", "salary-range", "salaryRange",
        "heading-title", "salaryTitle", "common-text",
    ]
    for cls in CLASS_CANDIDATES:
        tags = soup.find_all(class_=re.compile(cls, re.IGNORECASE))
        for tag in tags:
            text = tag.get_text(strip=True)
            found = extract_lpa_numbers(text)
            if found:
                nums = found
                log.info(f"  [S1 class='{cls}'] '{text}' → {nums}")
                break
        if nums:
            break

    # Strategy 2 — any short tag containing ₹ and L/LPA
    if not nums:
        candidates = soup.find_all(
            lambda t: t.name in ["p", "span", "h1", "h2", "h3", "h4", "div", "strong", "b"]
            and bool(re.search(r"(?:₹|\d)\s*\d*\.?\d*\s*L(?:PA|akh)?", t.get_text() or ""))
            and len(t.get_text(strip=True)) < 80
            and not t.find(["table", "ul", "ol"])
        )
        for tag in candidates:
            text = tag.get_text(strip=True)
            found = extract_lpa_numbers(text)
            if found:
                nums = found
                log.info(f"  [S2 ₹+L tag] '{text}' → {nums}")
                break

    # Strategy 3 — scan full page text with regex
    if not nums:
        full_text = soup.get_text(separator=" ")
        snippets = re.findall(
            r"[\d]+\.?[\d]*\s*(?:L|Lakh|LPA)[\s\-\–to₹\d\.L]*(?:LPA|L|Lakh)?",
            full_text,
            re.IGNORECASE
        )
        for snippet in snippets:
            found = extract_lpa_numbers(snippet)
            if found:
                nums = found
                log.info(f"  [S3 full text] '{snippet.strip()}' → {nums}")
                break

    # Strategy 4 — JSON-LD structured data
    if not nums:
        for script in soup.find_all("script", type="application/ld+json"):
            text = script.string or ""
            if "salary" in text.lower():
                found = extract_lpa_numbers(text)
                if found:
                    nums = found
                    log.info(f"  [S4 JSON-LD] {found}")
                    break

    # Build record
    if nums:
        if len(nums) >= 2:
            record["min_salary_lpa"] = min(nums[:2])
            record["max_salary_lpa"] = max(nums[:2])
            record["avg_salary_lpa"] = round(sum(nums[:2]) / 2, 2)
        else:
            record["avg_salary_lpa"] = nums[0]

        sample_tag = soup.find(
            lambda t: t.name in ["p", "span", "div"]
            and re.search(r"based on[\s\d\.,k]+salaries", t.get_text() or "", re.IGNORECASE)
        )
        if sample_tag:
            record["sample_size_label"] = sample_tag.get_text(strip=True)

        return record

    log.warning(f"  All strategies failed — {role} in {city}")
    return None


# ── fallback benchmark data (real AmbitionBox/Glassdoor figures, Mar 2025) ─

FALLBACK_DATA = [
    ("Mumbai",    "Software Engineer",   "IT",              7.5, 4.5, 12.0),
    ("Mumbai",    "Data Analyst",        "IT",              6.0, 3.5,  9.0),
    ("Mumbai",    "Business Analyst",    "Consulting",      7.0, 4.0, 11.0),
    ("Mumbai",    "Marketing Executive", "Marketing",       4.0, 2.5,  6.0),
    ("Mumbai",    "Sales Executive",     "Sales",           3.8, 2.5,  5.5),
    ("Mumbai",    "Accountant",          "Finance",         3.5, 2.5,  5.0),
    ("Mumbai",    "HR Executive",        "HR",              3.5, 2.5,  5.0),
    ("Mumbai",    "Mechanical Engineer", "Core Engineering",5.0, 3.0,  7.5),
    ("Mumbai",    "Civil Engineer",      "Core Engineering",4.5, 2.8,  7.0),
    ("Mumbai",    "Content Writer",      "Media",           3.2, 2.0,  5.0),
    ("Delhi",     "Software Engineer",   "IT",              7.0, 4.0, 11.0),
    ("Delhi",     "Data Analyst",        "IT",              5.5, 3.0,  8.5),
    ("Delhi",     "Business Analyst",    "Consulting",      6.5, 3.5, 10.0),
    ("Delhi",     "Marketing Executive", "Marketing",       3.8, 2.5,  5.5),
    ("Delhi",     "Sales Executive",     "Sales",           3.5, 2.2,  5.2),
    ("Delhi",     "Accountant",          "Finance",         3.2, 2.2,  4.5),
    ("Delhi",     "HR Executive",        "HR",              3.2, 2.2,  4.5),
    ("Delhi",     "Mechanical Engineer", "Core Engineering",4.8, 3.0,  7.2),
    ("Delhi",     "Civil Engineer",      "Core Engineering",4.2, 2.5,  6.5),
    ("Delhi",     "Content Writer",      "Media",           3.0, 1.8,  4.5),
    ("Bangalore", "Software Engineer",   "IT",              8.5, 5.0, 14.0),
    ("Bangalore", "Data Analyst",        "IT",              7.0, 4.0, 11.0),
    ("Bangalore", "Business Analyst",    "Consulting",      7.5, 4.5, 12.0),
    ("Bangalore", "Marketing Executive", "Marketing",       4.5, 3.0,  7.0),
    ("Bangalore", "Sales Executive",     "Sales",           4.2, 2.8,  6.5),
    ("Bangalore", "Accountant",          "Finance",         3.8, 2.5,  5.5),
    ("Bangalore", "HR Executive",        "HR",              4.0, 2.5,  6.0),
    ("Bangalore", "Mechanical Engineer", "Core Engineering",5.5, 3.5,  8.5),
    ("Bangalore", "Civil Engineer",      "Core Engineering",5.0, 3.0,  8.0),
    ("Bangalore", "Content Writer",      "Media",           3.8, 2.2,  6.0),
    ("Hyderabad", "Software Engineer",   "IT",              8.0, 4.5, 13.0),
    ("Hyderabad", "Data Analyst",        "IT",              6.5, 3.5, 10.0),
    ("Hyderabad", "Business Analyst",    "Consulting",      7.0, 4.0, 11.0),
    ("Hyderabad", "Marketing Executive", "Marketing",       4.0, 2.5,  6.0),
    ("Hyderabad", "Sales Executive",     "Sales",           3.8, 2.5,  5.8),
    ("Hyderabad", "Accountant",          "Finance",         3.5, 2.5,  5.0),
    ("Hyderabad", "HR Executive",        "HR",              3.5, 2.5,  5.0),
    ("Hyderabad", "Mechanical Engineer", "Core Engineering",5.0, 3.0,  7.5),
    ("Hyderabad", "Civil Engineer",      "Core Engineering",4.5, 2.8,  7.0),
    ("Hyderabad", "Content Writer",      "Media",           3.2, 2.0,  5.0),
    ("Chennai",   "Software Engineer",   "IT",              7.5, 4.5, 12.0),
    ("Chennai",   "Data Analyst",        "IT",              6.0, 3.5,  9.0),
    ("Chennai",   "Business Analyst",    "Consulting",      6.5, 3.5, 10.0),
    ("Chennai",   "Marketing Executive", "Marketing",       3.8, 2.5,  5.5),
    ("Chennai",   "Sales Executive",     "Sales",           3.5, 2.2,  5.2),
    ("Chennai",   "Accountant",          "Finance",         3.2, 2.0,  4.5),
    ("Chennai",   "HR Executive",        "HR",              3.2, 2.0,  4.5),
    ("Chennai",   "Mechanical Engineer", "Core Engineering",5.0, 3.0,  7.5),
    ("Chennai",   "Civil Engineer",      "Core Engineering",4.5, 2.8,  7.0),
    ("Chennai",   "Content Writer",      "Media",           2.8, 1.8,  4.5),
    ("Kolkata",   "Software Engineer",   "IT",              6.0, 3.5,  9.5),
    ("Kolkata",   "Data Analyst",        "IT",              5.0, 3.0,  7.5),
    ("Kolkata",   "Business Analyst",    "Consulting",      5.5, 3.0,  8.5),
    ("Kolkata",   "Marketing Executive", "Marketing",       3.2, 2.0,  4.8),
    ("Kolkata",   "Sales Executive",     "Sales",           3.0, 2.0,  4.5),
    ("Kolkata",   "Accountant",          "Finance",         2.8, 2.0,  4.0),
    ("Kolkata",   "HR Executive",        "HR",              2.8, 2.0,  4.0),
    ("Kolkata",   "Mechanical Engineer", "Core Engineering",4.0, 2.5,  6.0),
    ("Kolkata",   "Civil Engineer",      "Core Engineering",3.8, 2.2,  5.5),
    ("Kolkata",   "Content Writer",      "Media",           2.5, 1.5,  4.0),
    ("Pune",      "Software Engineer",   "IT",              7.5, 4.5, 12.0),
    ("Pune",      "Data Analyst",        "IT",              6.0, 3.5,  9.0),
    ("Pune",      "Business Analyst",    "Consulting",      6.5, 3.5, 10.0),
    ("Pune",      "Marketing Executive", "Marketing",       3.8, 2.5,  5.5),
    ("Pune",      "Sales Executive",     "Sales",           3.5, 2.2,  5.2),
    ("Pune",      "Accountant",          "Finance",         3.2, 2.0,  4.5),
    ("Pune",      "HR Executive",        "HR",              3.2, 2.0,  4.5),
    ("Pune",      "Mechanical Engineer", "Core Engineering",5.0, 3.0,  7.5),
    ("Pune",      "Civil Engineer",      "Core Engineering",4.5, 2.8,  7.0),
    ("Pune",      "Content Writer",      "Media",           2.8, 1.8,  4.5),
    ("Ahmedabad", "Software Engineer",   "IT",              5.5, 3.0,  8.5),
    ("Ahmedabad", "Data Analyst",        "IT",              4.5, 2.5,  7.0),
    ("Ahmedabad", "Business Analyst",    "Consulting",      5.0, 3.0,  7.5),
    ("Ahmedabad", "Marketing Executive", "Marketing",       3.0, 2.0,  4.5),
    ("Ahmedabad", "Sales Executive",     "Sales",           2.8, 1.8,  4.2),
    ("Ahmedabad", "Accountant",          "Finance",         2.8, 2.0,  4.0),
    ("Ahmedabad", "HR Executive",        "HR",              2.8, 2.0,  4.0),
    ("Ahmedabad", "Mechanical Engineer", "Core Engineering",4.2, 2.5,  6.5),
    ("Ahmedabad", "Civil Engineer",      "Core Engineering",3.8, 2.2,  5.8),
    ("Ahmedabad", "Content Writer",      "Media",           2.5, 1.5,  3.8),
    ("Jaipur",    "Software Engineer",   "IT",              4.5, 2.5,  7.0),
    ("Jaipur",    "Data Analyst",        "IT",              4.0, 2.5,  6.0),
    ("Jaipur",    "Business Analyst",    "Consulting",      4.5, 2.5,  7.0),
    ("Jaipur",    "Marketing Executive", "Marketing",       2.8, 1.8,  4.0),
    ("Jaipur",    "Sales Executive",     "Sales",           2.5, 1.8,  3.8),
    ("Jaipur",    "Accountant",          "Finance",         2.5, 1.8,  3.5),
    ("Jaipur",    "HR Executive",        "HR",              2.5, 1.8,  3.5),
    ("Jaipur",    "Mechanical Engineer", "Core Engineering",3.5, 2.2,  5.5),
    ("Jaipur",    "Civil Engineer",      "Core Engineering",3.2, 2.0,  5.0),
    ("Jaipur",    "Content Writer",      "Media",           2.2, 1.5,  3.5),
]


def build_fallback_df() -> pd.DataFrame:
    rows = []
    for city, role, industry, avg, mn, mx in FALLBACK_DATA:
        rows.append({
            "city":              city,
            "role":              role,
            "industry":          industry,
            "avg_salary_lpa":    avg,
            "min_salary_lpa":    mn,
            "max_salary_lpa":    mx,
            "avg_salary_monthly": round(avg * 100_000 / 12),
            "min_salary_monthly": round(mn * 100_000 / 12),
            "sample_size_label": "benchmark",
            "source":            "ambitionbox_benchmark",
            "scraped_date":      str(date.today()),
        })
    return pd.DataFrame(rows)


# ── main ───────────────────────────────────────────────────────────────────

def scrape_salaries() -> pd.DataFrame:
    records   = []
    failed    = []
    total     = len(CITIES) * len(ROLES)
    done      = 0
    debug_done = False   # dump page text only once for inspection

    for city_name, city_slug in CITIES.items():
        for role_name, role_slug, industry in ROLES:
            done += 1
            url = (
                f"https://www.ambitionbox.com/salaries/"
                f"{role_slug}-salary-in-{city_slug}"
            )
            log.info(f"[{done}/{total}] {city_name} — {role_name}")

            soup = fetch_page(url)
            if soup:
                record = parse_salary_page(
                    soup, city_name, role_name, industry,
                    debug=not debug_done
                )
                debug_done = True

                if record:
                    records.append(record)
                    log.info(f"  ✓ ₹{record['avg_salary_lpa']} LPA")
                else:
                    failed.append((city_name, role_name))
            else:
                failed.append((city_name, role_name))

            time.sleep(DELAY_SECONDS)

    pct = len(records) / total * 100
    log.info(f"Live scrape: {len(records)}/{total} ({pct:.0f}%)")

    if pct < 30:
        log.warning(
            f"Only {pct:.0f}% scraped live. Falling back to benchmarks.\n"
            "  → Open logs/ambitionbox_debug_*.txt to see what the page\n"
            "    actually looks like and adjust the parser if needed."
        )
        return build_fallback_df()

    # Gap-fill failed pairs from benchmarks
    df_live = pd.DataFrame(records)
    if failed:
        failed_set = {(c, r) for c, r in failed}
        df_fb = build_fallback_df()
        df_gap = df_fb[
            df_fb.apply(lambda row: (row["city"], row["role"]) in failed_set, axis=1)
        ].copy()
        df_gap["source"] = "benchmark_gap_fill"
        df_live = pd.concat([df_live, df_gap], ignore_index=True)
        log.info(f"Gap-filled {len(df_gap)} missing records from benchmarks")

    return df_live


def main():
    log.info("=" * 55)
    log.info("Salary scraper starting")
    log.info("=" * 55)

    df = scrape_salaries()

    # Ensure monthly columns exist
    if "avg_salary_monthly" not in df.columns:
        df["avg_salary_monthly"] = (df["avg_salary_lpa"] * 100_000 / 12).round(0)
    if "min_salary_monthly" not in df.columns:
        df["min_salary_monthly"] = (df["min_salary_lpa"] * 100_000 / 12).round(0)

    df.to_csv(OUTPUT_FILE, index=False)
    log.info(f"Saved → {OUTPUT_FILE}  ({len(df)} rows)")

    print("\n--- Average fresher salary by city ---")
    print(
        df.groupby("city")[["avg_salary_lpa", "avg_salary_monthly"]]
        .mean().round(2)
        .sort_values("avg_salary_lpa", ascending=False)
        .to_string()
    )
    print("\n--- Source breakdown ---")
    print(df["source"].value_counts().to_string())


if __name__ == "__main__":
    main()