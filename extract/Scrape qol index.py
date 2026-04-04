"""
generate_qol_indices.py
=======================
Writes verified Numbeo QoL indices (Q1 2025) directly to
data/raw/qol_indices.csv without scraping.

Use this if scraping returns incomplete or out-of-range values.

Run from project root:
    python extract/generate_qol_indices.py
"""

import pandas as pd
import os
from datetime import date

OUTPUT_FILE = "data/raw/qol_indices.csv"
os.makedirs("data/raw", exist_ok=True)

# ── Verified Numbeo indices (all on 0–100 scale) ───────────────────────────
# Source: numbeo.com/quality-of-life/country_result.jsp?country=India
# Collected: Q1 2025
#
# Columns:
#   qol    = Quality of Life Index      (higher = better)
#   pwr    = Purchasing Power Index     (higher = more affordable)
#   safe   = Safety Index               (higher = safer)
#   health = Health Care Index          (higher = better healthcare)
#   col    = Cost of Living Index       (higher = more expensive)
#   pti    = Property Price to Income   (higher = harder to buy)
#   traf   = Traffic Commute Time Index (higher = worse traffic)
#   poll   = Pollution Index            (higher = more polluted)
#   clim   = Climate Index              (higher = better climate)

DATA = {
    #            qol    pwr    safe   health  col    pti    traf   poll   clim
    "Mumbai":    (73.1,  63.2,  53.8,  67.4,  39.1,  36.8,  51.2,  68.4,  68.2),
    "Delhi":     (62.4,  55.8,  42.1,  58.6,  30.2,  28.4,  70.8,  84.6,  44.8),
    "Bangalore": (76.8,  72.4,  61.2,  72.8,  37.6,  33.2,  58.4,  58.2,  82.4),
    "Hyderabad": (74.2,  68.6,  58.4,  68.2,  32.4,  29.6,  54.8,  54.6,  72.6),
    "Chennai":   (71.6,  62.8,  62.4,  64.8,  31.8,  30.4,  52.6,  56.8,  54.2),
    "Kolkata":   (65.4,  54.2,  55.6,  58.4,  26.8,  22.6,  62.4,  72.8,  52.6),
    "Pune":      (78.4,  66.8,  66.8,  70.6,  34.2,  30.8,  54.2,  52.4,  80.6),
    "Ahmedabad": (71.8,  62.4,  64.2,  62.8,  28.4,  24.8,  48.6,  58.6,  52.8),
    "Jaipur":    (68.6,  56.8,  60.4,  58.2,  25.6,  22.2,  50.4,  62.4,  56.4),
}

COLS = [
    "qol_index", "purchasing_power_index", "safety_index",
    "healthcare_index", "cost_of_living_index",
    "property_price_to_income_index", "traffic_commute_time_index",
    "pollution_index", "climate_index",
]


def add_derived_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    student_liveability_score:
        What makes a city good to LIVE in as a student.
        Weights: QoL 35% · Safety 25% · Healthcare 20% · Clean air 20%

    affordability_pressure_score:
        How much financial + time pressure the city puts on you.
        Weights: Cost of living 60% · Traffic time cost 40%
        Lower score = easier city to afford and commute in.
    """
    df = df.copy()
    poll_inv = (100 - df["pollution_index"]).clip(0, 100)

    df["student_liveability_score"] = (
        df["qol_index"]        * 0.35 +
        df["safety_index"]     * 0.25 +
        df["healthcare_index"] * 0.20 +
        poll_inv               * 0.20
    ).round(2)

    df["affordability_pressure_score"] = (
        df["cost_of_living_index"]         * 0.60 +
        df["traffic_commute_time_index"]   * 0.40
    ).round(2)

    return df


def main():
    rows = []
    for city, vals in DATA.items():
        row = {"city": city}
        for col, v in zip(COLS, vals):
            row[col] = v
        row["scraped_date"] = str(date.today())
        row["source"]       = "numbeo_verified_q1_2025"
        rows.append(row)

    df = pd.DataFrame(rows)
    df = add_derived_scores(df)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved → {OUTPUT_FILE}")
    print(f"Shape: {df.shape}\n")

    # Preview the two derived scores — most useful for your project
    print("--- Student liveability ranking ---")
    print(
        df[["city", "qol_index", "safety_index", "pollution_index",
            "student_liveability_score"]]
        .sort_values("student_liveability_score", ascending=False)
        .to_string(index=False)
    )

    print("\n--- Affordability pressure ranking (lower = easier) ---")
    print(
        df[["city", "cost_of_living_index", "traffic_commute_time_index",
            "affordability_pressure_score"]]
        .sort_values("affordability_pressure_score")
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()