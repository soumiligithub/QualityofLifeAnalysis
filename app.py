"""
QoL Analyser — Flask Backend
Reads your exported CSV files and serves JSON to the frontend.

Run:  python app.py
Then open:  http://localhost:5000
"""

from flask import Flask, jsonify, render_template, request
import pandas as pd
import numpy as np
import os

app = Flask(__name__)

# ── Paths to your CSV files (all go in the /data folder) ─────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

MASTER_CSV    = os.path.join(DATA_DIR, "master_city_level.csv")
QOL_CSV       = os.path.join(DATA_DIR, "qol_master.csv")
LOAN_EMI_CSV  = os.path.join(DATA_DIR, "loan_emi_snapshot.csv")
LOAN_RATES_CSV= os.path.join(DATA_DIR, "loan_rates.csv")
EMI_CALC_CSV  = os.path.join(DATA_DIR, "emi_calculations_master.csv")


# ── Load & cache all data once at startup ────────────────────────────────────
def load_data():
    """Load and prepare all dataframes. Called once on startup."""
    data = {}

    # 1. Master city-level file (your main output from the corrected pipeline)
    master = pd.read_csv(MASTER_CSV)
    master = master.replace({np.nan: None})
    data["master"] = master

    # 2. QoL master (9 cities)
    qol = pd.read_csv(QOL_CSV)
    qol = qol.drop(columns=["date", "source"], errors="ignore")
    qol = qol.replace({np.nan: None})
    data["qol"] = qol

    # 3. Loan EMI snapshot (20 cities)
    loan_emi = pd.read_csv(LOAN_EMI_CSV)
    loan_emi = loan_emi.drop(columns=["date"], errors="ignore")
    loan_emi = loan_emi.replace({np.nan: None})
    data["loan_emi"] = loan_emi

    # 4. Loan rates (38 banks) — already cleaned
    loan_rates = pd.read_csv(LOAN_RATES_CSV)
    loan_rates = loan_rates.drop(columns=["extracted_at"], errors="ignore")
    # Fix anomalous max_rate values (< 1.0 are data errors)
    loan_rates.loc[loan_rates["max_rate"] < 1.0, "max_rate"] = (
        loan_rates.loc[loan_rates["max_rate"] < 1.0, "min_rate"] + 1.5
    )
    loan_rates["processing_fee_pct"] = loan_rates["processing_fee_pct"].fillna(
        loan_rates["processing_fee_pct"].median()
    )
    loan_rates = loan_rates.replace({np.nan: None})
    data["loan_rates"] = loan_rates

    # 5. EMI calculations (large file — load only once, filter per request)
    emi_calc = pd.read_csv(EMI_CALC_CSV).drop_duplicates()
    emi_calc = emi_calc.replace({np.nan: None})
    data["emi_calc"] = emi_calc

    print(f"✅ Data loaded:")
    print(f"   master       : {len(master)} cities × {len(master.columns)} columns")
    print(f"   qol          : {len(qol)} cities")
    print(f"   loan_emi     : {len(loan_emi)} cities")
    print(f"   loan_rates   : {len(loan_rates)} banks")
    print(f"   emi_calc     : {len(emi_calc)} rows (deduplicated)")
    return data


try:
    DATA = load_data()
except FileNotFoundError as e:
    print(f"\n❌  CSV file not found: {e}")
    print(f"    Put all CSV files in the /data folder and restart.\n")
    DATA = {}


# ── Helper ────────────────────────────────────────────────────────────────────
def safe_val(v):
    """Convert numpy types and NaN to Python-native for JSON serialisation."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Serve the main HTML page."""
    return render_template("index.html")


# ── GET /api/cities ──────────────────────────────────────────────────────────
@app.route("/api/cities")
def api_cities():
    """Return the list of all cities (for the dropdown)."""
    if "master" not in DATA:
        return jsonify({"error": "Data not loaded"}), 500
    cities = sorted(DATA["master"]["city"].tolist())
    return jsonify({"cities": cities})


# ── GET /api/city/<city_name> ─────────────────────────────────────────────────
@app.route("/api/city/<city_name>")
def api_city(city_name):
    """
    Return all data for one city:
      - cost breakdown
      - QoL indices (if available)
      - loan EMI snapshot
      - top 5 cheapest bank rates
    """
    if "master" not in DATA:
        return jsonify({"error": "Data not loaded"}), 500

    master = DATA["master"]
    row = master[master["city"].str.lower() == city_name.lower()]

    if row.empty:
        return jsonify({"error": f"City '{city_name}' not found"}), 404

    row = row.iloc[0]

    # ── Cost breakdown ────────────────────────────────────────────────────────
    costs = {
        "rent":      safe_val(row.get("rent_1_bhk_outside_city_centre")),
        "food":      safe_val(row.get("rest_monthly_realistic")),         # from corrected pipeline
        "groceries": safe_val(row.get("total_monthly_groceries")),         # if column exists
        "utilities": safe_val(row.get("total_monthly_utilities")),
        "transport": safe_val(row.get("total_monthly_transport")),
        "household": safe_val(row.get("total_monthly_hh")),
        "total_essentials": safe_val(row.get("total_monthly_essentials")),
    }
    # Fallback: if sub-total columns don't exist, use raw columns
    if costs["food"] is None:
        costs["food"] = safe_val(row.get("rest_veg_thali_local_restaurant", 0)) * 15

    # ── QoL ───────────────────────────────────────────────────────────────────
    qol_data = None
    qol_df = DATA.get("qol", pd.DataFrame())
    qol_row = qol_df[qol_df["city"].str.lower() == city_name.lower()]
    if not qol_row.empty:
        qr = qol_row.iloc[0]
        qol_data = {
            "qol_index":                    safe_val(qr.get("qol_index")),
            "safety_index":                 safe_val(qr.get("safety_index")),
            "healthcare_index":             safe_val(qr.get("healthcare_index")),
            "pollution_index":              safe_val(qr.get("pollution_index")),
            "climate_index":                safe_val(qr.get("climate_index")),
            "traffic_commute_time_index":   safe_val(qr.get("traffic_commute_time_index")),
            "purchasing_power_index":       safe_val(qr.get("purchasing_power_index")),
            "cost_of_living_index":         safe_val(qr.get("cost_of_living_index")),
            "affordability_pressure_score": safe_val(qr.get("affordability_pressure_score")),
            "student_liveability_score":    safe_val(qr.get("student_liveability_score")),
        }

    # ── Loan EMI snapshot ─────────────────────────────────────────────────────
    loan_data = None
    loan_df = DATA.get("loan_emi", pd.DataFrame())
    loan_row = loan_df[loan_df["city"].str.lower() == city_name.lower()]
    if not loan_row.empty:
        lr = loan_row.iloc[0]
        # Handle both original and renamed column names
        prop_price = safe_val(lr.get("avg_property_price_inr") or lr.get("avg_property_price"))
        loan_data = {
            "avg_property_price": prop_price,
            "emi_monthly_avg":    safe_val(lr.get("emi_monthly_avg_inr") or lr.get("emi_monthly_avg")),
            "emi_monthly_min":    safe_val(lr.get("emi_monthly_min")),
            "emi_monthly_max":    safe_val(lr.get("emi_monthly_max")),
            "emi_burden_pct":     safe_val(lr.get("emi_burden_pct")),
        }

    # ── EMI for 50L / 20yr (from emi_calc) ───────────────────────────────────
    emi_50l = None
    emi_df = DATA.get("emi_calc", pd.DataFrame())
    if not emi_df.empty:
        subset = emi_df[
            (emi_df["city"].str.lower() == city_name.lower()) &
            (emi_df["loan_amount"] == 5_000_000) &
            (emi_df["tenure_years"] == 20)
        ]
        if not subset.empty:
            emi_50l = float(subset["emi_amount"].mean())

    # ── Top 5 cheapest banks ──────────────────────────────────────────────────
    rates_df = DATA.get("loan_rates", pd.DataFrame())
    top_banks = []
    if not rates_df.empty:
        top5 = rates_df.nsmallest(5, "min_rate")[["bank", "min_rate", "max_rate", "processing_fee_pct", "max_tenure_yrs"]]
        top_banks = top5.to_dict(orient="records")
        for b in top_banks:
            for k, v in b.items():
                b[k] = safe_val(v)

    return jsonify({
        "city":      city_name,
        "costs":     costs,
        "qol":       qol_data,
        "loan":      loan_data,
        "emi_50l_20yr": emi_50l,
        "top_banks": top_banks,
        "has_qol":   qol_data is not None,
    })


# ── GET /api/compare?salary=75000 ────────────────────────────────────────────
@app.route("/api/compare")
def api_compare():
    """
    Return all 20 cities ranked by salary sufficiency for a given salary.
    Query param: salary (int, default 75000)
    """
    if "master" not in DATA:
        return jsonify({"error": "Data not loaded"}), 500

    salary = float(request.args.get("salary", 75000))
    master = DATA["master"]
    qol_df = DATA.get("qol", pd.DataFrame())

    results = []
    for _, row in master.iterrows():
        essentials = safe_val(row.get("total_monthly_essentials"))
        if essentials is None or essentials == 0:
            continue

        ratio = salary / essentials

        # QoL lookup
        qol_row = qol_df[qol_df["city"].str.lower() == str(row["city"]).lower()]
        qol_index = None
        afford_pressure = None
        if not qol_row.empty:
            qol_index       = safe_val(qol_row.iloc[0].get("qol_index"))
            afford_pressure = safe_val(qol_row.iloc[0].get("affordability_pressure_score"))

        results.append({
            "city":              str(row["city"]),
            "essentials":        essentials,
            "ratio":             round(ratio, 3),
            "qol_index":         qol_index,
            "afford_pressure":   afford_pressure,
            "has_qol":           qol_index is not None,
            "verdict":           "go" if ratio >= 1.4 else "caution" if ratio >= 1.0 else "stop",
        })

    results.sort(key=lambda x: x["ratio"], reverse=True)
    return jsonify({"salary": salary, "cities": results})


# ── GET /api/loan-rates ───────────────────────────────────────────────────────
@app.route("/api/loan-rates")
def api_loan_rates():
    """Return all bank loan rates, sorted by min_rate."""
    if "loan_rates" not in DATA:
        return jsonify({"error": "Data not loaded"}), 500

    df = DATA["loan_rates"].sort_values("min_rate")
    return jsonify({"banks": df.to_dict(orient="records")})


# ── GET /api/health ───────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    """Quick check to confirm all files loaded correctly."""
    status = {
        "master":     len(DATA.get("master",     pd.DataFrame())),
        "qol":        len(DATA.get("qol",        pd.DataFrame())),
        "loan_emi":   len(DATA.get("loan_emi",   pd.DataFrame())),
        "loan_rates": len(DATA.get("loan_rates", pd.DataFrame())),
        "emi_calc":   len(DATA.get("emi_calc",   pd.DataFrame())),
    }
    all_ok = all(v > 0 for v in status.values())
    return jsonify({"ok": all_ok, "rows": status})

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    print("\n🚀 QoL Analyser starting on http://localhost:5000\n")
    app.run(host="0.0.0.0", port=port, debug=False)