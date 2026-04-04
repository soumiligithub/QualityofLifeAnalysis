import pandas as pd
from pathlib import Path
import json
from datetime import datetime

DATA_RAW = Path("data/raw")
DATA_SRC = Path("data")
DATA_PROC = Path("data/processed")
DATA_PROC.mkdir(exist_ok=True)

# helper to read from data/raw first, then data if missing
def read_csv_from_sources(name):
    for base in (DATA_RAW, DATA_SRC):
        path = base / name
        if path.exists():
            return pd.read_csv(path)
    raise FileNotFoundError(f"Could not find CSV file {name} in {DATA_RAW} or {DATA_SRC}")

# ====================== 1. CITY COSTS MASTER (from col_all + fuel) ======================
col_all = read_csv_from_sources("col_all.csv")
latest_date = col_all['scraped_date'].max()
col_all = col_all[col_all['scraped_date'] == latest_date].copy()
col_all = col_all.rename(columns={"scraped_date": "date"})

fuel_latest = read_csv_from_sources("fuel_latest.csv")
fuel_latest = fuel_latest.rename(columns={"extracted_at": "fuel_extracted", "date": "fuel_date"})
master = col_all.merge(fuel_latest[['city', 'petrol', 'diesel', 'cng', 'lpg', 'ev_per_kwh']], 
                       on='city', how='left')

# Optional: add food & transport if you want extra granularity
food = read_csv_from_sources("food_prices.csv")
transport = read_csv_from_sources("transport_prices.csv")
master = master.merge(food[['city', 'expat_eggs_12_inr']], on='city', how='left')
master = master.merge(transport[['city', 'auto_base_inr', 'auto_per_km_inr']], on='city', how='left')

# ====================== 2. QOL MASTER ======================
qol = read_csv_from_sources("qol_indices.csv")
qol = qol.rename(columns={"scraped_date": "date"})
qol_master = qol.copy()

# ====================== 3. LOAN / AFFORDABILITY ======================
loan_emi = read_csv_from_sources("loan_emi_city.csv")
loan_emi = loan_emi.rename(columns={"extracted_at": "date"})
loan_snapshot = loan_emi.copy()

# loan_rates for reference
loan_rates = read_csv_from_sources("loan_rates.csv")

# ====================== 4. BIG TABLES (optional but complete) ======================
# Combine all emi_calculations
emi_files = list(DATA_RAW.glob("emi_calculations_*.csv"))
def write_table(df, name):
    out_parquet = DATA_PROC / f"{name}.parquet"
    out_csv = DATA_PROC / f"{name}.csv"
    try:
        df.to_parquet(out_parquet, index=False)
        print(f"Wrote {out_parquet}")
    except Exception as e:
        print(f"Parquet write failed for {name}: {e}. Falling back to CSV.")
        df.to_csv(out_csv, index=False)
        print(f"Wrote {out_csv}")

if emi_files:
    emi_all = pd.concat((pd.read_csv(f) for f in emi_files), ignore_index=True)
    write_table(emi_all, "emi_calculations_master")

# Combine all home_loans
home_files = list(DATA_RAW.glob("home_loans_*.csv"))
if home_files:
    home_all = pd.concat((pd.read_csv(f) for f in home_files), ignore_index=True)
    write_table(home_all, "home_loans_master")

# Rent raw (for locality-level analysis if needed)
rent_raw = read_csv_from_sources("rent_raw.csv")

# ====================== 5. SAVE ======================
write_table(master, "city_costs_master")
write_table(qol_master, "qol_master")
write_table(loan_snapshot, "loan_emi_snapshot")
write_table(loan_rates, "loan_rates")

print("✅ Complete organization done!")
print(f"   city_costs_master: {master.shape}")
print(f"   qol_master: {qol_master.shape}")
print(f"   emi_calculations_master: {len(emi_files)} files combined")
print(f"   home_loans_master: {len(home_files)} files combined")
print("All files accounted for.")