# QoL Analyser — Setup Guide

## Exact folder structure

After setup, your VS Code project must look like this:

```
qol_app/
│
├── app.py                        ← Flask backend (handles all API routes)
├── requirements.txt              ← Python dependencies
│
├── data/                         ← PUT ALL YOUR CSV FILES HERE
│   ├── master_city_level.csv         (from Colab: eda_master_with_insights.csv, renamed)
│   ├── qol_master.csv                (from Colab: qol_master.csv)
│   ├── loan_emi_snapshot.csv         (from Colab: loan_emi_snapshot.csv)
│   ├── loan_rates.csv                (from Colab: loan_rates.csv)
│   └── emi_calculations_master.csv   (from Colab: emi_calculations_master.csv)
│
└── templates/
    └── index.html                ← The frontend (Flask serves this automatically)
```

---

## Step 1 — Export files from Google Colab

Run this cell at the END of your Colab notebook to download all required files:

```python
from google.colab import files

files.download('/content/eda_master_with_insights.csv')   # rename to master_city_level.csv
files.download('/content/qol_master.csv')
files.download('/content/loan_emi_snapshot.csv')
files.download('/content/loan_rates.csv')
files.download('/content/emi_calculations_master.csv')
```

Rename `eda_master_with_insights.csv` → `master_city_level.csv` after downloading.

---

## Step 2 — Put files in the right place

Move all 5 downloaded CSV files into the `data/` folder inside your project.
Do NOT put them in the root folder or inside `templates/`.

---

## Step 3 — Install dependencies

Open a terminal in VS Code (Ctrl + `) and run:

```bash
pip install flask pandas numpy
```

Or using requirements.txt:

```bash
pip install -r requirements.txt
```

---

## Step 4 — Run the app

In your terminal, from inside the `qol_app/` folder:

```bash
python app.py
```

You should see:

```
✅ Data loaded:
   master       : 20 cities × 118 columns
   qol          : 9 cities
   loan_emi     : 20 cities
   loan_rates   : 38 banks
   emi_calc     : 347850 rows (deduplicated)

🚀 QoL Analyser starting on http://localhost:5000
```

Then open your browser at:  http://localhost:5000

---

## Step 5 — Verify the API is working

Before using the app, open these URLs in your browser to confirm data loaded:

- http://localhost:5000/api/health         → should show all row counts > 0
- http://localhost:5000/api/cities         → should list all 20 cities
- http://localhost:5000/api/city/Mumbai    → should return Mumbai's full data as JSON

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `FileNotFoundError: data/master_city_level.csv` | Check the file is in the `data/` folder and named exactly right |
| `ModuleNotFoundError: flask` | Run `pip install flask pandas numpy` |
| Browser shows "Could not load cities" | Make sure `python app.py` is still running in the terminal |
| City data looks wrong (too high essentials) | Make sure you're using the CORRECTED pipeline output (`eda_master_with_insights.csv`), not the original broken one |
| Column not found warnings on startup | Normal if your CSV has slightly different column names — the app handles missing columns gracefully |

---

## How the data flows

```
Browser (index.html)
    │
    │  fetch('/api/cities')           → returns city list for dropdown
    │  fetch('/api/city/<name>')      → returns costs, QoL, loan, banks for one city
    │  fetch('/api/compare?salary=N') → returns all 20 cities ranked by salary sufficiency
    ▼
Flask (app.py)
    │
    │  reads from /data/ folder once at startup
    │  caches all 5 DataFrames in memory
    │  filters and computes on each request
    ▼
CSV files in /data/
    master_city_level.csv   (primary source for costs + derived features)
    qol_master.csv          (QoL indices for 9 cities)
    loan_emi_snapshot.csv   (city property prices + EMI snapshot)
    loan_rates.csv          (38 bank interest rates)
    emi_calculations_master.csv  (362k EMI simulations — used for 50L/20yr lookup)
```

---

## Columns the app reads from master_city_level.csv

Make sure your corrected pipeline output includes these columns
(they are all created by the fixed CELL 10 in qol_analyser_corrected.py):

| Column | Used for |
|---|---|
| `city` | Dropdown + all joins |
| `total_monthly_essentials` | Core sufficiency calculation |
| `rent_1_bhk_outside_city_centre` | Budget bar: rent |
| `rest_monthly_realistic` | Budget bar: food (or fallback calc) |
| `total_monthly_utilities` | Budget bar: utilities |
| `total_monthly_transport` | Budget bar: transport |
| `total_monthly_hh` | Budget bar: household help |
| `salary_sufficiency_ratio_50k` | Comparison table |
| `city_affordability_tier` | Tier labels |

If any column is missing, the app falls back gracefully — it won't crash.