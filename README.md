# Municipal Bond Anomaly Scanner

Detects small municipal bonds (< $20M par) paying 50+ basis points above peer average for similar credit quality, maturity, state, and issuer type. Outputs plain-English alerts in JSON + HTML email format.

---

## What it flags

An **anomaly** is a bond where:
- Yield to worst (YTW) ≥ 0.50% above peer average
- Peers share: same state, same issuer type (GO vs Revenue), same rating tier (Aa/A/Baa), maturity ±2 years, par < $20M

Alerts are tagged **Public** (spread ≥ 75bps) or **Subscriber Only** (50–74bps).

---

## Quick start

```bash
# Test with built-in mock data (no files needed)
python bond_scanner.py --mock

# Run against your CSV
python bond_scanner.py --csv data/sample_bonds.csv

# Save alerts to JSON
python bond_scanner.py --csv data/my_bonds.csv --output output/alerts.json

# Raise the threshold to 75bps
python bond_scanner.py --csv data/my_bonds.csv --threshold 75
```

---

## Manual data entry (first 30 days)

EMMA (emma.msrb.org) has no public API. For the first 30 days, enter bonds manually into a CSV.

### Step 1 — Find bonds on EMMA

1. Go to **emma.msrb.org**
2. Click **Bond Search** → set filters:
   - Par amount: **max $20M**
   - State (optional)
   - Maturity range
   - Rating class
3. Click a bond → note these fields from the Security Details page:

| EMMA field | CSV column |
|-----------|-----------|
| CUSIP | `cusip` |
| Issuer Name | `issuer` |
| State | `state` |
| Bond Purpose | `purpose` |
| Security Type | `issuer_type` (use "GO" or "Revenue") |
| Par Amount | `par_amount` |
| Maturity Date | `maturity_date` (YYYY-MM-DD) |
| Coupon Rate | `coupon` |
| Call Date | `call_date` (YYYY-MM-DD or blank) |
| Call Price | `call_price` (e.g. 100.00 or blank) |
| Moody's Rating | `rating_moodys` |
| S&P Rating | `rating_sp` |

4. Click **Trade Data** tab → use the most recent trade's **Yield** as `ytw` and `ytm`
   - If no recent trade: use the **Offered yield** from a dealer quote on EMMA
   - Leave `last_trade_date` and `last_trade_price` blank if unavailable

### Step 2 — Mark targets vs peers

- Set `is_target=true` for bonds you want to analyze
- Set `is_target=false` for peer/comparison bonds
- You need at least 2 peers per target to run the comparison

### Step 3 — Run the scanner

```bash
python bond_scanner.py --csv data/my_bonds.csv --output output/alerts.json
```

### CSV schema

```
cusip,issuer,state,purpose,issuer_type,par_amount,maturity_date,coupon,ytw,ytm,
call_date,call_price,rating_moodys,rating_sp,rating_fitch,
last_trade_date,last_trade_price,is_target
```

See `data/sample_bonds.csv` for a complete example with 15 bonds (3 targets, 12 peers).

---

## Output

### Console
```
══════════════════════════════════════════════════════════════
  ALERT  MUNI-20260511-AA1  [PUBLIC]
  Albuquerque Water Utility Authority  |  NM  |  CUSIP 558000AA1
══════════════════════════════════════════════════════════════
  YTW          : 4.52%
  Peer avg YTW : 3.87%
  Spread       : +65 bps  (4 peers)
  Rating       : A2/A
  Maturity     : 2031-06-01
  Par          : $8.5M
  ...
```

### JSON (`output/alerts_YYYYMMDD.json`)
```json
[
  {
    "alert_id": "MUNI-20260511-AA1",
    "cusip": "558000AA1",
    "is_public": true,
    "spread_bps": 65,
    "ytw": 4.52,
    "peer_avg_ytw": 3.87,
    "plain_english": "...",
    "red_flag": "...",
    "anon_id": "a3f9c2b1d8e4"
  }
]
```

`anon_id` is a SHA-256 hash of CUSIP + timestamp — use this as the content flywheel ID.

### HTML email
Rendered from `templates/alert_email.html` (Jinja2-compatible). Plug into any email sender:
```python
from jinja2 import Template
html = Template(open("templates/alert_email.html").read()).render(**alert_dict)
```

---

## File structure

```
muni_scanner/
├── bond_scanner.py          # main script
├── data/
│   └── sample_bonds.csv     # 15 mock bonds (3 targets + 12 peers)
├── templates/
│   └── alert_email.html     # HTML email template (Jinja2)
├── output/                  # generated alert JSON files
└── README.md
```

---

## Peer matching logic

| Criterion | Match rule |
|-----------|-----------|
| State | Exact match (expands nationally if < 2 peers found) |
| Issuer type | GO vs Revenue (broad) |
| Rating tier | Aa (Aaa–Aa3) / A (A1–A3) / Baa (Baa1–Baa3) |
| Maturity | ±2 years |
| Par | ≤ $20M |

---

## Roadmap

- [ ] EMMA Playwright scraper (replace manual CSV)
- [ ] MSRB bulk data feed integration
- [ ] Email delivery via SendGrid / AWS SES
- [ ] Subscriber vs public gating (webhook to CMS)
- [ ] Weekly digest roll-up
- [ ] State-level filter presets

---

## Disclaimer

For informational purposes only. Not investment advice. Always verify yield data against current market quotes before use.
