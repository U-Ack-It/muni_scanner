# AGENTS.md — Municipal Bond Anomaly Scanner

## What this project does
Detects small municipal bonds (< $20M par) paying 50+ basis points above peer average
for similar credit quality, maturity, state, and issuer type. Outputs plain-English
alerts in JSON. Serves them via FastAPI. Accumulates institutional memory over time.

The LLM role here is **explainer and contextualizer**, not primary detector.
Statistical peer comparison runs first; the agent interprets results and surfaces patterns.

---

## Key files

| File | Purpose |
|------|---------|
| `bond_scanner.py` | Core detection logic — peer matching, spread calc, alert generation |
| `api.py` | FastAPI service — POST /scan, GET /alerts, POST /alerts/{id}/feedback |
| `data/sample_bonds.csv` | Sample bond universe (15 bonds, 3 targets + 12 peers) |
| `templates/alert_email.html` | Jinja2 HTML email template |
| `memory/alerts_log.md` | Persistent log of all alerts + human validation |
| `memory/issuer_patterns.md` | Recurring issuers, structural observations |
| `memory/state_patterns.md` | State-level spread patterns accumulated over time |
| `output/` | JSON alert files by date (alerts_YYYYMMDD.json) |

---

## How to run

```bash
# One-time: install runtime deps (fastapi, uvicorn, httpx, jinja2, python-multipart)
# REQUIRED before any API path works — daily_scan.sh's uvicorn launch will
# silently crash with ModuleNotFoundError: No module named 'fastapi' otherwise.
python3 -m pip install -r requirements.txt

# Test with mock data (no files needed)
python bond_scanner.py --mock

# Run against CSV
python bond_scanner.py --csv data/sample_bonds.csv

# Start API server (interpreter MUST have fastapi installed — see install step above)
uvicorn api:app --reload --port 8000

# Docker
docker build -t muni-scanner .
docker run -p 8000:8000 -e WEBHOOK_URLS=https://your-endpoint.com muni-scanner
```

### Service health: is the API actually up?

`daily_scan.sh` auto-starts uvicorn on :8000 if nothing is bound there, but
it uses `python3 -m uvicorn` — i.e. whichever interpreter `python3` resolves
to. If that interpreter does not have `fastapi` installed, uvicorn imports
`api.py`, hits `from fastapi import ...`, and dies before binding the port.
The CLI scanner path keeps working, but `/scan`, `/alerts/{id}/feedback`,
`/digest/send`, and webhook delivery all stay dark.

Quick check:
```bash
ss -tln | grep ':8000 '         # should show LISTEN
tail -20 logs/api.log           # look for ModuleNotFoundError
ls logs/api_down.flag           # present => last start attempt failed
```

If `fastapi` is missing, `pip install -r requirements.txt` against the same
interpreter `python3` points to. Do not assume the project `.venv` is the
one being used — `daily_scan.sh` does not activate it.

---

## Detection logic

Peer matching criteria (all must pass):
1. Same state (expands nationally if < 2 peers)
2. Same issuer type: GO vs Revenue
3. Same rating tier: Aa (Aaa–Aa3) / A (A1–A3) / Baa (Baa1–Baa3)
4. Maturity within ±2 years
5. Par ≤ $20M

Anomaly: YTW ≥ 50bps above peer average → alert generated
Public tier: spread ≥ 75bps | Subscriber tier: 50–74bps

---

## API endpoints

- `POST /scan?mock=true` — run scanner, returns + stores alerts, fires webhooks
- `POST /scan` + CSV file upload — same, with your data
- `GET /alerts?public_only=true&state=NM&since=2026-05-01` — historical alerts
- `POST /alerts/{alert_id}/feedback` — body: `{"valid": true, "notes": "..."}` → updates memory
- `GET /memory/summary` — current state of knowledge base
- `GET /health` — status + counts

---

## Memory system (Karpathy pattern)

Every alert is appended to `memory/alerts_log.md` with structured tags.
Human feedback via `/alerts/{id}/feedback` updates the validation status in-place.
Over time, check `issuer_patterns.md` and `state_patterns.md` for recurring signals.

When answering questions about historical alerts or patterns:
1. Read `memory/alerts_log.md` for individual alert history
2. Read `memory/issuer_patterns.md` for issuer-level recurrence
3. Trust current JSON files in `output/` over memory for raw numbers

---

## Data entry (EMMA has no public API)

Enter bonds manually into CSV. See README.md for the field-by-field EMMA guide.
Set `is_target=true` for bonds to analyze; `is_target=false` for peer/comparison bonds.
Need at least 2 peers per target for a valid comparison.

---

## Webhook push model

Bond scanner pushes alerts OUT to consumers — nothing polls it.
Set `WEBHOOK_URLS` env var (comma-separated) to route alerts to:
- Newsletter CMS
- Telegram bot
- Ouroboros internal endpoint (optional signal layer)
