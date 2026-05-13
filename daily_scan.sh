#!/bin/bash
cd /home/u-ack-it/Projects/muni_scanner

# --- Ensure API service (uvicorn api:app) is running on :8000 ---
# Idempotent: only starts if nothing is bound to port 8000.
# Webhook delivery, /alerts/{id}/feedback, and /digest/send all require this.
if ! ss -tln 2>/dev/null | grep -q ':8000 '; then
    mkdir -p logs
    nohup python3 -m uvicorn api:app --host 0.0.0.0 --port 8000 \
        >> logs/api.log 2>&1 &
    # Wait for the listener to actually come up. uvicorn import errors
    # (e.g. missing fastapi) crash immediately and leave nothing on :8000;
    # without this check the script silently proceeds and webhook/digest/
    # feedback endpoints stay dark for the rest of the day.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        ss -tln 2>/dev/null | grep -q ':8000 ' && break
    done
    if ! ss -tln 2>/dev/null | grep -q ':8000 '; then
        echo "[daily_scan] FATAL: uvicorn failed to bind :8000 — see logs/api.log" >&2
        tail -n 5 logs/api.log >&2 2>/dev/null
        # Continue to scanner (CLI path still works) but mark API as down.
        date -Iseconds > logs/api_down.flag
    else
        rm -f logs/api_down.flag
    fi
fi

python3 bond_scanner.py --csv data/incoming_bonds.csv --output output/latest_alerts.json
python3 -c "
import json, sys
sys.path.insert(0, '.')
from bond_email_agent import _email_alerts
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.env'))
alerts = json.loads(open('output/latest_alerts.json').read())
if alerts:
    _email_alerts(alerts)
"

# --- Validate RIA prospect domains, then run outreach ---
python3 mx_precheck.py prospects.csv --column contact_email --out prospects_clean.csv >> logs/mx_precheck.log 2>&1
python3 ria_outreach.py >> logs/outreach.log 2>&1
python3 ria_followup.py >> logs/followup.log 2>&1
