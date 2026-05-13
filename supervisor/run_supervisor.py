"""
muni_scanner + BondAnomaly Supervisor — joint L2 + L3 entry point.

L2 scans every 15 minutes for both muni_scanner and BondAnomaly.
L3 triggers automatically on HIGH severity escalations.
MEDIUM → Telegram alert, human review.
LOW → logged only.

Usage:
    cd /home/u-ack-it/Projects/muni_scanner
    python3 supervisor/run_supervisor.py &
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from supervisor.l2_muni_scanner import run_scan as muni_scan
from supervisor.l2_bondanomaly import run_scan as bond_scan
from supervisor.l3_fixer import run_l3 as muni_l3
from supervisor.l3_fixer_bondanomaly import run_l3 as bond_l3

POLL_INTERVAL_SEC  = 900
L3_AUTO_SEVERITIES = {"HIGH"}

BOND_TYPES = {
    "OUTREACH_DUPLICATE", "LOG_CORRUPTION", "FOLLOWUP_CRON_BROKEN",
    "NUDGE_OVERDUE", "PROSPECT_QUALITY_DRIFT", "NO_CONVERSION_TRACKING",
    "STRIPE_UNTRACKED",
}


def _route_l3(esc: dict):
    if esc["type"] in BOND_TYPES:
        return bond_l3(esc)
    return muni_l3(esc)


def run():
    print("[Supervisor] Starting — muni_scanner + BondAnomaly, L2 every 15 min, L3 on HIGH")
    while True:
        print(f"\n[Supervisor] Scan at {datetime.now().strftime('%H:%M:%S')}")

        new_escalations = []
        for label, scan_fn in (("muni_scanner", muni_scan), ("BondAnomaly", bond_scan)):
            try:
                escs = scan_fn()
                new_escalations.extend(escs)
            except Exception as exc:
                print(f"[Supervisor] {label} L2 error: {exc}")

        for esc in new_escalations:
            if esc["severity"] in L3_AUTO_SEVERITIES:
                print(f"[Supervisor] Routing {esc['id']} → L3")
                try:
                    result = _route_l3(esc)
                    print(f"[Supervisor] L3 {'FIXED' if result['fixed'] else 'PROPOSED'}: {esc['id']}")
                except Exception as exc:
                    print(f"[Supervisor] L3 error: {exc}")
            else:
                print(f"[Supervisor] {esc['id']} ({esc['severity']}) — Telegram sent")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)
    run()
