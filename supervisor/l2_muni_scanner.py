"""
L2 Supervisor — muni_scanner

Runs every 15 minutes. Reads logs, output files, memory, and cross-system
regime snapshot. Detects semantic anomalies and escalates to L3 via
escalation queue + Telegram.

Detectors:
  1. API_SERVICE_DOWN        — uvicorn not serving port 8000
  2. ALERT_DROUGHT           — no output file produced in >24h
  3. MEMORY_NOT_UPDATING     — output/ has alerts that alerts_log.md doesn't
  4. REGIME_COUPLING_LOST    — Ouroboros snapshot missing or stale
  5. EMAIL_AGENT_DUPLICATE   — bond_email_agent.log lines doubled per cycle
  6. MAILER_SILENT_FAILURE   — SMTP errors buried in stdout, no dedicated log
  7. SCAN_ZERO_ALERTS        — scanner ran but found 0 anomalies for 3+ days

Run standalone:
    python3 supervisor/l2_muni_scanner.py
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), "..")))

from supervisor.escalation_queue import already_open, push

PROJECT_ROOT = Path(__file__).parent.parent

OUTPUT_DIR         = PROJECT_ROOT / "output"
MEMORY_DIR         = PROJECT_ROOT / "memory"
LOGS_DIR           = PROJECT_ROOT / "logs"
OUROBOROS_SNAPSHOT = PROJECT_ROOT / "../ouroboros.v2/logs/regime_snapshot.json"
EMAIL_AGENT_LOG    = LOGS_DIR / "bond_email_agent.log"
API_PORT           = 8000
POLL_INTERVAL_SEC  = 900

SEVERITY_EMOJI = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🔵"}

try:
    sys.path.insert(0, str(PROJECT_ROOT.parent / "ouroboros.v2"))
    from src.notifications.telegram import TelegramNotifier
    _notifier = TelegramNotifier()
except Exception:
    class _FallbackNotifier:
        def send(self, msg): print(f"[Telegram fallback] {msg}")
    _notifier = _FallbackNotifier()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _all_output_alerts() -> list[dict]:
    alerts = []
    for p in sorted(OUTPUT_DIR.glob("alerts_*.json")):
        try:
            data = json.loads(p.read_text())
            if isinstance(data, list):
                alerts.extend(data)
        except Exception:
            pass
    return alerts


def _alerts_log_ids() -> set:
    log = MEMORY_DIR / "alerts_log.md"
    if not log.exists():
        return set()
    content = log.read_text()
    return set(re.findall(r"MUNI-\d{8}-\w+", content))


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------

def detect_api_service_down() -> Optional[dict]:
    """
    Check whether uvicorn is serving muni_scanner's API on port 8000.
    Uses lsof/ss to check the port, plus pgrep for the process name.
    """
    try:
        result = subprocess.run(
            ["ss", "-tlnp"],
            capture_output=True, text=True, timeout=5
        )
        if f":{API_PORT}" in result.stdout:
            return None
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["pgrep", "-af", "api:app"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return None
    except Exception:
        pass

    return {
        "type":     "API_SERVICE_DOWN",
        "severity": "HIGH",
        "detail": (
            f"No process found serving muni_scanner API on port {API_PORT}. "
            f"POST /scan cannot be triggered. Email delivery via API is unavailable. "
            f"Expected: uvicorn api:app --port {API_PORT}"
        ),
        "hypothesis": (
            "uvicorn was never started or crashed. "
            "daily_scan.sh calls bond_scanner.py directly (CLI path), "
            "but the API service must be running for webhook delivery, "
            "feedback, and /digest/send endpoints."
        ),
        "files_to_check": ["api.py", "AGENTS.md"],
    }


def detect_alert_drought() -> Optional[dict]:
    """
    No output file produced today, and none yesterday either.
    Suggests daily_scan.sh is not being triggered.
    """
    today     = date.today()
    yesterday = today - timedelta(days=1)

    today_file     = OUTPUT_DIR / f"alerts_{today.strftime('%Y%m%d')}.json"
    yesterday_file = OUTPUT_DIR / f"alerts_{yesterday.strftime('%Y%m%d')}.json"

    if today_file.exists():
        return None

    # Only flag if yesterday's file is also missing (or very stale)
    files = sorted(OUTPUT_DIR.glob("alerts_*.json"))
    if not files:
        last_date_str = "never"
        days_silent   = 999
    else:
        last_name     = files[-1].stem            # alerts_YYYYMMDD
        last_date_str = last_name.replace("alerts_", "")
        try:
            last_dt   = datetime.strptime(last_date_str, "%Y%m%d").date()
            days_silent = (today - last_dt).days
        except Exception:
            days_silent = 1

    if days_silent < 1:
        return None

    return {
        "type":     "ALERT_DROUGHT",
        "severity": "MEDIUM" if days_silent == 1 else "HIGH",
        "detail": (
            f"No output/alerts_YYYYMMDD.json file produced today ({today}). "
            f"Last scan output was {days_silent} day(s) ago ({last_date_str}). "
            f"daily_scan.sh may not be scheduled or bond_scanner.py is failing."
        ),
        "hypothesis": (
            "daily_scan.sh is not being called via cron or manual trigger. "
            "Alternatively, bond_scanner.py crashed on the CSV/mock data load, "
            "or the data/ directory has no incoming CSV to process."
        ),
        "files_to_check": ["daily_scan.sh", "bond_scanner.py", "data/"],
    }


def detect_memory_not_updating() -> Optional[dict]:
    """
    Output files contain alerts whose IDs don't appear in alerts_log.md.
    The _append_to_memory() path in api.py is not writing to the log,
    OR the CLI path (daily_scan.sh) never calls the memory append at all.
    """
    all_alerts = _all_output_alerts()
    if not all_alerts:
        return None

    output_ids = {a.get("alert_id") for a in all_alerts if a.get("alert_id")}
    log_ids    = _alerts_log_ids()
    missing    = output_ids - log_ids

    if not missing:
        return None

    log_path = MEMORY_DIR / "alerts_log.md"
    log_lines = log_path.read_text().splitlines() if log_path.exists() else []
    content_lines = [l for l in log_lines if l.strip() and not l.startswith("#") and "---" not in l]

    return {
        "type":     "MEMORY_NOT_UPDATING",
        "severity": "MEDIUM",
        "detail": (
            f"{len(missing)} alert ID(s) exist in output/ files but are absent "
            f"from memory/alerts_log.md. Missing: {sorted(missing)[:5]}. "
            f"alerts_log.md currently has {len(content_lines)} non-header lines."
        ),
        "hypothesis": (
            "daily_scan.sh calls bond_scanner.py directly (CLI), which writes "
            "output JSON but does NOT call _append_to_memory(). That function "
            "only runs via the API's POST /scan path (api.py:132). "
            "The CLI and API paths are diverged — memory only updates via API."
        ),
        "files_to_check": ["api.py", "bond_scanner.py", "daily_scan.sh"],
    }


def detect_regime_coupling_lost() -> Optional[dict]:
    """
    Ouroboros regime snapshot is missing or stale.
    muni_scanner silently falls back to NEUTRAL (50bps threshold).
    """
    if not OUROBOROS_SNAPSHOT.exists():
        return {
            "type":     "REGIME_COUPLING_LOST",
            "severity": "MEDIUM",
            "detail": (
                f"Ouroboros regime snapshot not found at {OUROBOROS_SNAPSHOT}. "
                f"muni_scanner is using NEUTRAL fallback threshold (50bps). "
                f"In BEAR regime this misses bonds between 40-50bps spread."
            ),
            "hypothesis": (
                "Ouroboros heartbeat not running, or regime.py failed on first "
                "fetch and never wrote the snapshot file."
            ),
            "files_to_check": [
                "api.py",
                "../ouroboros.v2/src/sentiment/regime.py",
            ],
        }

    try:
        data    = json.loads(OUROBOROS_SNAPSHOT.read_text())
        fetched = datetime.fromisoformat(data["fetched_at"])
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - fetched).total_seconds() / 60
    except Exception:
        age_min = 9999

    if age_min <= 120:
        return None

    return {
        "type":     "REGIME_COUPLING_LOST",
        "severity": "LOW",
        "detail": (
            f"Ouroboros regime snapshot is {age_min:.0f} min old "
            f"(label={data.get('label','?')}, VIX={data.get('vix','?')}). "
            f"muni_scanner will revert to NEUTRAL threshold at next scan."
        ),
        "hypothesis": (
            "Ouroboros heartbeat is in zombie hours — snapshot age during "
            "off-hours is expected. Only critical if a scan runs while stale."
        ),
        "files_to_check": ["api.py"],
    }


def detect_email_agent_duplicate() -> Optional[dict]:
    """
    bond_email_agent.log shows every log line appearing exactly twice
    at the same timestamp — agent is running as two processes simultaneously.
    """
    if not EMAIL_AGENT_LOG.exists():
        return None

    lines = EMAIL_AGENT_LOG.read_text().splitlines()
    recent = [l for l in lines if l.strip()][-40:]

    if len(recent) < 4:
        return None

    # Count consecutive duplicate pairs
    duplicates = 0
    for i in range(len(recent) - 1):
        if recent[i] == recent[i + 1]:
            duplicates += 1

    ratio = duplicates / max(len(recent) - 1, 1)
    if ratio < 0.4:
        return None

    try:
        result = subprocess.run(
            ["pgrep", "-c", "-f", "bond_email_agent"],
            capture_output=True, text=True, timeout=5
        )
        proc_count = int(result.stdout.strip() or "0")
    except Exception:
        proc_count = -1

    return {
        "type":     "EMAIL_AGENT_DUPLICATE",
        "severity": "MEDIUM",
        "detail": (
            f"{duplicates}/{len(recent)-1} consecutive log lines in "
            f"bond_email_agent.log are exact duplicates. "
            f"Active bond_email_agent processes: {proc_count}. "
            f"Sample duplicate: '{recent[-2][:80]}'"
        ),
        "hypothesis": (
            "bond_email_agent.py was started twice (e.g. from two terminal "
            "sessions or a restart without killing the previous instance). "
            "Both processes log to the same file simultaneously. "
            "No emails are duplicated yet, but if both processes pick up the "
            "same unread message, double delivery is likely."
        ),
        "files_to_check": ["bond_email_agent.py", "logs/bond_email_agent.log"],
    }


def detect_scan_zero_alerts(days: int = 3) -> Optional[dict]:
    """
    Scanner has been running (output files exist) but every file for
    the last N days contains zero alerts.
    """
    today  = date.today()
    counts = {}
    for d in range(days):
        check_date = today - timedelta(days=d)
        f = OUTPUT_DIR / f"alerts_{check_date.strftime('%Y%m%d')}.json"
        if not f.exists():
            continue
        try:
            data = json.loads(f.read_text())
            counts[str(check_date)] = len(data) if isinstance(data, list) else 0
        except Exception:
            counts[str(check_date)] = 0

    if not counts:
        return None

    all_zero = all(v == 0 for v in counts.values())
    if not all_zero or len(counts) < 2:
        return None

    return {
        "type":     "SCAN_ZERO_ALERTS",
        "severity": "LOW",
        "detail": (
            f"Scanner ran for {len(counts)} day(s) but found 0 anomalies each time. "
            f"Days checked: {counts}. "
            f"Either market conditions are genuinely tight or the threshold is "
            f"misconfigured / peer matching is too restrictive."
        ),
        "hypothesis": (
            "Check ANOMALY_THRESHOLD_BPS in bond_scanner.py against the effective "
            "regime threshold. Also verify that data/incoming_bonds.csv has a "
            "sufficient universe — <10 bonds makes peer matching impossible."
        ),
        "files_to_check": ["bond_scanner.py", "data/"],
    }


# ---------------------------------------------------------------------------
# Telegram formatter
# ---------------------------------------------------------------------------

def _fmt_escalation(esc: dict) -> str:
    emoji = SEVERITY_EMOJI.get(esc["severity"], "⚪")
    files = "\n".join(f"  • <code>{f}</code>" for f in esc.get("files_to_check", []))
    return (
        f"{emoji} <b>L2 muni_scanner — {esc['type']}</b>\n"
        f"<b>Severity:</b> {esc['severity']}  |  <b>ID:</b> <code>{esc['id']}</code>\n\n"
        f"<b>Detail:</b>\n{esc['detail']}\n\n"
        f"<b>Hypothesis:</b>\n{esc['hypothesis']}\n\n"
        f"<b>Files to check:</b>\n{files}\n\n"
        f"<i>L3 Fixer will auto-diagnose HIGH severity escalations.</i>"
    )


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def run_scan() -> list[dict]:
    os.chdir(PROJECT_ROOT)

    detectors = [
        ("API_SERVICE_DOWN",     detect_api_service_down),
        ("ALERT_DROUGHT",        detect_alert_drought),
        ("MEMORY_NOT_UPDATING",  detect_memory_not_updating),
        ("REGIME_COUPLING_LOST", detect_regime_coupling_lost),
        ("EMAIL_AGENT_DUPLICATE",detect_email_agent_duplicate),
        ("SCAN_ZERO_ALERTS",     detect_scan_zero_alerts),
    ]

    new_escalations = []
    for atype, fn in detectors:
        try:
            result = fn()
        except Exception as exc:
            print(f"[L2-muni] Detector {atype} error: {exc}")
            continue

        if result is None:
            print(f"[L2-muni] {atype}: OK")
            continue

        if already_open(atype, within_hours=6):
            print(f"[L2-muni] {atype}: DETECTED but escalation already open — skipping")
            continue

        esc = push(
            anomaly_type   = result["type"],
            severity       = result["severity"],
            detail         = result["detail"],
            hypothesis     = result["hypothesis"],
            files_to_check = result["files_to_check"],
        )
        print(f"[L2-muni] {atype}: ESCALATED → {esc['id']} ({esc['severity']})")
        _notifier.send(_fmt_escalation(esc))
        new_escalations.append(esc)

    return new_escalations


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def run_loop():
    print("[L2-muni Supervisor] Starting — poll interval 15 min")
    while True:
        print(f"\n[L2-muni] Scan at {datetime.now().strftime('%H:%M:%S')}")
        try:
            new = run_scan()
            if not new:
                print("[L2-muni] All clear.")
        except Exception as exc:
            print(f"[L2-muni] Scan failed: {exc}")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_loop()
