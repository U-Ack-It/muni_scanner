"""
L2 Supervisor — BondAnomaly ($299/month RIA outreach SaaS)

Monitors the business layer built on muni_scanner:
  ria_outreach.py, ria_followup.py, prospect_hunter.py, outreach_log.csv

Detectors:
  1. OUTREACH_DUPLICATE     — same email sent >1x within 48h (no dedup guard in outreach script)
  2. LOG_CORRUPTION         — malformed rows in outreach_log.csv (wrong field in sent_at column)
  3. FOLLOWUP_CRON_BROKEN   — ria_followup.py cron missing `cd` prefix; runs from wrong dir
  4. NUDGE_OVERDUE          — prospects >96h since outreach with no nudge sent
  5. PROSPECT_QUALITY_DRIFT — hunter adding generic/compliance emails instead of RIA decision makers
  6. NO_CONVERSION_TRACKING — outreach_log.csv has no 'converted' column; no subscriber visibility
  7. STRIPE_UNTRACKED       — no code reads Stripe API to confirm active subscribers or revenue
"""

import csv
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from supervisor.escalation_queue import already_open, push

PROJECT_ROOT  = Path(__file__).parent.parent
OUTREACH_LOG  = PROJECT_ROOT / "outreach_log.csv"
PROSPECTS_CSV = PROJECT_ROOT / "prospects.csv"
OUTREACH_PY   = PROJECT_ROOT / "ria_outreach.py"
FOLLOWUP_PY   = PROJECT_ROOT / "ria_followup.py"
HUNTER_PY     = PROJECT_ROOT / "prospect_hunter.py"
HUNTER_LOG    = PROJECT_ROOT / "logs" / "prospect_hunter.log"

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

def _load_log() -> list[dict]:
    if not OUTREACH_LOG.exists():
        return []
    try:
        return list(csv.DictReader(open(OUTREACH_LOG)))
    except Exception:
        return []


def _load_prospects() -> list[dict]:
    if not PROSPECTS_CSV.exists():
        return []
    try:
        return list(csv.DictReader(open(PROSPECTS_CSV)))
    except Exception:
        return []


def _parse_dt(s: str) -> Optional[datetime]:
    if not s or not s.strip():
        return None
    try:
        dt = datetime.fromisoformat(s.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------

def detect_outreach_duplicate() -> Optional[dict]:
    """
    Same email appears in outreach_log.csv more than once with valid sent_at
    values within 48 hours of each other. ria_outreach.py reads prospects.csv
    and sends to everyone without checking the log — running it twice duplicates.
    """
    rows = _load_log()
    by_email: dict[str, list[datetime]] = defaultdict(list)
    for r in rows:
        dt = _parse_dt(r.get("sent_at", ""))
        if dt:
            by_email[r["email"]].append(dt)

    dupes = {}
    for email, times in by_email.items():
        if len(times) < 2:
            continue
        times.sort()
        # Check if any two sends are within 48h
        for i in range(len(times) - 1):
            gap_h = (times[i+1] - times[i]).total_seconds() / 3600
            if gap_h < 48:
                dupes[email] = (len(times), gap_h)
                break

    if not dupes:
        return None

    sample = list(dupes.items())[:5]
    lines  = [f"  {e}: {n}x (gap {h:.1f}h)" for e, (n, h) in sample]

    return {
        "type":     "OUTREACH_DUPLICATE",
        "severity": "HIGH",
        "detail": (
            f"{len(dupes)} prospect(s) received duplicate outreach emails:\n"
            + "\n".join(lines)
            + f"\n{'  ...' if len(dupes) > 5 else ''}"
            + f"\nRoot cause: ria_outreach.py has no guard against re-sending "
            f"to addresses already in outreach_log.csv."
        ),
        "hypothesis": (
            "ria_outreach.py reads all rows from prospects.csv and sends without "
            "cross-checking outreach_log.csv. Running the script twice (or from "
            "two terminals within the same session) sends the same email to every "
            "prospect again. The fix is to load the log and skip any email already "
            "present before calling the Gmail API."
        ),
        "files_to_check": ["ria_outreach.py", "outreach_log.csv"],
    }


def detect_log_corruption() -> Optional[dict]:
    """
    Rows in outreach_log.csv where sent_at is not a valid ISO timestamp —
    e.g. a name value ('there') landed in the sent_at column due to a CSV
    quoting/field-count bug.
    """
    rows = _load_log()
    corrupt = []
    for r in rows:
        raw = r.get("sent_at", "").strip()
        if not raw:
            continue
        if _parse_dt(raw) is None:
            corrupt.append(r)

    if not corrupt:
        return None

    sample = corrupt[:3]
    lines  = [f"  email={r.get('email','?')} sent_at='{r.get('sent_at','?')}'" for r in sample]

    return {
        "type":     "LOG_CORRUPTION",
        "severity": "MEDIUM",
        "detail": (
            f"{len(corrupt)} row(s) in outreach_log.csv have a non-ISO sent_at value:\n"
            + "\n".join(lines)
            + "\nAffected rows are invisible to nudge eligibility logic — "
            f"nudge_sent timestamps on corrupt rows may fire incorrectly."
        ),
        "hypothesis": (
            "prospect_hunter.py appends newly discovered prospects and "
            "may trigger outreach inline. If the contact_name contains a comma "
            "and the CSV write is unquoted, field values shift right — "
            "a name like 'LLC' lands in sent_at. "
            "Alternatively, 'there' is the name fallback in ria_outreach.py "
            "EMAIL_BODY.format() leaking into the log write call."
        ),
        "files_to_check": ["ria_outreach.py", "prospect_hunter.py", "outreach_log.csv"],
    }


def detect_followup_cron_broken() -> Optional[dict]:
    """
    ria_followup.py cron entry has no `cd` prefix. Cron runs from $HOME,
    but the log redirect is a relative path ('logs/ria_followup.log') which
    resolves to ~/logs/ria_followup.log, not the muni_scanner logs dir.
    """
    try:
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, timeout=5
        )
        cron = result.stdout
    except Exception:
        return None

    # Find the ria_followup line
    followup_lines = [l for l in cron.splitlines() if "ria_followup" in l]
    if not followup_lines:
        return {
            "type":     "FOLLOWUP_CRON_BROKEN",
            "severity": "HIGH",
            "detail": (
                "No cron entry found for ria_followup.py. "
                "The 72-hour nudge is never automatically sent. "
                "All prospect follow-up requires manual invocation."
            ),
            "hypothesis": "Cron entry was removed or never created for ria_followup.",
            "files_to_check": ["ria_followup.py"],
        }

    for line in followup_lines:
        # Good pattern: has `cd /path &&` or full path to script
        has_cd       = re.search(r"cd\s+/", line)
        has_fullpath = re.search(r"/home/.+/ria_followup\.py", line)
        if not has_cd and not has_fullpath:
            return {
                "type":     "FOLLOWUP_CRON_BROKEN",
                "severity": "HIGH",
                "detail": (
                    f"ria_followup.py cron entry has no working-directory prefix:\n"
                    f"  '{line.strip()}'\n"
                    f"Cron runs from $HOME by default. The relative log redirect "
                    f"'logs/ria_followup.log' writes to ~/logs/ (or fails). "
                    f"SEND_LOG uses Path(__file__).parent which resolves correctly, "
                    f"but TOKEN_PATH = Path.home() / 'gallerysense/token.pickle' "
                    f"may not exist if gallerysense is not in $HOME."
                ),
                "hypothesis": (
                    "The cron was created with 'python3 ria_followup.py' without a "
                    "preceding 'cd /home/u-ack-it/Projects/muni_scanner &&'. "
                    "Output goes to ~/logs/ria_followup.log (may not exist), "
                    "and if the Gmail token path differs, authentication fails silently."
                ),
                "files_to_check": ["ria_followup.py"],
            }

    return None


def detect_nudge_overdue() -> Optional[dict]:
    """
    Prospects who received initial outreach >96h ago with no nudge sent.
    At 72h the followup script should have fired; >96h suggests the cron failed.
    """
    rows    = _load_log()
    now     = datetime.now(timezone.utc)
    cutoff  = now - timedelta(hours=96)

    # Deduplicate: one entry per email (first outreach)
    seen:  dict[str, dict] = {}
    for r in rows:
        e = r["email"]
        if e not in seen:
            seen[e] = r

    overdue = []
    for r in seen.values():
        sent_at  = _parse_dt(r.get("sent_at", ""))
        nudge    = r.get("nudge_sent", "").strip()
        if sent_at and sent_at < cutoff and not nudge:
            age_h = (now - sent_at).total_seconds() / 3600
            overdue.append((r.get("firm", r["email"]), round(age_h / 24, 1)))

    if not overdue:
        return None

    lines = [f"  {firm}: {days}d since outreach" for firm, days in sorted(overdue, key=lambda x: -x[1])[:8]]
    return {
        "type":     "NUDGE_OVERDUE",
        "severity": "MEDIUM",
        "detail": (
            f"{len(overdue)} prospect(s) past 96h with no nudge sent:\n"
            + "\n".join(lines)
        ),
        "hypothesis": (
            "ria_followup.py cron may be failing silently (see FOLLOWUP_CRON_BROKEN). "
            "Every 24h without a nudge after 72h is a missed conversion opportunity "
            "at $299/month per firm."
        ),
        "files_to_check": ["ria_followup.py", "outreach_log.csv"],
    }


def detect_prospect_quality_drift() -> Optional[dict]:
    """
    prospect_hunter.py is adding generic/compliance emails from large asset
    managers instead of boutique RIA decision-makers. Detects by checking
    for known generic prefixes or known large-firm domains in prospects.csv.
    """
    prospects = _load_prospects()
    if not prospects:
        return None

    generic_prefixes = ("info@", "compliance", "contact", "service@",
                        "team@", "support@", "admin@", "help@")
    large_firm_domains = (
        "gs.com", "blackrock.com", "vanguard.com", "fidelity.com",
        "jpmorgan.com", "morganstanley.com", "dws.com", "alliancebernstein.com",
        "franklintempleton.com", "loomissayles.com", "pimco.com",
    )

    bad = []
    for p in prospects:
        email = p.get("contact_email", "").lower().strip()
        if not email:
            continue
        is_generic = any(email.startswith(pfx) for pfx in generic_prefixes)
        is_large   = any(email.endswith(domain) for domain in large_firm_domains)
        if is_generic or is_large:
            bad.append((p.get("firm", "?"), email,
                        "generic prefix" if is_generic else "large-firm domain"))

    if not bad or len(bad) < 2:
        return None

    lines = [f"  {firm} <{email}> [{reason}]" for firm, email, reason in bad[:6]]
    return {
        "type":     "PROSPECT_QUALITY_DRIFT",
        "severity": "MEDIUM",
        "detail": (
            f"{len(bad)} prospect(s) in prospects.csv are low-quality targets "
            f"(generic email or large asset manager, not boutique RIA):\n"
            + "\n".join(lines)
            + f"\nThese will dilute the $299/month pitch — Goldman Sachs won't subscribe."
        ),
        "hypothesis": (
            "prospect_hunter.py uses Perplexity to discover firms then scrapes "
            "the first email found on the homepage. Large managers have generic "
            "emails (info@, compliance@) and irrelevant decision-makers. "
            "The hunter lacks a quality filter for firm AUM size or email role."
        ),
        "files_to_check": ["prospect_hunter.py", "prospects.csv"],
    }


def detect_no_conversion_tracking() -> Optional[dict]:
    """
    outreach_log.csv has no 'converted' or 'subscriber' column.
    There is no code anywhere that checks whether a prospect subscribed via Stripe.
    """
    rows = _load_log()
    if not rows:
        return None

    # Check if any row has a 'converted' or 'subscribed' column
    fieldnames = set(rows[0].keys()) if rows else set()
    has_conversion_col = bool(fieldnames & {"converted", "subscribed", "stripe_customer"})
    if has_conversion_col:
        return None

    # Check if any code reads Stripe API
    try:
        result = subprocess.run(
            ["grep", "-rl", "stripe", str(PROJECT_ROOT), "--include=*.py"],
            capture_output=True, text=True, timeout=10
        )
        stripe_files = [f for f in result.stdout.strip().splitlines()
                        if "stripe_setup.py" not in f and "stripe_payment_link" not in f]
    except Exception:
        stripe_files = []

    return {
        "type":     "NO_CONVERSION_TRACKING",
        "severity": "MEDIUM",
        "detail": (
            f"outreach_log.csv tracks outreach ({len(rows)} rows) but has no "
            f"'converted', 'subscribed', or 'stripe_customer' column. "
            f"There is no way to know how many prospects became $299/month subscribers. "
            f"Stripe-reading code found in: {stripe_files or 'none'}."
        ),
        "hypothesis": (
            "The Stripe payment link (buy.stripe.com/...) handles checkout, "
            "but no Stripe webhook receiver or API poller exists to match "
            "subscriber emails against the prospect list. "
            "Revenue is invisible to the outreach pipeline."
        ),
        "files_to_check": ["ria_outreach.py", "outreach_log.csv", "stripe_setup.py"],
    }


def detect_stripe_untracked() -> Optional[dict]:
    """
    No code in the project reads the Stripe API to confirm active subscribers,
    MRR, or churn. The $299/month business has no revenue dashboard.
    """
    try:
        result = subprocess.run(
            ["grep", "-rn", r"stripe.Customer\|stripe.Subscription\|stripe.list\|stripe.retrieve",
             str(PROJECT_ROOT), "--include=*.py"],
            capture_output=True, text=True, timeout=10
        )
        if result.stdout.strip():
            return None
    except Exception:
        pass

    # Check if stripe_setup.py does anything useful
    stripe_setup = PROJECT_ROOT / "stripe_setup.py"
    if stripe_setup.exists():
        content = stripe_setup.read_text()
        if "subscription" in content.lower() or "customer.list" in content.lower():
            return None

    return {
        "type":     "STRIPE_UNTRACKED",
        "severity": "LOW",
        "detail": (
            "No code reads the Stripe API for subscription status, MRR, or "
            "customer list. The payment link (buy.stripe.com/14AbIT3Vv08F7fWf13bEA00) "
            "accepts payments but there is no automated way to: "
            "(a) know who subscribed, (b) send them alerts, "
            "(c) track churn, or (d) confirm revenue."
        ),
        "hypothesis": (
            "stripe_setup.py creates the payment link / price objects but does "
            "not set up webhook handling or subscription polling. "
            "A subscriber paying $299/month gets no automated alert delivery — "
            "that delivery step doesn't exist yet."
        ),
        "files_to_check": ["stripe_setup.py", "ria_outreach.py"],
    }


# ---------------------------------------------------------------------------
# Telegram + scan runner
# ---------------------------------------------------------------------------

def _fmt_escalation(esc: dict) -> str:
    emoji = SEVERITY_EMOJI.get(esc["severity"], "⚪")
    files = "\n".join(f"  • <code>{f}</code>" for f in esc.get("files_to_check", []))
    return (
        f"{emoji} <b>L2 BondAnomaly — {esc['type']}</b>\n"
        f"<b>Severity:</b> {esc['severity']}  |  <b>ID:</b> <code>{esc['id']}</code>\n\n"
        f"<b>Detail:</b>\n{esc['detail']}\n\n"
        f"<b>Hypothesis:</b>\n{esc['hypothesis']}\n\n"
        f"<b>Files to check:</b>\n{files}"
    )


def run_scan() -> list[dict]:
    os.chdir(PROJECT_ROOT)

    detectors = [
        ("OUTREACH_DUPLICATE",     detect_outreach_duplicate),
        ("LOG_CORRUPTION",         detect_log_corruption),
        ("FOLLOWUP_CRON_BROKEN",   detect_followup_cron_broken),
        ("NUDGE_OVERDUE",          detect_nudge_overdue),
        ("PROSPECT_QUALITY_DRIFT", detect_prospect_quality_drift),
        ("NO_CONVERSION_TRACKING", detect_no_conversion_tracking),
        ("STRIPE_UNTRACKED",       detect_stripe_untracked),
    ]

    new_escalations = []
    for atype, fn in detectors:
        try:
            result = fn()
        except Exception as exc:
            print(f"[L2-bond] Detector {atype} error: {exc}")
            continue

        if result is None:
            print(f"[L2-bond] {atype}: OK")
            continue

        if already_open(atype, within_hours=6):
            print(f"[L2-bond] {atype}: DETECTED — escalation already open, skipping")
            continue

        esc = push(
            anomaly_type   = result["type"],
            severity       = result["severity"],
            detail         = result["detail"],
            hypothesis     = result["hypothesis"],
            files_to_check = result["files_to_check"],
            project        = "bondanomaly",
        )
        print(f"[L2-bond] {atype}: ESCALATED → {esc['id']} ({esc['severity']})")
        _notifier.send(_fmt_escalation(esc))
        new_escalations.append(esc)

    return new_escalations
