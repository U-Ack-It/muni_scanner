"""
EMMA Feed Monitor — polls MSRB's EMMA for new municipal bond disclosures
and primary market filings that match our alert criteria.

Checks every 4 hours. New filings matching high-yield or anomaly keywords
are added to the bond scanner queue and trigger a Telegram alert.

Usage:
    python3 emma_monitor.py --dry-run
    python3 emma_monitor.py
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DRY_RUN    = "--dry-run" in sys.argv
LOG_FILE   = Path(__file__).parent / "logs/emma_monitor.log"
SEEN_FILE  = Path(__file__).parent / "logs/emma_seen.json"
DATA_DIR   = Path(__file__).parent / "data"

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# EMMA public RSS feeds
EMMA_FEEDS = [
    # New issue primary market disclosures
    "https://emma.msrb.org/rss/PrimaryMarketDisclosure.aspx",
    # Continuing disclosures (issuer filings)
    "https://emma.msrb.org/rss/ContinuingDisclosure.aspx",
]

# Keywords that flag a filing as worth investigating
ALERT_KEYWORDS = [
    "hospital", "revenue bond", "general obligation", "tax exempt",
    "refinancing", "default", "downgrade", "rating", "amendment",
    "material event", "failure to pay", "bankruptcy"
]

# High-yield states of interest
FOCUS_STATES = ["LA", "IL", "PR", "NJ", "CT", "KY", "PA", "NY", "CA", "TX", "FL"]


def _log(msg: str):
    LOG_FILE.parent.mkdir(exist_ok=True)
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def _load_seen() -> set:
    if SEEN_FILE.exists():
        return set(json.loads(SEEN_FILE.read_text()))
    return set()


def _save_seen(seen: set):
    SEEN_FILE.parent.mkdir(exist_ok=True)
    SEEN_FILE.write_text(json.dumps(list(seen)))


def _telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=5,
        )
    except Exception:
        pass


def _is_relevant(entry: dict) -> bool:
    text = (entry.get("title", "") + " " + entry.get("summary", "")).lower()
    keyword_match = any(k in text for k in ALERT_KEYWORDS)
    state_match   = any(f" {s} " in f" {text.upper()} " for s in FOCUS_STATES)
    return keyword_match or state_match


def _extract_cusip(text: str) -> str:
    import re
    # CUSIP is 9 alphanumeric chars, often labeled in EMMA entries
    match = re.search(r'\b([0-9A-Z]{9})\b', text.upper())
    return match.group(1) if match else ""


def main():
    seen    = _load_seen()
    new_cnt = 0

    for feed_url in EMMA_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
        except Exception as e:
            _log(f"Feed error {feed_url}: {e}")
            continue

        for entry in feed.entries:
            uid = entry.get("id") or entry.get("link", "")
            if uid in seen:
                continue

            seen.add(uid)

            if not _is_relevant(entry):
                continue

            title   = entry.get("title", "")
            link    = entry.get("link", "")
            summary = entry.get("summary", "")[:300]
            cusip   = _extract_cusip(title + " " + summary)
            pub     = entry.get("published", "")

            _log(f"  NEW: {title[:80]}")
            new_cnt += 1

            if not DRY_RUN:
                msg = (
                    f"📋 *EMMA Filing*\n"
                    f"{title}\n"
                    + (f"CUSIP: `{cusip}`\n" if cusip else "")
                    + f"_{pub}_\n"
                    f"[View on EMMA]({link})"
                )
                _telegram(msg)

    _save_seen(seen)
    _log(f"New relevant filings: {new_cnt}")


if __name__ == "__main__":
    main()
