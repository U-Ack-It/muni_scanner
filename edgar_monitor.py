"""
SEC EDGAR Monitor — watches for new filings by municipal bond issuers
already in our BondAnomaly alert history.

Uses the free EDGAR full-text search API (no auth required).
Runs every 6 hours. Fires Telegram alert when a relevant filing appears.

Usage:
    python3 edgar_monitor.py --dry-run
    python3 edgar_monitor.py
"""

import json
import os
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DRY_RUN    = "--dry-run" in sys.argv
LOG_FILE   = Path(__file__).parent / "logs/edgar_monitor.log"
SEEN_FILE  = Path(__file__).parent / "logs/edgar_seen.json"
ALERTS_DIR = Path(__file__).parent / "output"

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

EDGAR_SEARCH  = "https://efts.sec.gov/LATEST/search-index"
EDGAR_EFTS    = "https://efts.sec.gov/LATEST/search-index"
EDGAR_BROWSE  = "https://www.sec.gov/cgi-bin/browse-edgar"

# Filing types relevant to muni bond credit risk
RELEVANT_FORMS = {"8-K", "10-K", "10-Q", "15", "15-12G", "ARS", "NT 10-K"}

# Keywords in filing text that signal credit events
CREDIT_KEYWORDS = [
    "material adverse", "default", "bankruptcy", "insolvency",
    "downgrade", "rating action", "covenant violation", "failure to pay",
    "revenue shortfall", "budget deficit", "pension obligation",
    "going concern", "restructuring"
]


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


def _load_alert_issuers() -> list[str]:
    """Extract issuer names from all past BondAnomaly alerts."""
    issuers = []
    for f in ALERTS_DIR.glob("*.json"):
        try:
            alerts = json.loads(f.read_text())
            for a in alerts:
                issuer = a.get("issuer", "")
                if issuer and issuer not in issuers:
                    issuers.append(issuer)
        except Exception:
            continue
    return issuers


EDGAR_UA = "Mozilla/5.0 (compatible; BondAnomaly; mailto:olivierboukli@gmail.com)"

def _search_edgar(query: str, days_back: int = 2) -> list[dict]:
    start_date = (date.today() - timedelta(days=days_back)).isoformat()
    try:
        resp = requests.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"q": f'"{query}"', "startdt": start_date,
                    "enddt": date.today().isoformat()},
            headers={"User-Agent": EDGAR_UA},
            timeout=15,
        )
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])
        results = []
        for h in hits:
            s = h.get("_source", {})
            results.append({
                "entity_name": ", ".join(s.get("display_names", ["?"])),
                "form_type":   s.get("form", s.get("root_forms", ["?"])[0] if s.get("root_forms") else "?"),
                "file_date":   s.get("file_date", ""),
                "adsh":        s.get("adsh", ""),
            })
        return results
    except Exception as e:
        _log(f"  EDGAR search error: {e}")
        return []


def main():
    seen    = _load_seen()
    issuers = _load_alert_issuers()
    new_cnt = 0

    if not issuers:
        # Fall back to general muni credit event search
        issuers = ["municipal bond", "general obligation", "revenue bond"]

    _log(f"Monitoring {len(issuers)} issuers in EDGAR...")

    for issuer in issuers[:20]:  # cap at 20 to avoid rate limits
        results = _search_edgar(issuer)
        for r in results:
            uid = f"{r.get('entity_name')}-{r.get('file_date')}-{r.get('form_type')}"
            if uid in seen:
                continue

            seen.add(uid)
            form_type   = r.get("form_type", "")
            entity_name = r.get("entity_name", "")
            file_date   = r.get("file_date", "")

            if form_type not in RELEVANT_FORMS:
                continue

            _log(f"  EDGAR: {entity_name} filed {form_type} on {file_date}")
            new_cnt += 1

            if not DRY_RUN:
                adsh = r.get("adsh", "").replace("-", "")
                edgar_url = (
                    f"https://www.sec.gov/Archives/edgar/data/0/{adsh[:10]}/{r.get('adsh','')}-index.htm"
                    if r.get("adsh") else
                    f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={requests.utils.quote(entity_name)}&type={form_type}&count=5"
                )
                msg = (
                    f"📄 *SEC EDGAR Filing*\n"
                    f"*{entity_name}*\n"
                    f"Form: `{form_type}` · Filed: {file_date}\n"
                    f"[Search EDGAR]({edgar_url})"
                )
                _telegram(msg)

    _save_seen(seen)
    _log(f"New relevant filings: {new_cnt}")


if __name__ == "__main__":
    main()
