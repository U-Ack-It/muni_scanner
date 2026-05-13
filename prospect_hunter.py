"""
Prospect Hunter — autonomously finds new boutique muni RIA firms and adds
them to prospects.csv, then triggers outreach for new entries only.

Runs on a cron schedule. Uses Perplexity to discover new firms, deduplicates
against existing prospects, appends new ones, and sends the outreach email.

Usage:
    python3 prospect_hunter.py --dry-run
    python3 prospect_hunter.py
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

PROSPECTS_CSV = Path(__file__).parent / "prospects.csv"
LOG_FILE      = Path(__file__).parent / "logs/prospect_hunter.log"
DRY_RUN       = "--dry-run" in sys.argv
PPLX_KEY      = os.getenv("PERPLEXITY_API_KEY", "")

# Rotate search queries so each run discovers different firms
QUERIES = [
    "boutique municipal bond RIA firm tax-exempt fixed income portfolio manager email contact 2025",
    "independent muni bond manager separately managed accounts HNW advisor contact email",
    "boutique tax-exempt fixed income firm family office wealth manager email 2025 2026",
    "municipal bond separately managed account boutique RIA firm director portfolio manager email",
    "independent fixed income investment manager muni bonds high net worth contact email site:firm.com",
    "boutique muni bond fund manager state-specific tax-exempt income advisor email contact",
    "community bank trust department municipal bond manager contact email",
    "regional broker dealer municipal bond desk boutique firm contact email",
]


def _log(msg: str):
    LOG_FILE.parent.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def _load_existing() -> set[str]:
    if not PROSPECTS_CSV.exists():
        return set()
    with open(PROSPECTS_CSV) as f:
        rows = list(csv.DictReader(f))
    return {r["firm"].lower().strip() for r in rows if r.get("firm")}


def _load_existing_emails() -> set[str]:
    if not PROSPECTS_CSV.exists():
        return set()
    with open(PROSPECTS_CSV) as f:
        rows = list(csv.DictReader(f))
    return {r["contact_email"].lower().strip() for r in rows if r.get("contact_email")}


def _pick_query() -> str:
    # Rotate by day-of-year so each daily run uses a different query
    idx = datetime.now().timetuple().tm_yday % len(QUERIES)
    return QUERIES[idx]


def _search_perplexity(query: str) -> str:
    if not PPLX_KEY:
        return ""
    try:
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {PPLX_KEY}"},
            json={
                "model": "sonar",
                "messages": [{
                    "role": "user",
                    "content": (
                        f"{query}\n\n"
                        "Return ONLY a JSON array of objects with these fields: "
                        "firm (name), website (their official website URL), focus (what they do), "
                        "state (US state), notes (one sentence about them). "
                        "Return 8-10 boutique firms. No markdown, no explanation, just the JSON array."
                    )
                }],
                "max_tokens": 1200,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        _log(f"Perplexity error: {e}")
        return ""


def _fetch_contact_email(website: str) -> tuple[str, str]:
    """Scrape contact page for an email address. Returns (contact_name, email)."""
    import re
    for path in ["", "/contact", "/contact-us", "/about", "/team", "/about-us"]:
        try:
            url = website.rstrip("/") + path
            r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                continue
            emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", r.text)
            for e in emails:
                # Skip images, fonts, common non-contact emails
                if any(x in e.lower() for x in ["@2x", ".png", ".jpg", "example", "schema", "sentry"]):
                    continue
                return "", e
        except Exception:
            continue
    return "", ""


def _parse_firms(raw: str) -> list[dict]:
    import re
    raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def _enrich_with_emails(firms: list[dict]) -> list[dict]:
    enriched = []
    for firm in firms:
        website = firm.get("website", "")
        if not website:
            continue
        _log(f"  Scraping {website}...")
        contact_name, email = _fetch_contact_email(website)
        if not email:
            _log(f"    No email found")
            continue
        firm["contact_name"]  = contact_name
        firm["contact_email"] = email
        _log(f"    Found: {email}")
        enriched.append(firm)
        time.sleep(1)
    return enriched


def _append_new(new_firms: list[dict], existing_firms: set, existing_emails: set) -> list[dict]:
    added = []
    fieldnames = ["firm", "contact_name", "contact_email", "focus", "state", "notes"]

    file_exists = PROSPECTS_CSV.exists()
    with open(PROSPECTS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for firm in new_firms:
            name  = firm.get("firm", "").strip()
            email = firm.get("contact_email", "").strip().lower()

            if not name or not email:
                continue
            if name.lower() in existing_firms:
                continue
            if email in existing_emails:
                continue
            if not email or "@" not in email:
                continue

            row = {
                "firm":          name,
                "contact_name":  firm.get("contact_name", ""),
                "contact_email": email,
                "focus":         firm.get("focus", ""),
                "state":         firm.get("state", ""),
                "notes":         firm.get("notes", "auto-discovered"),
            }
            writer.writerow(row)
            existing_firms.add(name.lower())
            existing_emails.add(email)
            added.append(row)
            _log(f"  + Added: {name} <{email}>")

    return added


def _send_outreach(new_firms: list[dict]):
    if not new_firms:
        return
    import subprocess
    for firm in new_firms:
        name = firm["firm"]
        _log(f"  → Sending outreach: {name}")
        if not DRY_RUN:
            subprocess.run(
                ["python3", "ria_outreach.py", "--firm", name],
                cwd=Path(__file__).parent,
                capture_output=True,
            )
            time.sleep(2)  # avoid Gmail rate limit


def main():
    _log(f"=== Prospect Hunter run {'[DRY RUN]' if DRY_RUN else ''} ===")

    existing_firms  = _load_existing()
    existing_emails = _load_existing_emails()
    _log(f"Existing prospects: {len(existing_firms)}")

    query = _pick_query()
    _log(f"Query: {query[:80]}...")

    raw   = _search_perplexity(query)
    firms = _parse_firms(raw)
    _log(f"Discovered: {len(firms)} firms from Perplexity")
    firms = _enrich_with_emails(firms)
    _log(f"Firms with emails: {len(firms)}")

    if DRY_RUN:
        for f in firms:
            print(f"  [{f.get('firm')}] {f.get('contact_email')}")
        return

    added = _append_new(firms, existing_firms, existing_emails)
    _log(f"New prospects added: {len(added)}")

    _send_outreach(added)
    _log(f"Done.")


if __name__ == "__main__":
    main()
