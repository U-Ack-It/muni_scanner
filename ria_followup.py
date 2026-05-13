"""
RIA Follow-up Nudge — 72h after initial outreach with no reply.

Checks outreach_log.csv, skips anyone who already got a nudge.
Sends a short second email with a different hook.

Usage:
    python3 ria_followup.py --dry-run
    python3 ria_followup.py
"""

import base64
import csv
import os
import pickle
import sys
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

TOKEN_PATH  = Path.home() / "gallerysense/token.pickle"
SEND_LOG    = Path(__file__).parent / "outreach_log.csv"
REPLY_TO    = os.getenv("REPORT_EMAIL", "olivierboukli@gmail.com")
SENDER_NAME = "Olivier Boukli"
DRY_RUN     = "--dry-run" in sys.argv
NUDGE_HOURS = 72


SUBJECT = "Re: muni bond anomaly we flagged (+127 bps) — one more thing"

BODY = """\
Hi {first_name},

Just wanted to make sure my note from earlier this week landed — \
inbox filters sometimes catch these.

Quick summary of what I sent: our scanner flagged a Jefferson Parish LA \
hospital revenue bond at 5.41% YTW — 127 bps above the A2/A peer average \
for the 2035 maturity bucket. Full alert with EMMA link attached to the \
prior email.

We're seeing 5-10 alerts like this per week right now across GO and revenue \
paper. The service is $299/month, no long contract.

If it's not a fit I completely understand — just reply and I'll stop following up. \
If you'd like the next alert before it goes to the broader list, happy to send it.

Subscribe directly: https://buy.stripe.com/14AbIT3Vv08F7fWf13bEA00
More info: https://bondanomaly.com

{sender}
{reply_to}
+1 (786) 716-8785
"""


def _gmail_service():
    with open(TOKEN_PATH, "rb") as f:
        creds = pickle.load(f)
    if creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
        with open(TOKEN_PATH, "wb") as f:
            pickle.dump(creds, f)
    from googleapiclient.discovery import build
    return build("gmail", "v1", credentials=creds)


def _send(service, to: str, subject: str, body: str):
    msg = MIMEMultipart()
    msg["to"]       = to
    msg["subject"]  = subject
    msg["Reply-To"] = REPLY_TO
    msg.attach(MIMEText(body, "plain"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


def _update_log(rows: list[dict]):
    with open(SEND_LOG, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "firm", "name", "sent_at", "nudge_sent"])
        writer.writeheader()
        writer.writerows(rows)


def main():
    if not SEND_LOG.exists():
        print("No outreach log found — run ria_outreach.py first.")
        return

    with open(SEND_LOG) as f:
        rows = list(csv.DictReader(f))

    service = None if DRY_RUN else _gmail_service()
    sent = skipped = 0
    now  = datetime.now(timezone.utc)

    # Guard: outreach_log.csv may contain duplicate rows for the same email
    # (legacy data from before ria_outreach.py had its dedup guard). Make sure
    # we only nudge each address once per pass. We mutate `rows` in place so
    # _update_log() persists nudge_sent correctly for the first occurrence.
    seen_emails: set[str] = set()

    for row in rows:
        addr = (row.get("email") or "").strip().lower()
        if addr and addr in seen_emails:
            skipped += 1
            continue
        if addr:
            seen_emails.add(addr)

        if row.get("nudge_sent"):
            skipped += 1
            continue

        sent_at = row.get("sent_at", "")
        try:
            sent_dt = datetime.fromisoformat(sent_at)
        except Exception:
            skipped += 1
            continue

        hours_elapsed = (now - sent_dt).total_seconds() / 3600
        if hours_elapsed < NUDGE_HOURS:
            skipped += 1
            continue

        email      = row["email"]
        firm       = row["firm"]
        name       = row.get("name", "there")
        first_name = name.split()[0] if name and name != "there" else "there"

        body = BODY.format(
            first_name=first_name,
            sender=SENDER_NAME,
            reply_to=REPLY_TO,
        )

        if DRY_RUN:
            print(f"\n[DRY RUN] → {name} <{email}> ({firm})")
            print(f"  {body[:200]}...")
        else:
            _send(service, email, SUBJECT, body)
            row["nudge_sent"] = now.isoformat()
            print(f"  → Nudge sent to {name} <{email}> ({firm})")

        sent += 1

    if not DRY_RUN:
        _update_log(rows)

    print(f"\nNudges sent: {sent}  Skipped: {skipped}")


if __name__ == "__main__":
    main()
