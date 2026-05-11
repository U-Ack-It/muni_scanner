"""
Municipal Bond Scanner — FastAPI service

POST /scan                    run scanner, return alerts, fire webhooks
GET  /alerts                  historical alerts (filter: public_only, state, since)
POST /alerts/{id}/feedback    human validates/rejects → updates memory
GET  /memory/summary          accumulated knowledge base
GET  /health                  status + counts
"""

import json
import os
import tempfile
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel

from bond_scanner import (
    ANOMALY_THRESHOLD_BPS,
    MAX_PAR,
    _anonymize_id,
    detect_anomaly,
    load_csv,
    load_mock,
)

app = FastAPI(title="Muni Bond Anomaly Scanner", version="1.0.0")

ALERTS_DIR = Path("output")
MEMORY_DIR = Path("memory")
ALERTS_DIR.mkdir(exist_ok=True)
MEMORY_DIR.mkdir(exist_ok=True)

WEBHOOK_URLS = [u.strip() for u in os.getenv("WEBHOOK_URLS", "").split(",") if u.strip()]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class FeedbackBody(BaseModel):
    valid: bool
    notes: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_saved_alerts() -> list[dict]:
    records = []
    for p in sorted(ALERTS_DIR.glob("alerts_*.json")):
        try:
            records.extend(json.loads(p.read_text()))
        except (json.JSONDecodeError, OSError):
            pass
    return records


def _save_new_alerts(records: list[dict]) -> list[dict]:
    """Merge into today's file, return only genuinely new records."""
    out_path = ALERTS_DIR / f"alerts_{date.today().strftime('%Y%m%d')}.json"
    existing: list[dict] = []
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    existing_ids = {r["alert_id"] for r in existing}
    new = [r for r in records if r["alert_id"] not in existing_ids]
    if new:
        out_path.write_text(json.dumps(existing + new, indent=2))
    return new


def _append_to_memory(alert: dict):
    log_path = MEMORY_DIR / "alerts_log.md"
    entry = (
        f"\n## {alert['alert_id']} — {alert['issuer']} ({alert['state']})\n"
        f"- **Date**: {date.today().isoformat()}\n"
        f"- **CUSIP**: {alert['cusip']} | **anon_id**: `{alert['anon_id']}`\n"
        f"- **Spread**: +{alert['spread_bps']}bps | **YTW**: {alert['ytw']}% | "
        f"**Peer avg**: {alert['peer_avg_ytw']}% ({alert['peer_count']} peers)\n"
        f"- **Rating**: {alert['rating']} | **Par**: ${alert['par_amount']/1e6:.1f}M\n"
        f"- **Public**: {'Yes' if alert['is_public'] else 'No (subscriber)'}\n"
        f"- **Red flag**: {alert.get('red_flag') or 'None'}\n"
        f"- **Validation**: pending\n"
    )
    with open(log_path, "a") as f:
        f.write(entry)


def _update_memory_feedback(alert_id: str, valid: bool, notes: str):
    log_path = MEMORY_DIR / "alerts_log.md"
    if not log_path.exists():
        return
    content = log_path.read_text()
    marker = f"## {alert_id} —"
    idx = content.find(marker)
    if idx == -1:
        return
    suffix = content[idx:]
    validation_line = "- **Validation**: pending"
    replacement = f"- **Validation**: {'✓ valid' if valid else '✗ rejected'}"
    if notes:
        replacement += f" — {notes}"
    updated = suffix.replace(validation_line, replacement, 1)
    log_path.write_text(content[:idx] + updated)


def _digest_issuer_patterns() -> None:
    """Rewrite issuer_patterns.md from all saved alerts."""
    all_alerts = _all_saved_alerts()
    if not all_alerts:
        return

    by_issuer: dict[str, list] = defaultdict(list)
    for a in all_alerts:
        by_issuer[a["issuer"]].append(a)

    lines = [
        "# Issuer Patterns",
        "",
        f"Auto-generated {date.today().isoformat()} from {len(all_alerts)} saved alerts.",
        "",
        "---",
        "",
    ]

    for issuer, alerts in sorted(by_issuer.items()):
        state   = alerts[0].get("state", "?")
        purpose = alerts[0].get("purpose", "")
        spreads = [a["spread_bps"] for a in alerts]
        itype   = "GO" if any(k in purpose for k in ("General Obligation", "GO")) else "Revenue"
        last    = max(a["generated_at"][:10] for a in alerts)

        lines += [
            f"### {issuer} ({state})",
            f"- **Type**: {itype}",
            f"- **Times flagged**: {len(alerts)}",
            f"- **Spread range**: {min(spreads)}bps – {max(spreads)}bps"
            f" (avg {sum(spreads) // len(spreads)}bps)",
            f"- **Public alerts**: {sum(1 for a in alerts if a.get('is_public'))}",
            f"- **Last seen**: {last}",
            "",
        ]

    (MEMORY_DIR / "issuer_patterns.md").write_text("\n".join(lines))


def _fire_webhooks(alerts: list[dict]):
    if not WEBHOOK_URLS or not alerts:
        return
    payload = {"alerts": alerts, "generated_at": datetime.now().isoformat()}
    for url in WEBHOOK_URLS:
        try:
            with httpx.Client(timeout=10) as client:
                client.post(url, json=payload)
        except Exception as exc:
            print(f"[webhook] {url} failed: {exc}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    saved = _all_saved_alerts()
    log_path = MEMORY_DIR / "alerts_log.md"
    return {
        "status": "ok",
        "total_alerts_saved": len(saved),
        "public_alerts": sum(1 for a in saved if a.get("is_public")),
        "memory_log_lines": len(log_path.read_text().splitlines()) if log_path.exists() else 0,
        "webhooks_configured": len(WEBHOOK_URLS),
    }


@app.post("/scan")
async def scan(
    background_tasks: BackgroundTasks,
    file: Optional[UploadFile] = File(None),
    mock: bool = Query(False, description="Use built-in mock bonds"),
    threshold: int = Query(ANOMALY_THRESHOLD_BPS, description="Minimum spread in bps"),
    public_only: bool = Query(False, description="Return only public-tier alerts"),
):
    if mock:
        universe = load_mock()
    elif file:
        content = await file.read()
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        try:
            universe = load_csv(tmp_path)
        finally:
            os.unlink(tmp_path)
    else:
        raise HTTPException(400, "Supply a CSV file or use ?mock=true")

    all_bonds   = [b for b in universe if b.par_amount <= MAX_PAR]
    targets     = [b for b in all_bonds if b.is_target] or all_bonds
    non_targets = [b for b in all_bonds if not b.is_target]

    alerts = []
    for bond in targets:
        alert = detect_anomaly(bond, non_targets)
        if alert and not (public_only and not alert.is_public):
            d = asdict(alert)
            d["anon_id"] = _anonymize_id(alert)
            alerts.append(d)

    new_alerts = _save_new_alerts(alerts) if alerts else []
    for a in new_alerts:
        _append_to_memory(a)
    if new_alerts:
        background_tasks.add_task(_fire_webhooks, new_alerts)
        background_tasks.add_task(_digest_issuer_patterns)

    return {
        "scanned": len(targets),
        "anomalies_found": len(alerts),
        "new_this_run": len(new_alerts),
        "results": alerts,
    }


@app.get("/alerts")
def list_alerts(
    public_only: bool = Query(False),
    state: Optional[str] = Query(None, description="Two-letter state code, e.g. NM"),
    since: Optional[str] = Query(None, description="ISO datetime lower bound, e.g. 2026-05-01"),
):
    results = _all_saved_alerts()
    if public_only:
        results = [a for a in results if a.get("is_public")]
    if state:
        results = [a for a in results if a.get("state", "").upper() == state.upper()]
    if since:
        results = [a for a in results if a.get("generated_at", "") >= since]
    return {"total": len(results), "alerts": results}


@app.post("/alerts/{alert_id}/feedback")
def submit_feedback(alert_id: str, body: FeedbackBody):
    _update_memory_feedback(alert_id, body.valid, body.notes)
    _digest_issuer_patterns()
    return {"alert_id": alert_id, "recorded": True, "valid": body.valid}


@app.get("/memory/summary")
def memory_summary():
    files = ["alerts_log.md", "issuer_patterns.md", "state_patterns.md"]
    return {
        fname: (MEMORY_DIR / fname).read_text() if (MEMORY_DIR / fname).exists() else ""
        for fname in files
    }
