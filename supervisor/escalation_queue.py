"""
Escalation queue for muni_scanner supervisor.
Identical contract to ouroboros.v2/supervisor/escalation_queue.py.
Writes to muni_scanner/logs/escalations.json.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

QUEUE_PATH = "logs/escalations.json"


def _load() -> list:
    if not os.path.exists(QUEUE_PATH):
        return []
    try:
        with open(QUEUE_PATH) as f:
            return json.load(f)
    except Exception:
        return []


def _save(records: list) -> None:
    os.makedirs(os.path.dirname(QUEUE_PATH), exist_ok=True)
    with open(QUEUE_PATH, "w") as f:
        json.dump(records, f, indent=2)


def push(
    anomaly_type:   str,
    severity:       str,
    detail:         str,
    hypothesis:     str,
    files_to_check: list,
    project:        str = "muni_scanner",
) -> dict:
    records = _load()
    esc_id  = f"muni-esc-{datetime.now().strftime('%Y%m%d')}-{len(records)+1:03d}"
    record  = {
        "id":             esc_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "project":        project,
        "type":           anomaly_type,
        "severity":       severity,
        "detail":         detail,
        "hypothesis":     hypothesis,
        "files_to_check": files_to_check,
        "status":         "OPEN",
        "resolution":     None,
    }
    records.append(record)
    _save(records)
    return record


def get_open(severity: Optional[str] = None) -> list:
    records = _load()
    open_esc = [r for r in records if r.get("status") == "OPEN"]
    if severity:
        open_esc = [r for r in open_esc if r.get("severity") == severity]
    return open_esc


def resolve(esc_id: str, resolution: str) -> bool:
    records = _load()
    for r in records:
        if r["id"] == esc_id:
            r["status"]      = "RESOLVED"
            r["resolution"]  = resolution
            r["resolved_at"] = datetime.now(timezone.utc).isoformat()
            _save(records)
            return True
    return False


def already_open(anomaly_type: str, within_hours: int = 24) -> bool:
    records = _load()
    cutoff  = datetime.now(timezone.utc) - timedelta(hours=within_hours)
    for r in records:
        if r.get("type") != anomaly_type:
            continue
        if r.get("status") not in ("OPEN", "IN_PROGRESS"):
            continue
        try:
            ts = datetime.fromisoformat(r["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts > cutoff:
                return True
        except Exception:
            pass
    return False
