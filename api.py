"""
Municipal Bond Scanner — FastAPI service

POST /scan                    run scanner, return alerts, fire webhooks
GET  /alerts                  historical alerts (filter: public_only, state, since)
POST /alerts/{id}/feedback    human validates/rejects → updates memory
GET  /memory/summary          accumulated knowledge base (alerts_log, issuer_patterns, state_patterns)
GET  /digest                  last N days of alerts as JSON
POST /digest/send             email weekly digest to ALERT_RECIPIENTS
GET  /analytics               spread distribution, top issuers, state heatmap, rating tiers
GET  /health                  status, alert counts, memory state, live regime panel
"""

import json
import os
import tempfile
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime, timedelta
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
from mailer import send_alert, send_digest

app = FastAPI(title="Muni Bond Anomaly Scanner", version="1.0.0")

ALERTS_DIR = Path("output")
MEMORY_DIR = Path("memory")
ALERTS_DIR.mkdir(exist_ok=True)
MEMORY_DIR.mkdir(exist_ok=True)

WEBHOOK_URLS = [u.strip() for u in os.getenv("WEBHOOK_URLS", "").split(",") if u.strip()]

REGIME_THRESHOLDS = {"BULL": 60, "NEUTRAL": 50, "BEAR": 40, "CRISIS": 35}
_OUROBOROS_SNAPSHOT = Path("../ouroboros.v2/logs/regime_snapshot.json")


def _read_regime_snapshot() -> dict:
    """
    Returns the raw snapshot dict plus computed fields, or a stale/missing sentinel.
    Keys: label, score, vix, fetched_at, age_minutes, fresh
    """
    if not _OUROBOROS_SNAPSHOT.exists():
        return {"label": "NEUTRAL", "score": 0.5, "vix": None,
                "fetched_at": None, "age_minutes": None, "fresh": False}
    try:
        data    = json.loads(_OUROBOROS_SNAPSHOT.read_text())
        fetched = datetime.fromisoformat(data["fetched_at"])
        age_s   = (datetime.now() - fetched).total_seconds()
        fresh   = age_s < 3600
        return {
            "label":       data.get("label", "NEUTRAL") if fresh else "NEUTRAL",
            "score":       data.get("score"),
            "vix":         data.get("vix"),
            "fetched_at":  data.get("fetched_at"),
            "age_minutes": round(age_s / 60, 1),
            "fresh":       fresh,
        }
    except Exception:
        return {"label": "NEUTRAL", "score": None, "vix": None,
                "fetched_at": None, "age_minutes": None, "fresh": False}


def _get_ouroboros_regime() -> str:
    return _read_regime_snapshot()["label"]


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


def _digest_state_patterns() -> None:
    """Rewrite state_patterns.md from all saved alerts."""
    all_alerts = _all_saved_alerts()
    if not all_alerts:
        return

    by_state: dict[str, list] = defaultdict(list)
    for a in all_alerts:
        by_state[a.get("state", "??")].append(a)

    lines = [
        "# State-Level Spread Patterns",
        "",
        f"Auto-generated {date.today().isoformat()} from {len(all_alerts)} saved alerts.",
        "",
        "---",
        "",
    ]

    for state, alerts in sorted(by_state.items()):
        spreads  = [a["spread_bps"] for a in alerts]
        n        = len(alerts)
        public_n = sum(1 for a in alerts if a.get("is_public"))
        flag_n   = sum(1 for a in alerts if a.get("red_flag"))
        last     = max(a["generated_at"][:10] for a in alerts)

        # Issuer type breakdown
        type_counts: dict[str, int] = defaultdict(int)
        for a in alerts:
            itype = a.get("issuer_type", "")
            bucket = "GO" if "General Obligation" in itype or itype == "GO" else (itype or "Revenue")
            type_counts[bucket] += 1
        type_str = " | ".join(f"{t}: {c}" for t, c in sorted(type_counts.items(), key=lambda x: -x[1]))

        # Rating tier distribution (Aaa–A / Baa / Ba–below / NR)
        rating_buckets: dict[str, int] = defaultdict(int)
        for a in alerts:
            r = a.get("rating", "NR") or "NR"
            if any(r.startswith(p) for p in ("Aaa", "Aa", "A", "AAA", "AA")):
                rating_buckets["IG-High"] += 1
            elif any(r.startswith(p) for p in ("Baa", "BBB")):
                rating_buckets["IG-Low"] += 1
            elif any(r.startswith(p) for p in ("Ba", "B", "BB")):
                rating_buckets["HY"] += 1
            else:
                rating_buckets["NR"] += 1
        rating_str = " | ".join(f"{k}: {v}" for k, v in sorted(rating_buckets.items()))

        # Top issuers (up to 3)
        issuer_counts: dict[str, int] = defaultdict(int)
        for a in alerts:
            issuer_counts[a.get("issuer", "?")] += 1
        top = sorted(issuer_counts.items(), key=lambda x: -x[1])[:3]
        top_str = ", ".join(f"{iss} ({cnt}×)" for iss, cnt in top)

        lines += [
            f"### {state}",
            f"- **Alert count**: {n} | **Spread range**: {min(spreads)}bps – {max(spreads)}bps"
            f" (avg {sum(spreads) // n}bps)",
            f"- **Issuer types**: {type_str}",
            f"- **Ratings**: {rating_str}",
            f"- **Public**: {public_n}/{n} alerts ({public_n * 100 // n}%)",
            f"- **Red flags**: {flag_n}/{n} alerts flagged",
            f"- **Top issuers**: {top_str}",
            f"- **Last seen**: {last}",
            "",
        ]

    (MEMORY_DIR / "state_patterns.md").write_text("\n".join(lines))


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
    saved    = _all_saved_alerts()
    log_path = MEMORY_DIR / "alerts_log.md"
    snap     = _read_regime_snapshot()
    regime   = snap["label"]
    threshold = REGIME_THRESHOLDS.get(regime, ANOMALY_THRESHOLD_BPS)

    last_alert_date = None
    if saved:
        last_alert_date = max(a.get("generated_at", "")[:10] for a in saved) or None

    return {
        "status": "ok",
        "alerts": {
            "total_saved":        len(saved),
            "public":             sum(1 for a in saved if a.get("is_public")),
            "last_alert_date":    last_alert_date,
        },
        "memory": {
            "log_lines":          len(log_path.read_text().splitlines()) if log_path.exists() else 0,
            "issuer_patterns":    (MEMORY_DIR / "issuer_patterns.md").exists(),
            "state_patterns":     (MEMORY_DIR / "state_patterns.md").exists(),
        },
        "regime": {
            "label":              regime,
            "vix":                snap["vix"],
            "score":              snap["score"],
            "fetched_at":         snap["fetched_at"],
            "age_minutes":        snap["age_minutes"],
            "fresh":              snap["fresh"],
            "effective_threshold_bps": threshold,
        },
        "webhooks_configured": len(WEBHOOK_URLS),
    }


@app.post("/scan")
async def scan(
    background_tasks: BackgroundTasks,
    file: Optional[UploadFile] = File(None),
    mock: bool = Query(False, description="Use built-in mock bonds"),
    threshold: Optional[int] = Query(None, description="Minimum spread in bps (default: regime-adaptive)"),
    public_only: bool = Query(False, description="Return only public-tier alerts"),
):
    regime = _get_ouroboros_regime()
    effective_threshold = threshold if threshold is not None else REGIME_THRESHOLDS.get(regime, ANOMALY_THRESHOLD_BPS)

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
        alert = detect_anomaly(bond, non_targets, threshold=effective_threshold)
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
        background_tasks.add_task(_digest_state_patterns)
        for a in new_alerts:
            background_tasks.add_task(send_alert, a)

    return {
        "scanned": len(targets),
        "anomalies_found": len(alerts),
        "new_this_run": len(new_alerts),
        "regime": regime,
        "effective_threshold_bps": effective_threshold,
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
    _digest_state_patterns()
    return {"alert_id": alert_id, "recorded": True, "valid": body.valid}


@app.get("/memory/summary")
def memory_summary():
    files = ["alerts_log.md", "issuer_patterns.md", "state_patterns.md"]
    return {
        fname: (MEMORY_DIR / fname).read_text() if (MEMORY_DIR / fname).exists() else ""
        for fname in files
    }


@app.get("/digest")
def digest(days: int = Query(7, description="Lookback window in days")):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    alerts = [a for a in _all_saved_alerts() if a.get("generated_at", "") >= cutoff]
    alerts.sort(key=lambda x: x.get("spread_bps", 0), reverse=True)
    return {
        "period_days":  days,
        "total":        len(alerts),
        "public":       sum(1 for a in alerts if a.get("is_public")),
        "states":       sorted({a.get("state", "") for a in alerts}),
        "alerts":       alerts,
    }


@app.post("/digest/send")
def send_digest_email(days: int = Query(7, description="Lookback window in days")):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    alerts = [a for a in _all_saved_alerts() if a.get("generated_at", "") >= cutoff]
    if not alerts:
        return {"sent": False, "reason": "no alerts in window"}
    ok = send_digest(alerts)
    return {"sent": ok, "count": len(alerts)}


@app.get("/analytics")
def analytics():
    alerts = _all_saved_alerts()
    if not alerts:
        return {"total": 0}

    # Spread distribution buckets
    buckets: dict[str, int] = {"50-74": 0, "75-99": 0, "100-149": 0, "150+": 0}
    for a in alerts:
        s = a.get("spread_bps", 0)
        if s >= 150:
            buckets["150+"] += 1
        elif s >= 100:
            buckets["100-149"] += 1
        elif s >= 75:
            buckets["75-99"] += 1
        else:
            buckets["50-74"] += 1

    # Top 5 issuers by flag count
    issuer_counts: dict[str, int] = defaultdict(int)
    issuer_spreads: dict[str, list] = defaultdict(list)
    for a in alerts:
        iss = a.get("issuer", "Unknown")
        issuer_counts[iss] += 1
        issuer_spreads[iss].append(a.get("spread_bps", 0))
    top_issuers = [
        {
            "issuer":    iss,
            "state":     next((a.get("state") for a in alerts if a.get("issuer") == iss), ""),
            "count":     cnt,
            "avg_spread_bps": sum(issuer_spreads[iss]) // cnt,
            "max_spread_bps": max(issuer_spreads[iss]),
        }
        for iss, cnt in sorted(issuer_counts.items(), key=lambda x: -x[1])[:5]
    ]

    # State heatmap — alert count + avg spread per state
    state_data: dict[str, list] = defaultdict(list)
    for a in alerts:
        state_data[a.get("state", "??")].append(a.get("spread_bps", 0))
    state_heatmap = [
        {
            "state":        st,
            "count":        len(spreads),
            "avg_spread_bps": sum(spreads) // len(spreads),
            "max_spread_bps": max(spreads),
        }
        for st, spreads in sorted(state_data.items(), key=lambda x: -len(x[1]))
    ]

    # Rating tier breakdown
    rating_tiers: dict[str, int] = defaultdict(int)
    for a in alerts:
        r = a.get("rating", "NR") or "NR"
        if any(r.startswith(p) for p in ("Aaa", "Aa", "AAA", "AA")):
            tier = "IG-High (Aa/AA and above)"
        elif any(r.startswith(p) for p in ("A",)):
            tier = "IG-Mid (A)"
        elif any(r.startswith(p) for p in ("Baa", "BBB")):
            tier = "IG-Low (Baa/BBB)"
        elif any(r.startswith(p) for p in ("Ba", "BB", "B")):
            tier = "High Yield"
        else:
            tier = "NR / Unrated"
        rating_tiers[tier] += 1

    # Summary stats
    spreads_all = [a.get("spread_bps", 0) for a in alerts]
    public_n    = sum(1 for a in alerts if a.get("is_public"))
    red_flag_n  = sum(1 for a in alerts if a.get("red_flag"))

    return {
        "total_alerts":     len(alerts),
        "public_alerts":    public_n,
        "red_flag_alerts":  red_flag_n,
        "spread_stats": {
            "min_bps":  min(spreads_all),
            "max_bps":  max(spreads_all),
            "avg_bps":  sum(spreads_all) // len(spreads_all),
            "median_bps": sorted(spreads_all)[len(spreads_all) // 2],
        },
        "spread_distribution": buckets,
        "top_issuers":         top_issuers,
        "state_heatmap":       state_heatmap,
        "rating_tiers":        dict(sorted(rating_tiers.items())),
    }
