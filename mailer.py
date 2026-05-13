"""
Muni Bond Alert Mailer — SMTP delivery via smtplib + Jinja2.

Config via environment variables:
  SMTP_HOST         SMTP server hostname   (default: smtp.gmail.com)
  SMTP_PORT         SMTP port              (default: 587)
  SMTP_USER         From address / login
  SMTP_PASS         Password / app-password
  ALERT_RECIPIENTS  Comma-separated recipient addresses
"""

import os
import smtplib
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_jinja: Optional[Environment] = None


def _env() -> Environment:
    global _jinja
    if _jinja is None:
        _jinja = Environment(
            loader=FileSystemLoader(str(_TEMPLATE_DIR)),
            autoescape=select_autoescape(["html"]),
        )
    return _jinja


def _cfg() -> dict:
    return {
        "host":       os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "port":       int(os.getenv("SMTP_PORT", "587")),
        "user":       os.getenv("SMTP_USER", ""),
        "password":   os.getenv("SMTP_PASS", ""),
        "recipients": [r.strip() for r in os.getenv("ALERT_RECIPIENTS", "").split(",") if r.strip()],
    }


def _send(subject: str, html: str, recipients: list[str]) -> bool:
    c = _cfg()
    if not c["user"] or not c["password"] or not recipients:
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = c["user"]
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(c["host"], c["port"], timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(c["user"], c["password"])
            s.sendmail(c["user"], recipients, msg.as_string())
        return True
    except Exception as exc:
        print(f"[mailer] send failed: {exc}")
        return False


def send_alert(alert: dict) -> bool:
    """Render alert_email.html and deliver to ALERT_RECIPIENTS."""
    c = _cfg()
    if not c["recipients"]:
        return False

    issuer   = alert.get("issuer", "Unknown")
    state    = alert.get("state", "")
    spread   = alert.get("spread_bps", 0)
    ytw      = alert.get("ytw", 0)
    peer_avg = alert.get("peer_avg_ytw", 0)
    peers    = alert.get("peer_count", 0)
    rating   = alert.get("rating", "NR")
    par_val  = alert.get("par_amount", 0)
    purpose  = alert.get("purpose", "")
    red_flag = alert.get("red_flag") or ""

    call_date  = alert.get("call_date") or ""
    call_price = alert.get("call_price") or ""

    is_go       = any(k in purpose for k in ("General Obligation", "GO"))
    issuer_type = "General Obligation" if is_go else "Revenue Bond"

    summary_paragraph = (
        f"{issuer} ({state}) is yielding {ytw}% to worst, "
        f"a spread of +{spread}bps above the {peers}-bond peer average of {peer_avg}%. "
        f"This anomaly may reflect credit deterioration, thin secondary-market liquidity, "
        f"or a rating action not yet reflected in broader indices."
    )

    call_risk = (
        f"This bond is callable on {call_date} at ${call_price}. "
        f"Elevated spread relative to peers increases the probability of early call "
        f"if the issuer refinances at lower rates. "
        f"Yield-to-call may differ materially from YTW."
        if call_date else
        "This bond is non-callable; yield-to-maturity equals yield-to-worst."
    )

    target_investor = (
        f"Suitable for fixed-income investors seeking higher-yielding muni exposure in {state}. "
        f"Rating {rating} — appropriate for risk-tolerant accounts or those with "
        f"{state}-specific tax exemption objectives. "
        f"Not suitable for capital-preservation mandates."
    )

    ctx = {
        "issuer":             issuer,
        "state":              state,
        "purpose":            purpose,
        "maturity_date":      alert.get("maturity_date", ""),
        "is_public":          alert.get("is_public", True),
        "ytw":                ytw,
        "peer_avg_ytw":       peer_avg,
        "spread_bps":         spread,
        "rating":             rating,
        "par_millions":       f"{par_val / 1e6:.1f}",
        "par_formatted":      f"{par_val:,.0f}",
        "cusip":              alert.get("cusip", ""),
        "issuer_type":        issuer_type,
        "coupon":             alert.get("coupon", ""),
        "call_date_display":  call_date or "Non-callable",
        "call_price_display": f"${call_price}" if call_price else "—",
        "peer_count":         peers,
        "alert_id":           alert.get("alert_id", ""),
        "red_flag":           red_flag,
        "target_investor":    target_investor,
        "summary_paragraph":  summary_paragraph,
        "call_risk":          call_risk,
        "generated_at":       alert.get("generated_at", datetime.now().isoformat()[:16]),
    }

    html    = _env().get_template("alert_email.html").render(**ctx)
    subject = f"[Muni Alert] {issuer} ({state}) +{spread}bps | {rating}"
    return _send(subject, html, c["recipients"])


def send_digest(alerts: list[dict]) -> bool:
    """Send a weekly HTML digest table to ALERT_RECIPIENTS."""
    c = _cfg()
    if not c["recipients"] or not alerts:
        return False

    rows = ""
    for a in sorted(alerts, key=lambda x: x.get("spread_bps", 0), reverse=True):
        flag = (
            f"<br><small style='color:#c53030'>&#9888; {a['red_flag']}</small>"
            if a.get("red_flag") else ""
        )
        pub = "&#10003;" if a.get("is_public") else "&mdash;"
        rows += (
            f"<tr>"
            f"<td>{a.get('generated_at','')[:10]}</td>"
            f"<td><strong>{a.get('issuer','')}</strong></td>"
            f"<td>{a.get('state','')}</td>"
            f"<td style='color:#276749;font-weight:700'>+{a.get('spread_bps',0)}bps</td>"
            f"<td>{a.get('ytw','')}%</td>"
            f"<td>{a.get('rating','')}</td>"
            f"<td style='text-align:center'>{pub}</td>"
            f"<td style='font-size:11px;color:#718096'>{str(a.get('alert_id',''))[:8]}{flag}</td>"
            f"</tr>"
        )

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>
  body{{font-family:'Segoe UI',Arial,sans-serif;background:#f4f4f5;margin:0;padding:0}}
  .wrap{{max-width:720px;margin:32px auto;background:#fff;border-radius:8px;
         box-shadow:0 2px 12px rgba(0,0,0,.1);overflow:hidden}}
  .hdr{{background:#1a1a2e;padding:24px 32px;color:#fff}}
  .hdr h1{{margin:0;font-size:18px}}
  .hdr .sub{{margin-top:4px;font-size:12px;color:#a0aec0}}
  .body{{padding:24px 32px}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{background:#f7f8fc;color:#718096;font-size:10px;text-transform:uppercase;
      letter-spacing:1px;padding:8px 6px;text-align:left;border-bottom:2px solid #e2e8f0}}
  td{{padding:9px 6px;border-bottom:1px solid #f0f0f0;color:#2d3748;vertical-align:top}}
  tr:hover td{{background:#f7f8fc}}
  .ftr{{background:#f7f8fc;border-top:1px solid #e2e8f0;padding:16px 32px;
        font-size:11px;color:#a0aec0}}
</style></head><body>
<div class="wrap">
  <div class="hdr">
    <h1>Municipal Bond Anomaly Digest</h1>
    <div class="sub">Week of {date.today().isoformat()} &middot; {len(alerts)} alert(s)</div>
  </div>
  <div class="body">
    <table>
      <tr>
        <th>Date</th><th>Issuer</th><th>State</th><th>Spread</th>
        <th>YTW</th><th>Rating</th><th>Public</th><th>Alert ID</th>
      </tr>
      {rows}
    </table>
  </div>
  <div class="ftr">
    For informational purposes only. Not investment advice.
    Generated {datetime.now().isoformat()[:16]}.
  </div>
</div>
</body></html>"""

    subject = f"[Muni Digest] {len(alerts)} alert(s) — week of {date.today().isoformat()}"
    return _send(subject, html, c["recipients"])
