"""
Municipal Bond Anomaly Scanner
================================
Detects small muni bonds (< $20M par) paying 50+ bps above peer average
for similar credit risk, maturity, state, and issuer type.

Data sources (in priority order):
  1. CSV file  — manual entry; use for first 30 days
  2. EMMA stub — placeholder for future scraper
  3. Mock data — built-in samples for testing

Usage:
    python bond_scanner.py --csv data/sample_bonds.csv
    python bond_scanner.py --cusips 558000AA1 502000CC3
    python bond_scanner.py --csv data/sample_bonds.csv --output output/alerts.json
    python bond_scanner.py --mock
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_PAR = 20_000_000          # small muni threshold
ANOMALY_THRESHOLD_BPS = 50    # minimum spread to flag
MIN_PEER_COUNT = 2            # minimum peers for valid comparison
MATURITY_WINDOW_YEARS = 2     # ±2 years peer matching

MOODY_TO_NUM: dict[str, int] = {
    "Aaa": 1, "Aa1": 2, "Aa2": 3, "Aa3": 4,
    "A1":  5, "A2":  6, "A3":  7,
    "Baa1": 8, "Baa2": 9, "Baa3": 10,
    "Ba1": 11, "Ba2": 12, "Ba3": 13,
    "B1":  14, "B2":  15, "B3":  16,
    "NR":  99, "":    99,
}

SP_TO_NUM: dict[str, int] = {
    "AAA": 1, "AA+": 2, "AA": 3, "AA-": 4,
    "A+":  5, "A":   6, "A-": 7,
    "BBB+": 8, "BBB": 9, "BBB-": 10,
    "BB+": 11, "BB": 12, "BB-": 13,
    "B+":  14, "B":  15, "B-":  16,
    "NR":  99, "":   99,
}

RATING_TIER: dict[tuple, str] = {
    (1, 4): "Aa",
    (5, 7): "A",
    (8, 10): "Baa",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class BondRecord:
    cusip:         str
    issuer:        str
    state:         str
    purpose:       str
    issuer_type:   str          # "GO" | "Revenue" | "Special Tax" etc.
    par_amount:    float
    maturity_date: str          # YYYY-MM-DD
    coupon:        float
    ytw:           float        # yield to worst (%)
    ytm:           float        # yield to maturity (%)
    call_date:     Optional[str]  = None
    call_price:    Optional[float] = None
    rating_moodys: Optional[str]  = None
    rating_sp:     Optional[str]  = None
    rating_fitch:  Optional[str]  = None
    last_trade_date:  Optional[str]  = None
    last_trade_price: Optional[float] = None
    is_target:     bool = False
    source:        str  = "csv"

    @property
    def rating_numeric(self) -> int:
        m = MOODY_TO_NUM.get(self.rating_moodys or "", 99)
        s = SP_TO_NUM.get(self.rating_sp or "", 99)
        candidates = [v for v in [m, s] if v != 99]
        return min(candidates) if candidates else 99

    @property
    def rating_tier(self) -> str:
        n = self.rating_numeric
        for (lo, hi), label in RATING_TIER.items():
            if lo <= n <= hi:
                return label
        return "NR" if n == 99 else "Sub-IG"

    @property
    def rating_display(self) -> str:
        parts = [r for r in [self.rating_moodys, self.rating_sp] if r]
        return "/".join(parts) if parts else "NR"

    @property
    def maturity_year(self) -> int:
        return int(self.maturity_date[:4])

    @property
    def years_to_maturity(self) -> float:
        mat = date.fromisoformat(self.maturity_date)
        return (mat - date.today()).days / 365.25

    @property
    def months_to_call(self) -> Optional[int]:
        if not self.call_date:
            return None
        cd = date.fromisoformat(self.call_date)
        return max(0, (cd - date.today()).days // 30)


@dataclass
class AnomalyAlert:
    alert_id:       str
    cusip:          str
    generated_at:   str
    is_public:      bool
    issuer:         str
    state:          str
    purpose:        str
    par_amount:     float
    ytw:            float
    peer_avg_ytw:   float
    spread_bps:     int
    peer_count:     int
    maturity_date:  str
    rating:         str
    call_risk:      str
    target_investor: str
    red_flag:       Optional[str]
    plain_english:  str
    peers_used:     list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_csv(path: str) -> list[BondRecord]:
    bonds = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                bond = BondRecord(
                    cusip        = row["cusip"].strip(),
                    issuer       = row["issuer"].strip(),
                    state        = row["state"].strip().upper(),
                    purpose      = row["purpose"].strip(),
                    issuer_type  = row["issuer_type"].strip(),
                    par_amount   = float(row["par_amount"]),
                    maturity_date= row["maturity_date"].strip(),
                    coupon       = float(row["coupon"]),
                    ytw          = float(row["ytw"]),
                    ytm          = float(row["ytm"]),
                    call_date    = row["call_date"].strip() or None,
                    call_price   = float(row["call_price"]) if row["call_price"].strip() else None,
                    rating_moodys= row["rating_moodys"].strip() or None,
                    rating_sp    = row["rating_sp"].strip() or None,
                    rating_fitch = row["rating_fitch"].strip() or None,
                    last_trade_date  = row["last_trade_date"].strip() or None,
                    last_trade_price = float(row["last_trade_price"]) if row["last_trade_price"].strip() else None,
                    is_target    = row.get("is_target", "false").strip().lower() == "true",
                    source       = "csv",
                )
                bonds.append(bond)
            except (KeyError, ValueError) as e:
                print(f"  ⚠️  Skipping row (parse error: {e}): {row.get('cusip','?')}")
    return bonds


def fetch_emma(cusip: str) -> Optional[BondRecord]:
    """
    EMMA data fetcher — stub for future implementation.

    EMMA (emma.msrb.org) does not expose a public REST API as of 2026.
    Options for future implementation:
      1. Playwright/Selenium scraper against emma.msrb.org/SecurityView/
      2. MSRB bulk data subscription (contact datarequest@msrb.org)
      3. Commercial data vendors: Bloomberg, Refinitiv, ICE BondPoint

    For now, returns None — fall back to CSV or mock data.
    """
    print(f"  [EMMA] No API available — use CSV or mock for {cusip}")
    return None


def load_mock() -> list[BondRecord]:
    """Built-in mock bonds for testing without any data files."""
    return [
        BondRecord(
            cusip="558000AA1", issuer="Albuquerque Water Utility Authority",
            state="NM", purpose="Water & Sewer Revenue", issuer_type="Revenue",
            par_amount=8_500_000, maturity_date="2031-06-01",
            coupon=4.500, ytw=4.52, ytm=4.61,
            call_date="2028-06-01", call_price=100.0,
            rating_moodys="A2", rating_sp="A",
            last_trade_date="2026-04-15", last_trade_price=99.25,
            is_target=True, source="mock"
        ),
        BondRecord(
            cusip="145000BB2", issuer="Cherokee County School District",
            state="GA", purpose="General Obligation", issuer_type="GO",
            par_amount=12_300_000, maturity_date="2029-08-01",
            coupon=3.250, ytw=3.28, ytm=3.28,
            call_date=None, call_price=None,
            rating_moodys="Aa2", rating_sp="AA",
            last_trade_date="2026-03-22", last_trade_price=100.50,
            is_target=True, source="mock"
        ),
        BondRecord(
            cusip="502000CC3", issuer="Louisiana Children's Medical Center",
            state="LA", purpose="Hospital Revenue", issuer_type="Revenue",
            par_amount=15_700_000, maturity_date="2033-12-01",
            coupon=5.125, ytw=5.18, ytm=5.30,
            call_date="2026-12-01", call_price=100.0,
            rating_moodys="Baa1", rating_sp="BBB+",
            last_trade_date="2026-04-28", last_trade_price=98.75,
            is_target=True, source="mock"
        ),
        # Peer universe — NM Revenue A-rated ~2030-2033
        BondRecord(cusip="559000EE5", issuer="Bernalillo County NM", state="NM",
            purpose="Gross Receipts Tax Revenue", issuer_type="Revenue",
            par_amount=11_000_000, maturity_date="2031-02-01",
            coupon=3.850, ytw=3.91, ytm=3.95, call_date="2029-02-01", call_price=100.0,
            rating_moodys="A2", rating_sp="A", is_target=False, source="mock"),
        BondRecord(cusip="559000FF6", issuer="Santa Fe NM Wastewater", state="NM",
            purpose="Water & Sewer Revenue", issuer_type="Revenue",
            par_amount=7_800_000, maturity_date="2032-06-01",
            coupon=3.780, ytw=3.80, ytm=3.88, call_date="2030-06-01", call_price=100.0,
            rating_moodys="A3", rating_sp="A-", is_target=False, source="mock"),
        BondRecord(cusip="559000GG7", issuer="Las Cruces NM Electric", state="NM",
            purpose="Electric Revenue", issuer_type="Revenue",
            par_amount=14_500_000, maturity_date="2030-10-01",
            coupon=3.900, ytw=3.95, ytm=3.98, call_date="2028-10-01", call_price=100.0,
            rating_moodys="A2", rating_sp="A", is_target=False, source="mock"),
        BondRecord(cusip="559000OO5", issuer="Taos NM Gross Receipts", state="NM",
            purpose="Gross Receipts Tax Revenue", issuer_type="Revenue",
            par_amount=6_500_000, maturity_date="2031-08-01",
            coupon=3.820, ytw=3.88, ytm=3.91, call_date="2029-08-01", call_price=100.0,
            rating_moodys="A2", rating_sp="A-", is_target=False, source="mock"),
        # Peer universe — GA GO Aa-rated ~2028-2031
        BondRecord(cusip="146000HH8", issuer="Forsyth County GA School District", state="GA",
            purpose="General Obligation", issuer_type="GO",
            par_amount=16_800_000, maturity_date="2028-06-01",
            coupon=3.150, ytw=3.19, ytm=3.19,
            rating_moodys="Aa2", rating_sp="AA", is_target=False, source="mock"),
        BondRecord(cusip="146000II9", issuer="Cobb County GA GO", state="GA",
            purpose="General Obligation", issuer_type="GO",
            par_amount=8_900_000, maturity_date="2030-04-01",
            coupon=3.200, ytw=3.22, ytm=3.22,
            rating_moodys="Aa1", rating_sp="AA+", is_target=False, source="mock"),
        BondRecord(cusip="146000JJ0", issuer="Henry County GA Public Schools", state="GA",
            purpose="General Obligation", issuer_type="GO",
            par_amount=13_400_000, maturity_date="2029-02-01",
            coupon=3.100, ytw=3.15, ytm=3.15,
            rating_moodys="Aa3", rating_sp="AA-", is_target=False, source="mock"),
        # Peer universe — LA Revenue Baa-rated ~2032-2035
        BondRecord(cusip="503000KK1", issuer="Ochsner Clinic Foundation LA", state="LA",
            purpose="Hospital Revenue", issuer_type="Revenue",
            par_amount=18_500_000, maturity_date="2032-06-01",
            coupon=4.350, ytw=4.38, ytm=4.45, call_date="2027-06-01", call_price=100.0,
            rating_moodys="Baa1", rating_sp="BBB+", is_target=False, source="mock"),
        BondRecord(cusip="503000LL2", issuer="Our Lady of the Lake Regional", state="LA",
            purpose="Hospital Revenue", issuer_type="Revenue",
            par_amount=12_100_000, maturity_date="2034-10-01",
            coupon=4.150, ytw=4.20, ytm=4.28, call_date="2028-10-01", call_price=100.0,
            rating_moodys="Baa2", rating_sp="BBB", is_target=False, source="mock"),
        BondRecord(cusip="503000MM3", issuer="Willis Knighton Health System LA", state="LA",
            purpose="Hospital Revenue", issuer_type="Revenue",
            par_amount=9_800_000, maturity_date="2033-06-01",
            coupon=4.050, ytw=4.12, ytm=4.18, call_date="2028-06-01", call_price=100.0,
            rating_moodys="Baa1", rating_sp="BBB+", is_target=False, source="mock"),
        BondRecord(cusip="503000NN4", issuer="Lafayette General Medical Center", state="LA",
            purpose="Hospital Revenue", issuer_type="Revenue",
            par_amount=17_200_000, maturity_date="2032-04-01",
            coupon=4.250, ytw=4.30, ytm=4.36, call_date="2027-04-01", call_price=100.0,
            rating_moodys="Baa1", rating_sp="BBB", is_target=False, source="mock"),
    ]


# ---------------------------------------------------------------------------
# Peer matching
# ---------------------------------------------------------------------------

def find_peers(target: BondRecord, universe: list[BondRecord]) -> list[BondRecord]:
    """
    Match peers on: same state, same issuer_type broad category,
    same rating tier, maturity ±2 years, par < $20M, not target itself.
    Falls back to dropping state requirement if fewer than MIN_PEER_COUNT found.
    """
    target_type = _broad_type(target.issuer_type)
    target_tier = target.rating_tier

    def matches(b: BondRecord, require_state: bool) -> bool:
        if b.cusip == target.cusip:
            return False
        if b.is_target:
            return False
        if b.par_amount > MAX_PAR:
            return False
        if _broad_type(b.issuer_type) != target_type:
            return False
        if b.rating_tier != target_tier:
            return False
        if abs(b.maturity_year - target.maturity_year) > MATURITY_WINDOW_YEARS:
            return False
        if require_state and b.state != target.state:
            return False
        return True

    peers = [b for b in universe if matches(b, require_state=True)]
    if len(peers) < MIN_PEER_COUNT:
        peers = [b for b in universe if matches(b, require_state=False)]
    return peers


def _broad_type(issuer_type: str) -> str:
    t = issuer_type.upper()
    if "GO" in t or "GENERAL" in t:
        return "GO"
    return "Revenue"


# ---------------------------------------------------------------------------
# Alert generation
# ---------------------------------------------------------------------------

def detect_anomaly(target: BondRecord, universe: list[BondRecord]) -> Optional[AnomalyAlert]:
    """
    Compare target to peers. Returns AnomalyAlert if spread ≥ threshold, else None.
    """
    if target.par_amount > MAX_PAR:
        return None

    peers = find_peers(target, universe)
    if len(peers) < MIN_PEER_COUNT:
        print(f"  ⚠️  {target.cusip}: only {len(peers)} peer(s) found — skipping")
        return None

    peer_avg_ytw = sum(p.ytw for p in peers) / len(peers)
    spread_bps   = round((target.ytw - peer_avg_ytw) * 100)

    if spread_bps < ANOMALY_THRESHOLD_BPS:
        return None

    alert_id = f"MUNI-{date.today().strftime('%Y%m%d')}-{target.cusip[-4:]}"

    call_risk     = _call_risk_text(target)
    tgt_investor  = _target_investor_text(target, spread_bps)
    red_flag      = _red_flag_text(target, spread_bps, len(peers))
    plain_english = _plain_english(target, peer_avg_ytw, spread_bps, call_risk,
                                   tgt_investor, red_flag)

    return AnomalyAlert(
        alert_id      = alert_id,
        cusip         = target.cusip,
        generated_at  = datetime.now().isoformat(timespec="seconds"),
        is_public     = spread_bps >= 75,   # notable spreads go public
        issuer        = target.issuer,
        state         = target.state,
        purpose       = target.purpose,
        par_amount    = target.par_amount,
        ytw           = target.ytw,
        peer_avg_ytw  = round(peer_avg_ytw, 4),
        spread_bps    = spread_bps,
        peer_count    = len(peers),
        maturity_date = target.maturity_date,
        rating        = target.rating_display,
        call_risk     = call_risk,
        target_investor = tgt_investor,
        red_flag      = red_flag,
        plain_english = plain_english,
        peers_used    = [p.cusip for p in peers],
    )


def _call_risk_text(b: BondRecord) -> str:
    mtc = b.months_to_call
    if mtc is None:
        return "Non-callable — yield is locked to maturity with no early redemption risk."
    if mtc < 24:
        return (f"HIGH call risk — issuer can redeem in ~{mtc} months at "
                f"${b.call_price:.2f}. The YTW already reflects this worst-case scenario.")
    if mtc < 60:
        yrs = mtc // 12
        return (f"Moderate call risk — callable in ~{yrs} year{'s' if yrs!=1 else ''} "
                f"at ${b.call_price:.2f}. Monitor refinancing conditions as call date approaches.")
    yrs = mtc // 12
    return (f"Low call risk — callable in ~{yrs} years at ${b.call_price:.2f}. "
            f"Yield advantage is well-protected near-term.")


def _target_investor_text(b: BondRecord, spread_bps: int) -> str:
    yrs = round(b.years_to_maturity, 0)
    horizon = f"{int(yrs)}+ year horizon"
    if spread_bps >= 75:
        return (f"Income-focused investors with a {horizon} who are comfortable "
                f"with small-issuer liquidity constraints and seeking above-market "
                f"tax-exempt yield.")
    return (f"Conservative tax-exempt income investors with a {horizon} "
            f"looking for modest spread pickup over benchmark peers.")


def _red_flag_text(b: BondRecord, spread_bps: int, peer_count: int) -> Optional[str]:
    mtc = b.months_to_call
    purpose_lower = b.purpose.lower()
    if peer_count < 3:
        return f"Thin peer group ({peer_count} comps) — spread may not fully reflect market."
    if mtc is not None and mtc < 18:
        return f"Near-term call risk ({b.call_date}) may erode the yield advantage if issuer refinances."
    if spread_bps > 150:
        return f"Unusually wide spread ({spread_bps}bps) — warrants independent credit review."
    if "hospital" in purpose_lower or "medical" in purpose_lower or "health" in purpose_lower:
        return "Healthcare revenue bonds face structural headwinds (reimbursement pressure, labor costs)."
    if b.issuer_type == "Revenue" and b.rating_numeric >= 8:
        return f"Baa-rated revenue bonds carry real default optionality — review pledged revenue coverage."
    return None


def _plain_english(
    b: BondRecord,
    peer_avg: float,
    spread_bps: int,
    call_risk: str,
    target_investor: str,
    red_flag: Optional[str],
) -> str:
    par_m = b.par_amount / 1_000_000
    severity = ("notably" if spread_bps >= 75 else "modestly")
    lines = [
        f"This ${par_m:.1f}M {b.issuer} bond is {severity} higher-yielding than its peers.",
        "",
        f"At {b.ytw:.2f}% yield-to-worst, it sits {spread_bps} basis points above the "
        f"{peer_avg:.2f}% average for similar {b.rating_display}-rated "
        f"{b.issuer_type.lower()} bonds in the {b.maturity_date[:4]} maturity range.",
        "",
        f"CALL RISK: {call_risk}",
        "",
        f"WHO IT'S FOR: {target_investor}",
    ]
    if red_flag:
        lines += ["", f"RED FLAG: {red_flag}"]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _anonymize_id(alert: AnomalyAlert) -> str:
    raw = f"{alert.cusip}{alert.generated_at}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def print_alert(alert: AnomalyAlert):
    SEP = "=" * 62
    print(f"\n{SEP}")
    print(f"  ALERT  {alert.alert_id}  "
          f"{'[PUBLIC]' if alert.is_public else '[SUBSCRIBER]'}")
    print(f"  {alert.issuer}  |  {alert.state}  |  CUSIP {alert.cusip}")
    print(SEP)
    print(f"  YTW          : {alert.ytw:.2f}%")
    print(f"  Peer avg YTW : {alert.peer_avg_ytw:.2f}%")
    print(f"  Spread       : +{alert.spread_bps} bps  ({len(alert.peers_used)} peers)")
    print(f"  Rating       : {alert.rating}")
    print(f"  Maturity     : {alert.maturity_date}")
    print(f"  Par          : ${alert.par_amount/1e6:.1f}M")
    print(f"\n{alert.plain_english}")
    print(f"\n{SEP}\n")


def save_alerts(alerts: list[AnomalyAlert], path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    records = []
    for a in alerts:
        d = asdict(a)
        d["anon_id"] = _anonymize_id(a)
        records.append(d)
    with open(path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"\nSaved {len(records)} alert(s) → {path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Municipal Bond Anomaly Scanner")
    src = p.add_mutually_exclusive_group()
    src.add_argument("--csv",    help="Path to bonds CSV file")
    src.add_argument("--mock",   action="store_true", help="Use built-in mock data")
    src.add_argument("--cusips", nargs="+", help="Fetch specific CUSIPs from EMMA (stub)")
    p.add_argument("--output",   default=None, help="Save alerts to JSON file")
    p.add_argument("--threshold", type=int, default=ANOMALY_THRESHOLD_BPS,
                   help=f"Anomaly threshold in bps (default: {ANOMALY_THRESHOLD_BPS})")
    p.add_argument("--public-only", action="store_true",
                   help="Only show alerts flagged as public")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Load universe
    if args.mock:
        universe = load_mock()
        print(f"Loaded {len(universe)} mock bonds")
    elif args.csv:
        universe = load_csv(args.csv)
        print(f"Loaded {len(universe)} bonds from {args.csv}")
    elif args.cusips:
        universe = [b for c in args.cusips if (b := fetch_emma(c)) is not None]
        print(f"Fetched {len(universe)} bonds from EMMA")
    else:
        print("No data source specified. Use --csv, --mock, or --cusips.")
        print("Run with --mock to test with built-in sample data.")
        sys.exit(1)

    # Filter to small munis
    all_bonds  = [b for b in universe if b.par_amount <= MAX_PAR]
    targets    = [b for b in all_bonds if b.is_target]
    non_targets = [b for b in all_bonds if not b.is_target]

    if not targets:
        # If no targets flagged, scan all bonds
        targets = all_bonds

    print(f"Scanning {len(targets)} target bond(s) against "
          f"{len(non_targets)} peer bond(s)...\n")

    # Detect anomalies
    threshold = args.threshold
    alerts = []
    for bond in targets:
        alert = detect_anomaly(bond, non_targets)
        if alert:
            if args.public_only and not alert.is_public:
                continue
            alerts.append(alert)
            print_alert(alert)
        else:
            print(f"  ✓  {bond.cusip} ({bond.issuer[:40]}) — within normal range")

    print(f"\n{'─'*62}")
    print(f"  {len(alerts)} anomal{'y' if len(alerts)==1 else 'ies'} detected "
          f"out of {len(targets)} target bond(s) scanned.")

    if args.output:
        save_alerts(alerts, args.output)
    elif alerts:
        default_path = f"output/alerts_{date.today().strftime('%Y%m%d')}.json"
        save_alerts(alerts, default_path)
