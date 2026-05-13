"""
Treasury Yield Curve — free data from Treasury.gov XML feed.

Fetches the daily par yield curve (nominal) and derives a synthetic AAA
municipal benchmark using the historical muni/Treasury ratio by maturity.

Muni/Treasury ratios (approximate, post-2020 normal range):
  2yr:  85%   5yr:  88%   10yr: 90%   20yr: 93%   30yr: 95%

These ratios reflect the tax-exempt nature of munis vs taxable Treasuries.
In risk-off environments ratios compress toward 100%+; in calm markets they
widen below 85%. The defaults are conservative mid-cycle estimates.

Usage:
    from src.treasury_curve import get_muni_benchmark_yield
    yield_5yr = get_muni_benchmark_yield(maturity_years=5)   # → 3.21 (%)
"""

import json
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

CACHE_PATH = Path("data/treasury_curve_cache.json")
CACHE_TTL_HOURS = 4

# Muni/Treasury ratio by maturity bucket (tax-exempt premium)
MUNI_TREASURY_RATIO: dict[int, float] = {
    1:  0.83,
    2:  0.85,
    3:  0.87,
    5:  0.88,
    7:  0.89,
    10: 0.90,
    20: 0.93,
    30: 0.95,
}

_NS = "{http://schemas.microsoft.com/ado/2007/08/dataservices}"


def _fetch_treasury_curve() -> dict[float, float]:
    """
    Fetch current Treasury par yield curve from Treasury.gov.
    Returns {maturity_years: yield_pct}.
    """
    ym = date.today().strftime("%Y%m")
    url = (
        f"https://home.treasury.gov/resource-center/data-chart-center/"
        f"interest-rates/pages/xml?data=daily_treasury_yield_curve"
        f"&field_tdr_date_value_month={ym}"
    )
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            xml_data = resp.read().decode()
    except Exception as exc:
        raise RuntimeError(f"Treasury.gov fetch failed: {exc}")

    root = ET.fromstring(xml_data)
    entries = list(root.iter("{http://www.w3.org/2005/Atom}entry"))
    if not entries:
        raise RuntimeError("No entries in Treasury XML")

    last = entries[-1]
    maturity_map = {
        "BC_1MONTH": 0.083, "BC_2MONTH": 0.167, "BC_3MONTH": 0.25,
        "BC_6MONTH": 0.5,   "BC_1YEAR": 1.0,   "BC_2YEAR": 2.0,
        "BC_3YEAR": 3.0,    "BC_5YEAR": 5.0,   "BC_7YEAR": 7.0,
        "BC_10YEAR": 10.0,  "BC_20YEAR": 20.0, "BC_30YEAR": 30.0,
    }
    curve: dict[float, float] = {}
    for child in last.iter():
        tag = child.tag.split("}")[-1]
        if tag in maturity_map and child.text:
            try:
                curve[maturity_map[tag]] = float(child.text)
            except ValueError:
                pass

    if not curve:
        raise RuntimeError("Could not parse any yield points from Treasury XML")
    return curve


def _load_cache() -> Optional[dict]:
    if not CACHE_PATH.exists():
        return None
    try:
        d = json.loads(CACHE_PATH.read_text())
        fetched = datetime.fromisoformat(d["fetched_at"])
        if (datetime.now() - fetched).total_seconds() < CACHE_TTL_HOURS * 3600:
            return d
    except Exception:
        pass
    return None


def _save_cache(curve: dict[int, float]) -> None:
    CACHE_PATH.parent.mkdir(exist_ok=True)
    try:
        CACHE_PATH.write_text(json.dumps({
            "fetched_at": datetime.now().isoformat(),
            "curve": {str(k): v for k, v in curve.items()},
        }))
    except Exception:
        pass


def get_treasury_curve() -> dict[float, float]:
    """Returns {maturity_years: treasury_yield_%}. Cached 4h."""
    cached = _load_cache()
    if cached:
        return {float(k): v for k, v in cached["curve"].items()}
    try:
        curve = _fetch_treasury_curve()
        _save_cache(curve)
        return {float(k): v for k, v in curve.items()}
    except Exception as exc:
        print(f"  [treasury_curve] fetch failed: {exc} — using fallback")
        # Fallback: approximate current curve (update manually if stale)
        return {0.25: 4.30, 0.5: 4.25, 1: 4.15, 2: 3.90, 3: 3.80,
                5: 3.85, 7: 3.95, 10: 4.05, 20: 4.35, 30: 4.45}


def get_muni_benchmark_yield(maturity_years: float, rating_tier: str = "A") -> float:
    """
    Returns estimated AAA/AA muni benchmark yield for a given maturity.
    Uses Treasury par curve × muni/Treasury ratio.

    rating_tier: "Aaa"/"AA" use base ratio; "A" adds 20bps; "Baa"/"BBB" adds 50bps
    """
    curve = get_treasury_curve()
    treasury_yield = _interpolate(curve, maturity_years)

    # Find closest ratio bucket
    buckets = sorted(MUNI_TREASURY_RATIO.keys())
    closest = min(buckets, key=lambda x: abs(x - maturity_years))
    ratio = MUNI_TREASURY_RATIO[closest]

    base_muni = treasury_yield * ratio

    # Credit spread by rating tier
    spread_add = {
        "Aaa": 0.0, "Aa1": 0.05, "Aa2": 0.08, "Aa3": 0.12,
        "A1": 0.18, "A2": 0.22, "A3": 0.28,
        "Baa1": 0.40, "Baa2": 0.55, "Baa3": 0.75,
        "AA+": 0.05, "AA": 0.08, "AA-": 0.12,
        "A+": 0.18, "A": 0.22, "A-": 0.28,
        "BBB+": 0.40, "BBB": 0.55, "BBB-": 0.75,
    }.get(rating_tier, 0.22)

    return round(base_muni + spread_add, 4)


def _interpolate(curve: dict, target: float) -> float:
    """Linear interpolation between two nearest maturity points."""
    maturities = sorted(curve.keys())
    if target <= maturities[0]:
        return curve[maturities[0]]
    if target >= maturities[-1]:
        return curve[maturities[-1]]
    for i in range(len(maturities) - 1):
        lo, hi = maturities[i], maturities[i + 1]
        if lo <= target <= hi:
            t = (target - lo) / (hi - lo)
            return curve[lo] + t * (curve[hi] - curve[lo])
    return curve[maturities[-1]]


def print_curve() -> None:
    curve = get_treasury_curve()
    print("Treasury par curve (today):")
    for mat in sorted(curve):
        muni = get_muni_benchmark_yield(mat)
        print(f"  {mat:>5.1f}yr  Treasury={curve[mat]:.3f}%  AAA-Muni≈{muni:.3f}%")
