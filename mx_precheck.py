#!/usr/bin/env python3
"""mx_precheck.py — Validate recipient domains before sending."""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from collections import Counter
from pathlib import Path

import dns.asyncresolver
import dns.exception
import dns.resolver

RESOLVERS = ["8.8.8.8", "1.1.1.1", "9.9.9.9"]
TIMEOUT = 5.0
CONCURRENCY = 50


async def classify(domain: str, sem: asyncio.Semaphore) -> tuple[str, str, str]:
    async with sem:
        resolver = dns.asyncresolver.Resolver()
        resolver.nameservers = RESOLVERS
        resolver.lifetime = TIMEOUT
        resolver.timeout = TIMEOUT

        try:
            answer = await resolver.resolve(domain, "MX")
            hosts = sorted(str(r.exchange).rstrip(".") for r in answer)
            return domain, "deliverable", f"MX: {', '.join(hosts)}"
        except dns.resolver.NoAnswer:
            pass
        except dns.resolver.NXDOMAIN:
            return domain, "dead", "NXDOMAIN"
        except dns.resolver.NoNameservers as e:
            return domain, "suspect", f"SERVFAIL/REFUSED ({type(e).__name__})"
        except dns.exception.Timeout:
            return domain, "suspect", "timeout"
        except Exception as e:
            return domain, "suspect", f"{type(e).__name__}: {e}"

        try:
            await resolver.resolve(domain, "A")
            return domain, "suspect", "no MX, has A (email likely fails)"
        except dns.resolver.NXDOMAIN:
            return domain, "dead", "NXDOMAIN on A"
        except dns.resolver.NoAnswer:
            return domain, "dead", "no MX, no A"
        except Exception as e:
            return domain, "suspect", f"A lookup: {type(e).__name__}"


async def run(emails: list[str]) -> dict[str, tuple[str, str]]:
    domains = sorted({e.split("@", 1)[1].lower().strip() for e in emails if "@" in e})
    sem = asyncio.Semaphore(CONCURRENCY)
    results = await asyncio.gather(*(classify(d, sem) for d in domains))
    return {d: (bucket, detail) for d, bucket, detail in results}


def main() -> int:
    ap = argparse.ArgumentParser(description="MX precheck for recipient lists")
    ap.add_argument("input", type=Path, help="CSV file with email column")
    ap.add_argument("--column", default="email", help="email column name (default: email)")
    ap.add_argument("--out", type=Path, default=Path("clean.csv"), help="output: deliverable rows")
    ap.add_argument("--report", type=Path, default=Path("mx_report.csv"), help="full bucketed report")
    args = ap.parse_args()

    if not args.input.exists():
        print(f"❌ {args.input} not found", file=sys.stderr)
        return 1

    with args.input.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if args.column not in fieldnames:
        print(f"❌ column '{args.column}' not in CSV. Found: {fieldnames}", file=sys.stderr)
        return 1

    emails = [r[args.column] for r in rows if r.get(args.column)]
    print(f"→ {len(emails)} emails, {len({e.split('@')[1] for e in emails if '@' in e})} unique domains")
    print(f"→ Resolving via {RESOLVERS} (concurrency={CONCURRENCY})...\n")

    domain_map = asyncio.run(run(emails))

    with args.report.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["domain", "bucket", "detail"])
        for d, (bucket, detail) in sorted(domain_map.items()):
            w.writerow([d, bucket, detail])

    with args.out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        kept = 0
        for row in rows:
            email = row.get(args.column, "")
            if "@" not in email:
                continue
            domain = email.split("@", 1)[1].lower().strip()
            if domain_map.get(domain, ("dead", ""))[0] == "deliverable":
                w.writerow(row)
                kept += 1

    counts = Counter(b for b, _ in domain_map.values())
    total = len(domain_map)
    print(f"✅ deliverable: {counts['deliverable']}/{total} domains")
    print(f"⚠️  suspect:     {counts['suspect']}/{total} domains  (retry later)")
    print(f"❌ dead:        {counts['dead']}/{total} domains  (drop)")
    print(f"\n→ {kept} rows written to {args.out}")
    print(f"→ Full report: {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
