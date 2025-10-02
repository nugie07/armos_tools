#!/usr/bin/env python3
import argparse
from datetime import datetime
from sync.manager import run_sync


def parse_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        raise SystemExit(f"Invalid date format: {s}. Use YYYY-MM-DD")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run TMS sync via CLI")
    parser.add_argument("--sync", required=True, choices=["fact_order", "fact_delivery", "both"], help="Sync type")
    parser.add_argument("--date-from", dest="date_from", help="YYYY-MM-DD", default=None)
    parser.add_argument("--date-to", dest="date_to", help="YYYY-MM-DD", default=None)
    args = parser.parse_args()

    df = parse_date(args.date_from)
    dt = parse_date(args.date_to)
    run_sync(args.sync, date_from=df, date_to=dt)
    print("Sync finished successfully")


if __name__ == "__main__":
    main()


