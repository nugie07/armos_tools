"""
konversi_30hari.py

Generate daily JSON log files for the last 30 days (today inclusive) based on
table sys_api_request_log. Output files are saved to data_log/ with the format
DDMMYYYY_log.json, one per day.
"""

import os
from datetime import datetime, timedelta

from log_konversi import ensure_data_dir, fetch_logs
import json


def write_for_day(day: datetime) -> str:
    start = datetime(year=day.year, month=day.month, day=day.day)
    end = start + timedelta(days=1)
    file_part = start.strftime("%d%m%Y")
    data = fetch_logs(start, end)
    out_dir = ensure_data_dir()
    out_path = os.path.join(out_dir, f"{file_part}_log.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


def main():
    today = datetime.now()
    for i in range(0, 30):
        day = today - timedelta(days=i)
        out = write_for_day(day)
        print(f"Wrote: {out}")


if __name__ == "__main__":
    main()


