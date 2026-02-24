"""
konversi_5hari.py

Generate daily SQLite log files for the last 5 days (today inclusive) based on
table sys_api_request_log. Output is saved under data_log/DDMMYYYY/{slug}.db
(satu file per tipe event per hari).
"""

from datetime import datetime, timedelta

from log_konversi import fetch_logs, write_logs_to_sqlite_per_event


def write_for_day(day: datetime) -> list:
    start = datetime(year=day.year, month=day.month, day=day.day)
    end = start + timedelta(days=1)
    file_part = start.strftime("%d%m%Y")
    data = fetch_logs(start, end)
    return write_logs_to_sqlite_per_event(data, file_part)


def main():
    today = datetime.now()
    for i in range(0, 5):
        day = today - timedelta(days=i)
        paths = write_for_day(day)
        print(f"Wrote {len(paths)} file(s) for {day.date()}: {paths}")


if __name__ == "__main__":
    main()
