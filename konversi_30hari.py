"""
konversi_30hari.py

Generate daily SQLite log files for the last 30 days (today inclusive) based on
table sys_api_request_log. Output files are saved to data_log/ with the format
DDMMYYYY_log.db, one per day.
"""

from datetime import datetime, timedelta

from log_konversi import fetch_logs, write_logs_to_sqlite


def write_for_day(day: datetime) -> str:
    start = datetime(year=day.year, month=day.month, day=day.day)
    end = start + timedelta(days=1)
    file_part = start.strftime("%d%m%Y")
    data = fetch_logs(start, end)
    return write_logs_to_sqlite(data, file_part)


def main():
    today = datetime.now()
    for i in range(0, 30):
        day = today - timedelta(days=i)
        out = write_for_day(day)
        print(f"Wrote: {out}")


if __name__ == "__main__":
    main()


