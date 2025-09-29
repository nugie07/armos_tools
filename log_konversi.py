"""
log_konversi.py

Export rows from table sys_api_request_log for TODAY into JSON file under data_log/.
- File name format: DDMMYYYY_log.json (e.g., 29092025_log.json)
- Overwrites if exists
- Intended to be scheduled every 30 minutes (cron or scheduler)
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List


def try_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)
    except Exception:
        pass


try_load_dotenv()


def _env(primary: str, fallback: str | None = None, default: str | None = None) -> str | None:
    v = os.getenv(primary)
    if v is None and fallback is not None:
        v = os.getenv(fallback)
    if v is None:
        v = default
    return v

# Use the same schema as app.py (DATABASE_MAIN_*) with fallback to older names
DB_HOST = _env("DATABASE_MAIN_HOST", "DB_HOST")
DB_PORT = int(_env("DATABASE_MAIN_PORT", "DB_PORT", "5432") or "5432")
DB_NAME = _env("DATABASE_MAIN_NAME", "DB_NAME")
DB_USER = _env("DATABASE_MAIN_USERNAME", "DB_USER")
DB_PASSWORD = _env("DATABASE_MAIN_PASS", "DB_PASSWORD")


def get_db_connection():
    import psycopg2
    # Validate required envs early for clearer errors
    missing = [
        name for name, val in [
            ("DB_HOST", DB_HOST),
            ("DB_NAME", DB_NAME),
            ("DB_USER", DB_USER),
            ("DB_PASSWORD", DB_PASSWORD),
        ] if not val
    ]
    if missing:
        raise RuntimeError(f"Missing database env vars: {', '.join(missing)}. Check your .env")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def ensure_data_dir() -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data_log")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def daterange_today() -> tuple[datetime, datetime, str]:
    now = datetime.now()
    start = datetime(year=now.year, month=now.month, day=now.day)
    end = start + timedelta(days=1)
    file_part = start.strftime("%d%m%Y")
    return start, end, file_part


def fetch_logs(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    sql = (
        "SELECT api_request_log_id, event, request, response, created_date "
        "FROM sys_api_request_log "
        "WHERE created_date >= %s AND created_date < %s "
        "ORDER BY created_date ASC"
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start, end))
            rows = cur.fetchall()
            result: List[Dict[str, Any]] = []
            for r in rows:
                result.append(
                    {
                        "api_request_log_id": r[0],
                        "event": None if r[1] is None else str(r[1]),
                        "request": None if r[2] is None else str(r[2]),
                        "response": None if r[3] is None else str(r[3]),
                        "created_date": r[4].isoformat() if r[4] is not None else None,
                    }
                )
            return result


def write_json_today() -> str:
    start, end, file_part = daterange_today()
    data = fetch_logs(start, end)
    data_dir = ensure_data_dir()
    out_path = os.path.join(data_dir, f"{file_part}_log.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


def clean_old_logs(retention_days: int = 7) -> list[str]:
    """Remove log files older than retention_days based on filename DDMMYYYY_log.json.

    Returns list of deleted file paths.
    """
    data_dir = ensure_data_dir()
    deleted: list[str] = []
    cutoff = datetime.now().date() - timedelta(days=retention_days - 1)
    for name in os.listdir(data_dir):
      if not name.endswith("_log.json") or len(name) < 13:
          continue
      date_str = name[:8]
      try:
          dt = datetime.strptime(date_str, "%d%m%Y").date()
      except Exception:
          # Skip files that don't match expected pattern
          continue
      if dt < cutoff:
          try:
              os.remove(os.path.join(data_dir, name))
              deleted.append(name)
          except Exception:
              # ignore deletion errors
              pass
    return deleted


if __name__ == "__main__":
    out = write_json_today()
    print(f"Log JSON written: {out}")
    removed = clean_old_logs(retention_days=7)
    if removed:
        print(f"Removed old logs (>7 days): {', '.join(removed)}")


