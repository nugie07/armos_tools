"""
log_konversi.py

Export rows from table sys_api_request_log for TODAY into SQLite files under data_log/.
- Struktur: data_log / DDMMYYYY / {slug}.db (satu file per tipe event)
- Table: log (api_request_log_id, event, request, response, created_date) dengan index
- Overwrites if exists
- Intended to be scheduled every 30 minutes (cron or scheduler)
"""

import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

from log_config import LOG_EVENT_SLUGS, event_to_slug


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


def ensure_data_dir_date(date_folder: str) -> str:
    """data_log/DDMMYYYY/"""
    data_dir = ensure_data_dir()
    path = os.path.join(data_dir, date_folder)
    os.makedirs(path, exist_ok=True)
    return path


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


def _write_one_sqlite(data: List[Dict[str, Any]], out_path: str) -> None:
    """Tulis satu file SQLite (satu event slug) dengan index."""
    conn = sqlite3.connect(out_path, timeout=30)
    try:
        conn.execute("DROP TABLE IF EXISTS log")
        conn.execute(
            "CREATE TABLE log ("
            "api_request_log_id INTEGER, event TEXT, request TEXT, response TEXT, created_date TEXT)"
        )
        conn.execute("CREATE INDEX idx_log_event ON log(event)")
        conn.execute("CREATE INDEX idx_log_created_date ON log(created_date)")
        for row in data:
            conn.execute(
                "INSERT INTO log (api_request_log_id, event, request, response, created_date) VALUES (?, ?, ?, ?, ?)",
                (
                    row.get("api_request_log_id"),
                    row.get("event") if row.get("event") is not None else None,
                    row.get("request") if row.get("request") is not None else None,
                    row.get("response") if row.get("response") is not None else None,
                    row.get("created_date") if row.get("created_date") is not None else None,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def write_logs_to_sqlite_per_event(data: List[Dict[str, Any]], file_part: str) -> List[str]:
    """Group by event slug, tulis data_log/file_part/{slug}.db untuk setiap slug. Return list path."""
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in data:
        slug = event_to_slug(row.get("event"))
        grouped[slug].append(row)
    date_dir = ensure_data_dir_date(file_part)
    written: List[str] = []
    for slug, rows in grouped.items():
        out_path = os.path.join(date_dir, f"{slug}.db")
        _write_one_sqlite(rows, out_path)
        written.append(out_path)
    return written


def write_sqlite_today() -> List[str]:
    """Export log hari ini ke data_log/DDMMYYYY/{slug}.db. Return list path yang ditulis."""
    start, end, file_part = daterange_today()
    data = fetch_logs(start, end)
    return write_logs_to_sqlite_per_event(data, file_part)


def clean_old_logs(retention_days: int = 7) -> list[str]:
    """Hapus folder log data_log/DDMMYYYY/ yang lebih lama dari retention_days."""
    data_dir = ensure_data_dir()
    deleted: list[str] = []
    cutoff = datetime.now().date() - timedelta(days=retention_days - 1)
    for name in os.listdir(data_dir):
        path = os.path.join(data_dir, name)
        if not os.path.isdir(path) or len(name) != 8:
            continue
        try:
            dt = datetime.strptime(name, "%d%m%Y").date()
        except Exception:
            continue
        if dt < cutoff:
            try:
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)
                deleted.append(name)
            except Exception:
                pass
    return deleted


if __name__ == "__main__":
    written = write_sqlite_today()
    print(f"Log SQLite written: {len(written)} file(s) -> {written}")
    removed = clean_old_logs(retention_days=7)
    if removed:
        print(f"Removed old log folders (>7 days): {', '.join(removed)}")


