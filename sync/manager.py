from sqlalchemy import text
from .db import DatabaseManager
from .fact_order import process_fact_order
from .fact_delivery import process_fact_delivery


def create_sync_log_table(db_manager: DatabaseManager) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS tms_sync_log (
        id SERIAL PRIMARY KEY,
        sync_type VARCHAR(50) NOT NULL,
        start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        end_time TIMESTAMP WITH TIME ZONE,
        status VARCHAR(20) NOT NULL,
        records_processed INTEGER DEFAULT 0,
        error_message TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


def log_sync_start(db_manager: DatabaseManager, sync_type: str) -> int | None:
    sql = """
    INSERT INTO tms_sync_log (sync_type, status, start_time)
    VALUES (:sync_type, 'RUNNING', CURRENT_TIMESTAMP)
    RETURNING id;
    """
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        res = conn.execute(text(sql), {"sync_type": sync_type})
        row = res.fetchone()
        conn.commit()
    return int(row[0]) if row else None


def log_sync_complete(db_manager: DatabaseManager, sync_id: int, status: str, records_processed: int = 0, error_message: str | None = None) -> None:
    sql = """
    UPDATE tms_sync_log
    SET end_time = CURRENT_TIMESTAMP,
        status = :status,
        records_processed = :records_processed,
        error_message = :error_message
    WHERE id = :sync_id;
    """
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        conn.execute(text(sql), {
            "status": status,
            "records_processed": records_processed,
            "error_message": error_message,
            "sync_id": sync_id,
        })
        conn.commit()


def get_sync_status(db_manager: DatabaseManager, sync_type: str | None = None, limit: int = 10, offset: int = 0):
    if sync_type:
        sql = """
        SELECT sync_type, start_time, end_time, status, records_processed, error_message
        FROM tms_sync_log
        WHERE sync_type = :sync_type
        ORDER BY start_time DESC
        LIMIT :limit OFFSET :offset
        """
        params = {"sync_type": sync_type, "limit": limit, "offset": offset}
    else:
        sql = """
        SELECT sync_type, start_time, end_time, status, records_processed, error_message
        FROM tms_sync_log
        ORDER BY start_time DESC
        LIMIT :limit OFFSET :offset
        """
        params = {"limit": limit, "offset": offset}
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        res = conn.execute(text(sql), params)
        rows = res.fetchall()
    return rows


def count_sync_status(db_manager: DatabaseManager, sync_type: str | None = None) -> int:
    if sync_type:
        sql = "SELECT COUNT(1) FROM tms_sync_log WHERE sync_type = :sync_type"
        params = {"sync_type": sync_type}
    else:
        sql = "SELECT COUNT(1) FROM tms_sync_log"
        params = {}
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        res = conn.execute(text(sql), params)
        n = res.scalar_one()
    return int(n)


def run_sync(sync_type: str, date_from=None, date_to=None) -> None:
    dbm = DatabaseManager()
    create_sync_log_table(dbm)
    sync_id = log_sync_start(dbm, sync_type)
    try:
        if sync_type == 'fact_order':
            process_fact_order(date_from=date_from, date_to=date_to)
        elif sync_type == 'fact_delivery':
            process_fact_delivery(date_from=date_from, date_to=date_to)
        elif sync_type == 'both':
            process_fact_order(date_from=date_from, date_to=date_to)
            process_fact_delivery(date_from=date_from, date_to=date_to)
        else:
            raise ValueError(f"Invalid sync_type: {sync_type}")
        if sync_id is not None:
            log_sync_complete(dbm, sync_id, 'SUCCESS')
    except Exception as exc:
        if sync_id is not None:
            log_sync_complete(dbm, sync_id, 'FAILED', error_message=str(exc))
        raise


