import pandas as pd
from sqlalchemy import text
from .db import DatabaseManager


def get_fact_order_query(date_from=None, date_to=None) -> str:
    where_clause = "WHERE 1=1"
    if date_from:
        s = date_from.strftime('%Y-%m-%d') if hasattr(date_from, 'strftime') else str(date_from)
        where_clause += f" AND a.faktur_date >= '{s}'"
    else:
        where_clause += " AND a.faktur_date >= '2024-12-01'"
    if date_to:
        s = date_to.strftime('%Y-%m-%d') if hasattr(date_to, 'strftime') else str(date_to)
        where_clause += f" AND a.faktur_date <= '{s}'"
    else:
        where_clause += " AND a.faktur_date <= CURRENT_DATE"

    return f"""
    SELECT DISTINCT ON (a.order_id)
      a.status,
      c.manifest_reference,
      a.order_id,
      c.manifest_integration_id,
      c.external_expedition_type,
      d.driver_name,
      e.code,
      a.faktur_date,
      a.created_date AS tms_created,
      CASE WHEN c.created_date IS NOT NULL THEN c.created_date::DATE ELSE NULL END AS route_created,
      CASE WHEN a.delivery_date IS NOT NULL AND a.delivery_date >= '1900-01-01'::date AND a.delivery_date <= '2100-12-31'::date THEN a.delivery_date ELSE NULL END AS delivery_date,
      c.route_id,
      a.updated_date AS tms_complete,
      CASE WHEN g.location_confirmation_timestamp IS NOT NULL AND g.location_confirmation_timestamp >= '1900-01-01'::timestamp AND g.location_confirmation_timestamp <= '2100-12-31'::timestamp THEN g.location_confirmation_timestamp::DATE ELSE NULL END as location_confirmation,
      SUM(od.quantity_faktur)::NUMERIC(15,2) AS faktur_total_quantity,
      SUM(od.quantity_delivery)::NUMERIC(15,2) AS tms_total_quantity,
      (SUM(od.quantity_delivery) - SUM(od.quantity_unloading))::NUMERIC(15,2) AS total_return,
      SUM(od.net_price)::NUMERIC(15,2) AS total_net_value,
      a.skip_count
    FROM "public"."order" AS a
    LEFT JOIN "public"."route_detail" AS b ON b.order_id = a.order_id
    LEFT JOIN "public"."route" AS c ON c.route_id = b.route_id
    LEFT JOIN "public"."dma_driver" AS d ON d.driver_id = c.driver_id
    LEFT JOIN "public"."mst_vehicle" AS e ON e.mst_vehicle_id = c.vehicle_id
    LEFT JOIN "public"."driver_tasks" AS f ON f.order_id = a.order_id
    LEFT JOIN "public"."driver_task_confirmations" AS g ON g.driver_task_id = f.driver_task_id
    LEFT JOIN "public"."order_detail" AS od ON od.order_id = a.order_id
    {where_clause}
    GROUP BY a.status, c.manifest_reference, a.order_id, c.manifest_integration_id, c.external_expedition_type, d.driver_name, e.code, a.faktur_date, a.created_date, c.created_date, a.delivery_date, c.route_id, a.updated_date, g.location_confirmation_timestamp, a.skip_count
    ORDER BY a.order_id, a.faktur_date DESC
    """


def create_fact_order_table_schema_b(db_manager: DatabaseManager) -> None:
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tms_fact_order (
        status VARCHAR(50),
        manifest_reference VARCHAR(100),
        order_id VARCHAR(50) PRIMARY KEY,
        manifest_integration_id VARCHAR(100),
        external_expedition_type VARCHAR(50),
        driver_name VARCHAR(100),
        code VARCHAR(50),
        faktur_date DATE,
        tms_created TIMESTAMP,
        route_created DATE,
        delivery_date DATE,
        route_id VARCHAR(50),
        tms_complete TIMESTAMP,
        location_confirmation DATE,
        faktur_total_quantity NUMERIC(15,2),
        tms_total_quantity NUMERIC(15,2),
        total_return NUMERIC(15,2),
        total_net_value NUMERIC(15,2),
        skip_count INTEGER,
        last_synced TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        # Ensure column exists when upgrading existing deployments
        conn.execute(text("ALTER TABLE IF EXISTS tms_fact_order ADD COLUMN IF NOT EXISTS skip_count INTEGER"))
        conn.commit()


def process_fact_order(date_from=None, date_to=None) -> None:
    db_manager = DatabaseManager()
    create_fact_order_table_schema_b(db_manager)
    query = get_fact_order_query(date_from=date_from, date_to=date_to)
    df = db_manager.execute_query_to_dataframe(query, 'A')
    if not df.empty:
        if 'route_created' in df.columns:
            df['route_created'] = pd.to_datetime(df['route_created'], errors='coerce').dt.date
        if 'location_confirmation' in df.columns:
            df['location_confirmation'] = pd.to_datetime(df['location_confirmation'], errors='coerce').dt.date
        if 'faktur_date' in df.columns:
            df['faktur_date'] = pd.to_datetime(df['faktur_date'], errors='coerce').dt.date
        if 'delivery_date' in df.columns:
            df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce').dt.date
    if df.empty:
        return
    db_manager.upsert_dataframe_to_db(df, 'tms_fact_order', ['order_id'], 'B')


