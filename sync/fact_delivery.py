import pandas as pd
from sqlalchemy import text
from .db import DatabaseManager


def get_fact_delivery_query(date_from=None, date_to=None) -> str:
    where_clause = "WHERE 1=1"
    if date_from:
        s = date_from.strftime('%Y-%m-%d') if hasattr(date_from, 'strftime') else str(date_from)
        where_clause += f" AND c.faktur_date >= '{s}'"
    else:
        where_clause += " AND c.faktur_date >= '2024-12-01'"
    if date_to:
        s = date_to.strftime('%Y-%m-%d') if hasattr(date_to, 'strftime') else str(date_to)
        where_clause += f" AND c.faktur_date <= '{s}'"
    else:
        where_clause += " AND c.faktur_date <= CURRENT_DATE"

    return f"""
    SELECT
        a.route_id,
        a.manifest_reference,
        b.route_detail_id,
        b.order_id,
        c.do_number,
        c.faktur_date,
        DATE(a.created_date) AS created_date_only,
        a.created_date::TIMESTAMP::TIME as waktu,
        CASE WHEN c.delivery_date IS NOT NULL AND c.delivery_date >= '1900-01-01'::date AND c.delivery_date <= '2100-12-31'::date THEN c.delivery_date ELSE NULL END AS delivery_date,
        a.status,
        c.client_id,
        c.warehouse_id,
        c.origin_name,
        c.origin_city,
        c.customer_id,
        e.code,
        e."name",
        d.address,
        d.address_text,
        a.external_expedition_type,
        a.vehicle_id,
        a.driver_id,
        f.plate_number,
        g.driver_name,
        a.kenek_id,
        h.kenek_name,
        a.driver_status,
        a.manifest_integration_id,
        i.complete_time,
        SUM(j.net_price)::NUMERIC(15,2) as net_price,
        SUM(j.quantity_delivery)::NUMERIC(15,2) as quantity_delivery,
        SUM(j.quantity_faktur)::NUMERIC(15,2) as quantity_faktur,
        c.skip_count
    FROM PUBLIC.route AS a
    LEFT JOIN PUBLIC.route_detail AS b ON b.route_id = a.route_id
    LEFT JOIN PUBLIC."order" AS c ON c.order_id = b.order_id
    LEFT JOIN PUBLIC.mst_location_child as d ON d.mst_location_child_id = c.customer_id
    LEFT JOIN PUBLIC.mst_location_parent as e ON e.mst_location_parent_id = d.mst_location_parent_id
    LEFT JOIN PUBLIC.mst_vehicle as f ON f.mst_vehicle_id = a.vehicle_id
    LEFT JOIN PUBLIC.dma_driver as g ON g.driver_id = a.driver_id
    LEFT JOIN PUBLIC.dma_kenek as h ON h.kenek_id = a.kenek_id
    LEFT JOIN PUBLIC.driver_tasks as i on i.order_id = b.order_id
    LEFT JOIN PUBLIC.order_detail as j on j.order_id = b.order_id
    {where_clause}
    GROUP BY a.route_id, a.manifest_reference, b.route_detail_id, b.order_id, c.do_number, c.faktur_date, a.created_date, a.status, c.client_id, c.warehouse_id, c.origin_name, c.origin_city, c.customer_id, e.code, e."name", d.address, d.address_text, a.external_expedition_type, a.vehicle_id, a.driver_id, f.plate_number, g.driver_name, a.kenek_id, h.kenek_name, a.driver_status, a.manifest_integration_id, i.complete_time, c.delivery_date, c.skip_count
    """


def create_fact_delivery_table_schema_b(db_manager: DatabaseManager) -> None:
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tms_fact_delivery (
        route_id VARCHAR(50),
        manifest_reference VARCHAR(100),
        route_detail_id VARCHAR(50),
        order_id VARCHAR(50),
        do_number VARCHAR(100),
        faktur_date DATE,
        created_date_only DATE,
        waktu TIME,
        delivery_date DATE,
        status VARCHAR(50),
        client_id VARCHAR(50),
        warehouse_id VARCHAR(50),
        origin_name VARCHAR(200),
        origin_city VARCHAR(100),
        customer_id VARCHAR(50),
        code VARCHAR(50),
        name VARCHAR(200),
        address TEXT,
        address_text TEXT,
        external_expedition_type VARCHAR(50),
        vehicle_id VARCHAR(50),
        driver_id VARCHAR(50),
        plate_number VARCHAR(20),
        driver_name VARCHAR(100),
        kenek_id VARCHAR(50),
        kenek_name VARCHAR(100),
        driver_status VARCHAR(50),
        manifest_integration_id VARCHAR(100),
        complete_time TIMESTAMP,
        net_price NUMERIC(15,2),
        quantity_delivery NUMERIC(15,2),
        quantity_faktur NUMERIC(15,2),
        skip_count INTEGER,
        last_synced TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (route_id, route_detail_id, order_id)
    );
    """
    engine = db_manager.get_db_b_engine()
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.execute(text("ALTER TABLE IF EXISTS tms_fact_delivery ADD COLUMN IF NOT EXISTS skip_count INTEGER"))
        conn.commit()


def process_fact_delivery(date_from=None, date_to=None) -> None:
    db_manager = DatabaseManager()
    create_fact_delivery_table_schema_b(db_manager)
    query = get_fact_delivery_query(date_from=date_from, date_to=date_to)
    df = db_manager.execute_query_to_dataframe(query, 'A')
    if not df.empty:
        if 'faktur_date' in df.columns:
            df['faktur_date'] = pd.to_datetime(df['faktur_date'], errors='coerce').dt.date
        if 'created_date_only' in df.columns:
            df['created_date_only'] = pd.to_datetime(df['created_date_only'], errors='coerce').dt.date
        if 'delivery_date' in df.columns:
            df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce').dt.date
    if df.empty:
        return
    db_manager.upsert_dataframe_to_db(df, 'tms_fact_delivery', ['route_id', 'route_detail_id', 'order_id'], 'B')


