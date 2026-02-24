"""
Konfigurasi log viewer: mapping event -> slug (nama file DB) dan daftar event untuk dropdown.
Struktur folder: data_log / DDMMYYYY / {slug}.db
"""

# (slug, label untuk dropdown)
LOG_EVENT_SLUGS = [
    ("syncing_inventory", "[ARMOS -> WMS] Syncing Inventory"),
    ("synchronizing_order_manifest", "[ARMOS -> WMS] Synchronizing Order Manifest"),
    ("synchronizing_route_manifest_generation", "[ARMOS -> WMS] Synchronizing Route Manifest Generation"),
    ("patch_order_status_sql", "[ARMOS -> SQL] Patch Order Status"),
    ("patch_order_status_atena", "[ARMOS -> ATENA] Patch Order Status"),
    ("picklist_route", "[ARMOS -> SQL] Picklist Route"),
    ("feed_order_v2_sql_tms", "[FEED ORDER V2 SQL -> TMS]"),
    ("feed_order_v2_atena_tms", "[FEED ORDER V2 ATENA -> TMS]"),
    ("webhook_good_issue_results", "Webhook Good Issue"),
    ("other", "Lainnya (other)"),
]

# Konfigurasi kolom "Cari Request" per slug: None = disabled, dict = label + placeholder + search_field
LOG_EVENT_REQUEST_CONFIG = {
    "syncing_inventory": None,  # disabled
    "synchronizing_order_manifest": {"label": "Masukan Do Reference", "placeholder": 'contoh do_reference":"B03SI2505-0558"', "search_field": "do_reference"},
    "synchronizing_route_manifest_generation": {"label": "Masukan Manifest Reference", "placeholder": 'contoh Manifest Reference":"RMSDA0120250508#34"', "search_field": "manifest_reference"},
    "patch_order_status_sql": {"label": "Masukan Faktur Reference", "placeholder": 'contoh Faktur Reference":"M30SI2505-0009"', "search_field": "faktur_reference"},
    "patch_order_status_atena": {"label": "Masukan Route Id", "placeholder": 'contoh Route Id:"RMSDA0120250919#66"', "search_field": "route_id"},
    "picklist_route": {"label": "Masukan Route Id", "placeholder": 'contoh Route Id:"RMSDA0120250919#66"', "search_field": "header.route_id"},
    "feed_order_v2_sql_tms": {"label": "Masukan outbound reference", "placeholder": 'contoh Outbound Reference "C10SI2509-0041"', "search_field": "outbound_reference"},
    "feed_order_v2_atena_tms": {"label": "Masukan outbound reference", "placeholder": 'contoh Outbound Reference "C10SI2509-0041"', "search_field": "outbound_reference"},
    "webhook_good_issue_results": {"label": "Masukan manifest reference", "placeholder": 'contoh manifest_reference":"RMSDA0120251118#196"', "search_field": "manifest_reference"},
    "other": {"label": "Cari Request", "placeholder": "Masukan Request", "search_field": None},
}


def event_to_slug(event: str | None) -> str:
    """Map raw event string dari DB ke slug (nama file .db)."""
    if not event:
        return "other"
    e = event.strip()
    if e == "[ARMOS -> WMS] Syncing Inventory":
        return "syncing_inventory"
    if "[ARMOS -> WMS] Synchronizing Order" in e and "Manifest" in e:
        return "synchronizing_order_manifest"
    if "[ARMOS -> WMS] Synchronizing Route" in e and "Manifest Generation" in e:
        return "synchronizing_route_manifest_generation"
    if e.startswith("[ARMOS -> SQL] Patch Order Status"):
        return "patch_order_status_sql"
    if e.startswith("[ARMOS -> ATENA] Patch Order Status"):
        return "patch_order_status_atena"
    if e.startswith("[ARMOS -> SQL] Picklist Route"):
        return "picklist_route"
    if e == "[FEED ORDER V2 SQL -> TMS]":
        return "feed_order_v2_sql_tms"
    if e == "[FEED ORDER V2 ATENA -> TMS]":
        return "feed_order_v2_atena_tms"
    if e == "[WMS -> ARMOS] WEBHOOK_GOOD_ISSUE_RESULTS":
        return "webhook_good_issue_results"
    return "other"
