import os
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template, request, url_for, session
import json
from pathlib import Path
import requests
import random


def try_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)
    except Exception:
        pass


try_load_dotenv()


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing env var: {name}")
    return value

def _env(primary: str, fallback: Optional[str] = None, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(primary)
    if v is None and fallback is not None:
        v = os.getenv(fallback)
    if v is None:
        v = default
    return v

def get_env_int(name: str, default: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        if default is None:
            raise RuntimeError(f"Missing env var: {name}")
        return int(default)
    s = str(raw).strip().rstrip(';,')
    return int(s)

DB_HOST = _env("DATABASE_MAIN_HOST", "DB_HOST")
DB_PORT = int(_env("DATABASE_MAIN_PORT", "DB_PORT", "5432") or "5432")
DB_NAME = _env("DATABASE_MAIN_NAME", "DB_NAME")
DB_USER = _env("DATABASE_MAIN_USERNAME", "DB_USER")
DB_PASSWORD = _env("DATABASE_MAIN_PASS", "DB_PASSWORD")
WH_TYPE = get_env_int("WH_TYPE")


def get_db_connection():
    import psycopg2

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


app = Flask(__name__)
# Session signing is required for Flask sessions. Use SECRET_KEY if provided,
# otherwise fall back to SUPABASE_KEY to avoid a separate var in simple setups.
_secret = os.getenv("SECRET_KEY") or os.getenv("SUPABASE_KEY")
if not _secret:
    raise RuntimeError("Please set SECRET_KEY or SUPABASE_KEY for Flask session signing")
app.secret_key = _secret


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def validate_user_supabase(username: str, access_code: str) -> tuple[bool, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False, "Konfigurasi login belum lengkap (SUPABASE_URL/KEY)"
    try:
        endpoint = SUPABASE_URL.rstrip("/") + "/rest/v1/log_user_auth"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Accept": "application/json",
        }
        params = {
            "select": "username,access_code",
            "username": f"eq.{username}",
            "access_code": f"eq.{access_code}",
            "limit": 1,
        }
        resp = requests.get(endpoint, headers=headers, params=params, timeout=15)
        if resp.status_code != 200:
            snippet = resp.text[:200] if resp.text else ""
            return False, f"Autentikasi gagal (HTTP {resp.status_code}). {snippet}"
        data = resp.json() if resp.content else []
        if isinstance(data, list) and len(data) > 0:
            return True, "OK"
        return False, "Username atau access code salah"
    except requests.Timeout:
        return False, "Timeout menghubungi layanan login"
    except Exception as exc:
        return False, f"Gagal menghubungi layanan login: {exc}"


@app.before_request
def _gate_access():
    # Allow login without session
    open_paths = {"/login"}
    if request.path in open_paths or request.path.startswith("/static"):
        return
    if not session.get("authorized"):
        return redirect(url_for("login"))


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        # Generate simple 6-digit numeric captcha and store in session
        code = f"{random.randint(0, 999999):06d}"
        session["captcha_code"] = code
        return render_template("login.html", error=None, captcha_code=code)

    # POST
    username = str((request.form.get("username") or "").strip())
    access_code = str((request.form.get("access_code") or "").strip())
    captcha_input = str((request.form.get("captcha") or "").strip())
    expected_captcha = str(session.get("captcha_code") or "")
    valid = False
    error_msg = None
    if not captcha_input or captcha_input != expected_captcha:
        # Regenerate captcha for the next attempt
        code = f"{random.randint(0, 999999):06d}"
        session["captcha_code"] = code
        error = "Captcha salah"
        return render_template("login.html", error=error, captcha_code=code)

    if username and access_code:
        valid, error_msg = validate_user_supabase(username, access_code)
    if valid:
        session["authorized"] = True
        session["username"] = username
        return redirect(url_for("index"))
    error = error_msg or "Username atau access code tidak valid"
    # Regenerate captcha on any failure
    code = f"{random.randint(0, 999999):06d}"
    session["captcha_code"] = code
    return render_template("login.html", error=error, captcha_code=code)



# ---------- Menu 1: Update Lokasi Customer ----------


def fetch_warehouses() -> List[Tuple[int, str]]:
    sql = (
        "SELECT mlc.mst_location_child_id, mlc.name FROM mst_location_child mlc "
        "LEFT JOIN mst_location_parent mlp ON mlc.mst_location_parent_id = mlp.mst_location_parent_id "
        "WHERE mlp.type_id = %s"
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (WH_TYPE,))
            rows = cur.fetchall()
            return [(int(r[0]), str(r[1])) for r in rows]


@app.get("/menu/update-lokasi")
def menu_update_lokasi():
    warehouses = fetch_warehouses()
    return render_template("update_lokasi.html", warehouses=warehouses)


def fetch_orders_by_faktur_and_warehouse(faktur_id: str, warehouse_id: int):
    sql = (
        'SELECT od.faktur_date, od.faktur_id, od.order_id, od.warehouse_id, '
        '       mlc.mst_location_child_id, mlc.code, mlc.name '
        'FROM "order" od '
        'LEFT JOIN mst_location_child mlc ON od.customer_id = mlc.mst_location_child_id '
        'WHERE od.faktur_id = %s AND od.warehouse_id = %s'
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (faktur_id, warehouse_id))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.get("/api/orders")
def api_orders():
    faktur_id = request.args.get("faktur_id", "").strip()
    warehouse_id = request.args.get("warehouse_id", "").strip()
    if not faktur_id or not warehouse_id.isdigit():
        return jsonify({"status": 400, "message": "Invalid parameters"}), 400
    rows = fetch_orders_by_faktur_and_warehouse(faktur_id, int(warehouse_id))
    return jsonify({"status": 200, "data": rows})


def fetch_all_locations() -> List[Dict[str, Any]]:
    sql = "SELECT mlc.mst_location_child_id, mlc.code, mlc.name FROM mst_location_child mlc"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [
                {"mst_location_child_id": int(r[0]), "code": str(r[1]), "name": str(r[2])}
                for r in cur.fetchall()
            ]


@app.get("/api/locations")
def api_locations():
    return jsonify({"status": 200, "data": fetch_all_locations()})


def update_order_customer_location(faktur_id: str, new_customer_id: int) -> int:
    sql = 'UPDATE "order" SET customer_id = %s WHERE faktur_id = %s'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (new_customer_id, faktur_id))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/orders/update-location")
def api_update_location():
    payload = request.get_json(silent=True) or {}
    faktur_id = str(payload.get("faktur_id", "")).strip()
    new_customer_id = payload.get("customer_id")
    if not faktur_id or not isinstance(new_customer_id, int):
        return jsonify({"status": 400, "message": "Invalid payload"}), 400
    affected = update_order_customer_location(faktur_id, new_customer_id)
    return jsonify({"status": 200, "affected": affected})


# ---------- Menu 2: Update Uncheck Document Reconciliation ----------


@app.get("/menu/uncheck-reconciliation")
def menu_uncheck_reconciliation():
    return render_template("uncheck_recon.html")


def fetch_odr_by_faktur(faktur_id: str) -> List[Dict[str, Any]]:
    sql = (
        'SELECT odr.*, od.faktur_id FROM order_document_reconciliation odr '
        'LEFT JOIN "order" od ON od.order_id = odr.order_id '
        'WHERE od.faktur_id = %s'
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (faktur_id,))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.get("/api/reconciliation")
def api_reconciliation_find():
    faktur_id = request.args.get("faktur_id", "").strip()
    if not faktur_id:
        return jsonify({"status": 400, "message": "faktur_id required"}), 400
    rows = fetch_odr_by_faktur(faktur_id)
    if not rows:
        return jsonify({"status": 404, "message": "Data tidak ditemukan"}), 404
    return jsonify({"status": 200, "data": rows})


def delete_reconciliation_by_order_id(order_id: int) -> int:
    sql = "DELETE FROM order_document_reconciliation WHERE order_id = %s"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (order_id,))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/reconciliation/uncheck")
def api_reconciliation_uncheck():
    payload = request.get_json(silent=True) or {}
    order_id = payload.get("order_id")
    if not isinstance(order_id, int):
        return jsonify({"status": 400, "message": "order_id required"}), 400
    affected = delete_reconciliation_by_order_id(order_id)
    return jsonify({"status": 200, "affected": affected})


# ---------- Menu 3: Log Viewer ----------


def data_log_dir() -> Path:
    base = Path(__file__).resolve().parent
    d = base / "data_log"
    d.mkdir(parents=True, exist_ok=True)
    return d


@app.get("/menu/log-viewer")
def menu_log_viewer():
    return render_template("log_viewer.html")


@app.get("/api/log/files")
def api_log_files():
    d = data_log_dir()
    files = sorted([p.name for p in d.glob("*_log.json")])
    return jsonify({"status": 200, "data": files})


def _load_log_file(file_name: str):
    d = data_log_dir()
    p = d / file_name
    if not p.exists() or not p.is_file():
        return []
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


@app.get("/api/log/search")
def api_log_search():
    file_name = request.args.get("file", "").strip()
    q_event = request.args.get("event", "").strip()
    q_request = request.args.get("request", "").strip()

    if not file_name:
        return jsonify({"status": 400, "message": "file required"}), 400

    data = _load_log_file(file_name)

    def _match(val: str, needle: str) -> bool:
        if not needle:
            return True
        if val is None:
            return False
        return needle.lower() in str(val).lower()

    results = []
    for row in data:
        if not isinstance(row, dict):
            continue
        if _match(row.get("event"), q_event) and _match(row.get("request"), q_request):
            results.append(row)

    return jsonify({"status": 200, "data": results})


# ---------- Menu 4: PRODUCT to ROUTE ----------


@app.get("/menu/product-to-route")
def menu_product_to_route():
    return render_template("product_to_route.html")


def fetch_product_to_route(sku: str, start_date: str, end_date: str):
    sql = (
        "SELECT ro.route_id, ro.manifest_reference, ro.status AS route_status, "
        "       o.faktur_id, o.status AS order_status, od.quantity_faktur, o.faktur_date "
        "FROM route ro "
        "LEFT JOIN route_detail rd ON rd.route_id = ro.route_id "
        "LEFT JOIN \"order\" o ON o.order_id = rd.order_id "
        "LEFT JOIN order_detail od ON od.order_id = o.order_id "
        "LEFT JOIN mst_product mp ON mp.mst_product_id = od.product_id "
        "WHERE mp.sku = %s AND o.faktur_date BETWEEN DATE %s AND DATE %s "
        "ORDER BY o.faktur_date"
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (sku, start_date, end_date))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.get("/api/product-to-route")
def api_product_to_route():
    sku = request.args.get("sku", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    if not sku or not start_date or not end_date:
        return jsonify({"status": 400, "message": "sku, start_date, end_date required"}), 400
    rows = fetch_product_to_route(sku, start_date, end_date)
    return jsonify({"status": 200, "data": rows})


# ---------- Menu 5: Update WMS Integrasi ----------


@app.get("/menu/wms-integrasi")
def menu_wms_integrasi():
    return render_template("wms_integrasi.html")


def fetch_wms_integration_by_faktur(faktur_id: str):
    sql = (
        'SELECT odr.order_id, odr.faktur_id, odr.faktur_date, odr.status, odr.order_integration_id '
        'FROM "order" odr '
        'WHERE odr.faktur_id = %s AND (odr.order_integration_id = \"pending\" OR odr.order_integration_id IS NULL)'
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (faktur_id,))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.get("/api/wms-integration")
def api_wms_integration_find():
    faktur_id = request.args.get("faktur_id", "").strip()
    if not faktur_id:
        return jsonify({"status": 400, "message": "faktur_id required"}), 400
    rows = fetch_wms_integration_by_faktur(faktur_id)
    return jsonify({"status": 200, "data": rows})


def update_wms_integration(order_id: int, new_status: str) -> int:
    sql = 'UPDATE "order" SET order_integration_id = %s WHERE order_id = %s'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (new_status, order_id))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/wms-integration/update")
def api_wms_integration_update():
    payload = request.get_json(silent=True) or {}
    order_id = payload.get("order_id")
    new_status = str(payload.get("order_integration_id", "")).strip()
    if not isinstance(order_id, int) or not new_status:
        return jsonify({"status": 400, "message": "order_id and order_integration_id required"}), 400
    affected = update_wms_integration(order_id, new_status)
    return jsonify({"status": 200, "affected": affected})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)


