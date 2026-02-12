import os
import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template, request, url_for, session, send_file, make_response
import json
from pathlib import Path
import requests
import math
import random
from werkzeug.datastructures import FileStorage

from konversi import convert_excel_to_json  # type: ignore
import send_orders as send_orders_module  # type: ignore
from concurrent.futures import ThreadPoolExecutor
import uuid
from datetime import datetime
from sync.manager import run_sync as sync_run, get_sync_status as sync_get_status, create_sync_log_table, count_sync_status as sync_count_status
from sync.db import DatabaseManager
import import_lokasi


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


def get_db_config_by_env(env: str = "preprod") -> Dict[str, Any]:
    """
    Ambil konfigurasi database berdasarkan environment.
    env: 'preprod' atau 'prod'
    """
    env_lower = env.lower()
    if env_lower not in ["preprod", "prod"]:
        raise ValueError(f"Environment harus 'preprod' atau 'prod', mendapat: {env}")
    
    prefix = "DATABASE_PREPROD" if env_lower == "preprod" else "DATABASE_PROD"
    
    config = {
        "host": _env(f"{prefix}_HOST"),
        "port": int(_env(f"{prefix}_PORT", default="5432") or "5432"),
        "dbname": _env(f"{prefix}_NAME"),
        "user": _env(f"{prefix}_USERNAME"),
        "password": _env(f"{prefix}_PASS"),
    }
    
    missing = []
    if not config["host"]:
        missing.append(f"{prefix}_HOST")
    if not config["dbname"]:
        missing.append(f"{prefix}_NAME")
    if not config["user"]:
        missing.append(f"{prefix}_USERNAME")
    if not config["password"]:
        missing.append(f"{prefix}_PASS")
    
    if missing:
        raise RuntimeError(
            f"Konfigurasi database untuk {env.upper()} tidak lengkap. "
            f"Variabel environment yang diperlukan: {', '.join(missing)}"
        )
    
    return config


def get_db_connection_by_env(env: str = "preprod"):
    """
    Buat koneksi database berdasarkan environment.
    env: 'preprod' atau 'prod'
    """
    import psycopg2
    config = get_db_config_by_env(env)
    
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )


app = Flask(__name__)

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Session signing is required for Flask sessions. Use SECRET_KEY if provided,
# otherwise fall back to SUPABASE_KEY to avoid a separate var in simple setups.
_secret = os.getenv("SECRET_KEY") or os.getenv("SUPABASE_KEY")
if not _secret:
    raise RuntimeError("Please set SECRET_KEY or SUPABASE_KEY for Flask session signing")
app.secret_key = _secret

# Global error handlers
@app.errorhandler(404)
def not_found(error):
    logger.warning(f"404 Not Found: {request.url}")
    return render_template("error.html", error_code=404, error_message="Halaman tidak ditemukan"), 404

@app.errorhandler(500)
def internal_error(error):
    error_trace = traceback.format_exc()
    logger.error(f"500 Internal Server Error: {error_trace}")
    logger.error(f"Request URL: {request.url}")
    logger.error(f"Request Method: {request.method}")
    return render_template("error.html", error_code=500, error_message="Terjadi kesalahan server"), 500

@app.errorhandler(Exception)
def handle_exception(e):
    error_trace = traceback.format_exc()
    logger.error(f"Unhandled Exception: {str(e)}")
    logger.error(f"Traceback: {error_trace}")
    logger.error(f"Request URL: {request.url}")
    logger.error(f"Request Method: {request.method}")
    return render_template("error.html", error_code=500, error_message=f"Error: {str(e)}"), 500


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Background executor for sync jobs
_executor = ThreadPoolExecutor(max_workers=2)
_jobs: dict[str, dict[str, Any]] = {}

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
    # Allow login without session and ignore favicon
    open_paths = {"/login", "/favicon.ico", "/logout"}
    if request.path in open_paths or request.path.startswith("/static"):
        return
    if not session.get("authorized"):
        return redirect(url_for("login"))


@app.route("/")
def index():
    return render_template("index.html")


@app.get("/logout")
def logout():
    session.clear()
    resp = redirect(url_for("login"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        # Keep existing captcha to avoid race with additional GETs (e.g., favicon)
        code = session.get("captcha_code")
        if not code:
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
    search_field = request.args.get("search_field", "").strip()  # Field spesifik yang akan dicari di request JSON
    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1
    try:
        per_page = max(1, int(request.args.get("per_page", "10")))
    except Exception:
        per_page = 10

    if not file_name:
        return jsonify({"status": 400, "message": "file required"}), 400
    if not q_event:
        return jsonify({"status": 400, "message": "event keyword required"}), 400

    data = _load_log_file(file_name)

    def _match(val: str, needle: str) -> bool:
        if not needle:
            return True
        if val is None:
            return False
        return needle.lower() in str(val).lower()

    def _match_request_field(request_data: str, search_field: str, search_value: str) -> bool:
        """
        Cari field spesifik di request JSON.
        Jika search_field diberikan, parse request JSON dan cari field tersebut.
        Mendukung nested path seperti "header.route_id" atau "route_id".
        Jika tidak, gunakan substring search seperti sebelumnya.
        """
        if not search_field or not search_value:
            # Jika tidak ada search_field, gunakan substring search
            return _match(request_data, search_value)
        
        if not request_data:
            return False
        
        try:
            # Parse request JSON
            if isinstance(request_data, str):
                request_obj = json.loads(request_data)
            else:
                request_obj = request_data
            
            if not isinstance(request_obj, dict):
                return False
            
            # Support nested path seperti "header.route_id"
            field_value = None
            if '.' in search_field:
                # Nested path: "header.route_id"
                parts = search_field.split('.')
                current = request_obj
                for part in parts:
                    if isinstance(current, dict):
                        current = current.get(part)
                        if current is None:
                            break
                    else:
                        current = None
                        break
                field_value = current
            else:
                # Root level: "route_id"
                field_value = request_obj.get(search_field)
            
            if field_value is None:
                return False
            
            # Match case-insensitive
            return search_value.lower() in str(field_value).lower()
        except (json.JSONDecodeError, TypeError, AttributeError):
            # Jika gagal parse, fallback ke substring search
            return _match(request_data, search_value)

    results = []
    for row in data:
        if not isinstance(row, dict):
            continue
        
        # Match event
        # Untuk "[ARMOS -> SQL] Picklist Route", gunakan startswith karena di JSON ada "ID" di akhir
        # Contoh: "[ARMOS -> SQL] Picklist Route ID 33846"
        event_str = row.get("event", "").strip()
        if q_event == "[ARMOS -> SQL] Picklist Route":
            event_match = event_str.startswith("[ARMOS -> SQL] Picklist Route")
        else:
            # Untuk event lain, gunakan exact match
            event_match = event_str == q_event
        
        # Match request berdasarkan search_field atau substring
        # Jika search_field kosong dan q_request kosong (field disabled), match semua
        if not search_field and not q_request:
            request_match = True  # Field disabled, match semua
        else:
            request_match = _match_request_field(row.get("request"), search_field, q_request)
        
        if event_match and request_match:
            results.append(row)

    total = len(results)
    pages = max(1, math.ceil(total / per_page))
    start = (page - 1) * per_page
    end = start + per_page
    paged = results[start:end]

    return jsonify({
        "status": 200,
        "data": paged,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": pages
    })


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
        'WHERE odr.faktur_id = %s'
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


# ---------- Menu 6: Upload Order (Convert & Send) ----------


@app.get("/menu/convert-send")
def menu_convert_send():
    return render_template("convert_send.html")


@app.post("/api/convert-send")
def api_convert_send():
    steps: List[Dict[str, str]] = []
    base_dir = Path(__file__).resolve().parent
    upload_target = base_dir / "template_feed_order.xlsx"
    output_json = base_dir / "output_order.json"
    try:
        f: Optional[FileStorage] = request.files.get("file")  # type: ignore
        if f is None or f.filename == "":
            return jsonify({"status": 400, "message": "File .xlsx wajib diunggah."}), 400

        # Simpan file sebagai template_feed_order.xlsx
        try:
            f.save(str(upload_target))
            steps.append({"status": "OK", "message": f"File diupload sebagai {upload_target.name}"})
        except Exception as exc:
            steps.append({"status": "ERROR", "message": f"Gagal menyimpan file: {exc}"})
            return jsonify({"status": 500, "message": "Gagal menyimpan file.", "steps": steps}), 500

        # Jalankan konversi
        try:
            convert_excel_to_json(str(upload_target), str(output_json))
            steps.append({"status": "OK", "message": "Konversi Excel → JSON berhasil."})
        except Exception as exc:
            steps.append({"status": "ERROR", "message": f"Gagal konversi: {exc}"})
            return jsonify({"status": 500, "message": "Konversi gagal.", "steps": steps}), 500

        # Muat hasil JSON untuk ditampilkan di UI
        converted_json = None
        try:
            if output_json.exists():
                with output_json.open("r", encoding="utf-8") as rf:
                    converted_json = json.load(rf)
        except Exception as exc:
            steps.append({"status": "WARN", "message": f"Gagal membaca output JSON: {exc}"})

        # Jalankan pengiriman order
        try:
            orders = send_orders_module.iter_orders_from_json(str(output_json))
            token = send_orders_module.login_get_token(
                os.getenv("SEND_ORDER_USERNAME", "integration_sql_x_armos_system"),
                os.getenv("SEND_ORDER_PASSWORD", "QW5kYWkga3UgdGFodSBLYXBhbiB0aWJhIGFqYWxrdSBLdSBha2FuIG1lbW9ob24gVHVoYW4sIHRvbG9uZyBwYW5qYW5na2FuIHVtdXJrdSBBbmRhaSBrdSB0YWh1IChrdSB0YWh1KSBLYXBhbiB0aWJhIG1hc2FrdQ=="),
            )
            ok = 0
            fail = 0
            for order in orders:
                payload = send_orders_module.build_payload(order)
                ref = payload.get("outbound_reference")
                try:
                    resp = send_orders_module.send_order(token, payload)
                    content = resp.text
                    if resp.headers.get("content-type", "").startswith("application/json"):
                        try:
                            content = json.dumps(resp.json())
                        except Exception:
                            pass
                    if resp.ok:
                        steps.append({"status": "OK", "message": f"Kirim {ref}: {resp.status_code} -> {content[:500]}"})
                        ok += 1
                    else:
                        steps.append({"status": "ERROR", "message": f"Kirim {ref}: {resp.status_code} -> {content[:500]}"})
                        fail += 1
                except Exception as exc:
                    steps.append({"status": "ERROR", "message": f"Kirim {ref}: {exc}"})
                    fail += 1
            overall_msg = f"Selesai kirim. Berhasil: {ok}, Gagal: {fail}"
            return jsonify({
                "status": 200,
                "message": overall_msg,
                "steps": steps,
                "converted_json": converted_json,
            })
        except Exception as exc:
            steps.append({"status": "ERROR", "message": f"Gagal proses pengiriman: {exc}"})
            return jsonify({"status": 500, "message": "Gagal mengirim order.", "steps": steps, "converted_json": converted_json}), 500
    except Exception as exc:
        steps.append({"status": "ERROR", "message": f"Kesalahan tak terduga: {exc}"})
        return jsonify({"status": 500, "message": "Kesalahan tak terduga.", "steps": steps}), 500


# ---------- Menu 7: Sync Manager & Dashboard ----------


def _parse_date(s: Optional[str]):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


@app.get("/menu/sync-manager")
def menu_sync_manager():
    return render_template("sync_manager.html")


# Removed separate sync dashboard page; merged into Sync Manager


@app.post("/api/sync/run")
def api_sync_run():
    payload = request.get_json(silent=True) or {}
    sync_type = str(payload.get("sync_type", "")).strip()
    date_from = _parse_date(str(payload.get("date_from", "")).strip())
    date_to = _parse_date(str(payload.get("date_to", "")).strip())
    if sync_type not in {"fact_order", "fact_delivery", "both"}:
        return jsonify({"status": 400, "message": "sync_type must be fact_order|fact_delivery|both"}), 400

    # Ensure log table exists
    try:
        create_sync_log_table(DatabaseManager())
    except Exception:
        pass

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "PENDING", "sync_type": sync_type, "date_from": str(date_from or ""), "date_to": str(date_to or ""), "started_at": None, "finished_at": None, "error": None}

    def _task():
        _jobs[job_id]["status"] = "RUNNING"
        _jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
        try:
            sync_run(sync_type, date_from=date_from, date_to=date_to)
            _jobs[job_id]["status"] = "SUCCESS"
        except Exception as exc:
            _jobs[job_id]["status"] = "FAILED"
            _jobs[job_id]["error"] = str(exc)
        finally:
            _jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()

    _executor.submit(_task)
    return jsonify({"status": 200, "job_id": job_id})


@app.get("/api/sync/status")
def api_sync_status():
    sync_type = request.args.get("sync_type", "").strip() or None
    try:
        limit = max(1, int(request.args.get("limit", "20")))
    except Exception:
        limit = 20
    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1
    offset = (page - 1) * limit
    rows = sync_get_status(DatabaseManager(), sync_type=sync_type, limit=limit, offset=offset)
    total = sync_count_status(DatabaseManager(), sync_type=sync_type)
    data = []
    for r in rows:
        data.append({
            "sync_type": r[0],
            "start_time": r[1].isoformat() if r[1] else None,
            "end_time": r[2].isoformat() if r[2] else None,
            "status": r[3],
            "records_processed": r[4],
            "error_message": r[5],
        })
    # Build stats
    success = len([1 for r in rows if r[3] == "SUCCESS"])
    failed = len([1 for r in rows if r[3] == "FAILED"])
    last_sync = data[0]["start_time"] if data else None
    pages = max(1, (total + limit - 1) // limit)
    return jsonify({
        "status": 200,
        "stats": {"total_syncs": total, "successful_syncs": success, "failed_syncs": failed, "last_sync": last_sync},
        "sync_history": data,
        "page": page,
        "per_page": limit,
        "pages": pages,
        "total": total
    })


@app.get("/api/sync/job/<job_id>")
def api_sync_job(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": 404, "message": "job not found"}), 404
    return jsonify({"status": 200, "job": job})


# ---------- Menu 8: Update Quantity Delivery / Unloading ----------


def fetch_warehouses_for_type() -> List[Tuple[int, str]]:
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


@app.get("/menu/update-qty-unloading")
def menu_update_qty_unloading():
    warehouses = fetch_warehouses_for_type()
    return render_template("update_qty_unloading.html", warehouses=warehouses)


def find_order_detail_for_update(warehouse_id: int, faktur_id: str, sku: str):
    sql = (
        'SELECT od.order_detail_id, mp.sku, od.quantity_faktur, od.quantity_unloading '\
        'FROM order_detail od '\
        'LEFT JOIN "order" o ON o.order_id = od.order_id '\
        'LEFT JOIN mst_product mp on mp.mst_product_id = od.product_id '\
        'WHERE o.warehouse_id = %s AND o.faktur_id = %s AND mp.sku = %s'
    )
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (warehouse_id, faktur_id, sku))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "order_detail_id": int(row[0]),
                "sku": str(row[1]),
                "quantity_faktur": float(row[2]) if row[2] is not None else 0,
                "quantity_unloading": float(row[3]) if row[3] is not None else 0,
            }


@app.get("/api/qty-unloading/find")
def api_qty_unloading_find():
    try:
        warehouse_id = int(request.args.get("warehouse_id", "0"))
    except Exception:
        return jsonify({"status": 400, "message": "warehouse_id invalid"}), 400
    faktur_id = request.args.get("faktur_id", "").strip()
    sku = request.args.get("sku", "").strip()
    if not warehouse_id or not faktur_id or not sku:
        return jsonify({"status": 400, "message": "warehouse_id, faktur_id, sku wajib diisi"}), 400
    row = find_order_detail_for_update(warehouse_id, faktur_id, sku)
    if not row:
        return jsonify({"status": 404, "message": "Data tidak ditemukan"}), 404
    return jsonify({"status": 200, "data": row})


def update_order_detail_unloading(order_detail_id: int, new_unloading: float) -> int:
    sql = "UPDATE order_detail SET quantity_unloading = %s WHERE order_detail_id = %s"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (new_unloading, order_detail_id))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/qty-unloading/update")
def api_qty_unloading_update():
    payload = request.get_json(silent=True) or {}
    order_detail_id = payload.get("order_detail_id")
    new_val = payload.get("quantity_unloading")
    if not isinstance(order_detail_id, int):
        return jsonify({"status": 400, "message": "order_detail_id invalid"}), 400
    try:
        new_unloading = float(new_val)
    except Exception:
        return jsonify({"status": 400, "message": "quantity_unloading invalid"}), 400
    affected = update_order_detail_unloading(order_detail_id, new_unloading)
    return jsonify({"status": 200, "affected": affected})


# ---------- Menu 9: Hapus Driver Cost ----------


@app.get("/menu/hapus-driver-cost")
def menu_hapus_driver_cost():
    return render_template("hapus_driver_cost.html")


def find_driver_costs(manifest_reference: str, limit: int, offset: int) -> Tuple[List[Dict[str, Any]], int]:
    sql = (
        'SELECT oc.order_cost_id, rt.manifest_reference, oc.nominal, dd.driver_name, oc.receipt_picture '
        'FROM order_cost oc '
        'LEFT JOIN route_detail rd ON rd.order_id = oc.order_id '
        'LEFT JOIN route rt ON rt.route_id = rd.route_id '
        'LEFT JOIN dma_driver dd ON dd.driver_id = oc."driverIdDriverId" '
        'WHERE rt.manifest_reference = %s '
        'ORDER BY oc.order_cost_id DESC '
        'LIMIT %s OFFSET %s'
    )
    sql_count = 'SELECT COUNT(1) FROM order_cost oc LEFT JOIN route_detail rd ON rd.order_id = oc.order_id LEFT JOIN route rt ON rt.route_id = rd.route_id WHERE rt.manifest_reference = %s'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_count, (manifest_reference,))
            total = int(cur.fetchone()[0])
            cur.execute(sql, (manifest_reference, limit, offset))
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows], total


@app.get("/api/driver-cost/list")
def api_driver_cost_list():
    mr = request.args.get("manifest_reference", "").strip()
    if not mr:
        return jsonify({"status": 400, "message": "manifest_reference wajib diisi"}), 400
    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1
    per_page = 10
    offset = (page - 1) * per_page
    rows, total = find_driver_costs(mr, per_page, offset)
    pages = max(1, (total + per_page - 1) // per_page)
    return jsonify({"status": 200, "data": rows, "page": page, "per_page": per_page, "pages": pages, "total": total})


def delete_driver_cost(order_cost_id: int) -> int:
    sql = "DELETE FROM order_cost WHERE order_cost_id = %s"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (order_cost_id,))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/driver-cost/delete")
def api_driver_cost_delete():
    payload = request.get_json(silent=True) or {}
    order_cost_id = payload.get("order_cost_id")
    if not isinstance(order_cost_id, int):
        return jsonify({"status": 400, "message": "order_cost_id invalid"}), 400
    affected = delete_driver_cost(order_cost_id)
    return jsonify({"status": 200, "affected": affected})


# ---------- Menu 10: Import Lokasi ----------


def import_lokasi_dir() -> Path:
    base = Path(__file__).resolve().parent
    d = base / "import_lokasi"
    d.mkdir(parents=True, exist_ok=True)
    return d


@app.get("/menu/import-lokasi")
def menu_import_lokasi():
    return render_template("import_lokasi.html")


@app.post("/api/import-lokasi")
def api_import_lokasi():
    try:
        f: Optional[FileStorage] = request.files.get("file")  # type: ignore
        env = str(request.form.get("env", "preprod")).strip().lower()
        
        if f is None or f.filename == "":
            return jsonify({"status": 400, "message": "File .xlsx wajib diunggah."}), 400
        
        if env not in ["preprod", "prod"]:
            return jsonify({"status": 400, "message": "Environment harus preprod atau prod."}), 400
        
        # Simpan file ke folder import_lokasi
        import_dir = import_lokasi_dir()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        original_filename = f.filename or "import.xlsx"
        filename_without_ext = Path(original_filename).stem
        saved_filename = f"{filename_without_ext}_{timestamp}.xlsx"
        saved_path = import_dir / saved_filename
        
        try:
            f.save(str(saved_path))
        except Exception as exc:
            return jsonify({"status": 500, "message": f"Gagal menyimpan file: {exc}"}), 500
        
        # Jalankan import
        try:
            success, messages, result_data = import_lokasi.import_location_from_excel(str(saved_path), env)
            
            # Simpan result_data ke file JSON untuk download log nanti
            import json as json_module
            log_data_filename = f"log_{filename_without_ext}_{timestamp}.json"
            log_data_path = import_dir / log_data_filename
            with open(log_data_path, 'w', encoding='utf-8') as f:
                json_module.dump(result_data, f, ensure_ascii=False, indent=2)
            
            return jsonify({
                "status": 200 if success else 500,
                "message": "Import selesai" if success else "Import gagal",
                "success": success,
                "messages": messages,
                "filename": saved_filename,
                "log_data_filename": log_data_filename
            })
        except Exception as exc:
            return jsonify({
                "status": 500,
                "message": f"Gagal proses import: {exc}",
                "success": False,
                "messages": [f"Error: {str(exc)}"],
                "filename": saved_filename
            })
    except Exception as exc:
        return jsonify({"status": 500, "message": f"Kesalahan tak terduga: {exc}"}), 500


@app.get("/api/import-lokasi/download-log")
def api_import_lokasi_download_log():
    """Download log hasil import dalam format Excel dengan 2 sheet (parent dan child)"""
    try:
        log_data_filename = request.args.get("filename")
        if not log_data_filename:
            return jsonify({"status": 400, "message": "Parameter filename diperlukan"}), 400
        
        import_dir = import_lokasi_dir()
        log_data_path = import_dir / log_data_filename
        
        if not log_data_path.exists():
            return jsonify({"status": 404, "message": "File log tidak ditemukan"}), 404
        
        # Baca JSON data
        import json as json_module
        with open(log_data_path, 'r', encoding='utf-8') as f:
            result_data = json_module.load(f)
        
        # Buat Excel dengan 2 sheet menggunakan pandas
        import pandas as pd
        from io import BytesIO
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet Parent
            parent_df = pd.DataFrame(result_data.get("parent_results", []))
            if not parent_df.empty:
                # Reorder columns: row_num, code, name, status, remark
                parent_df = parent_df[["row_num", "code", "name", "status", "remark"]]
            else:
                parent_df = pd.DataFrame(columns=["row_num", "code", "name", "status", "remark"])
            parent_df.to_excel(writer, sheet_name="Parent", index=False)
            
            # Sheet Child
            child_df = pd.DataFrame(result_data.get("child_results", []))
            if not child_df.empty:
                # Reorder columns: row_num, code, name, status, remark
                child_df = child_df[["row_num", "code", "name", "status", "remark"]]
            else:
                child_df = pd.DataFrame(columns=["row_num", "code", "name", "status", "remark"])
            child_df.to_excel(writer, sheet_name="Child", index=False)
        
        output.seek(0)
        
        # Generate filename untuk download
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_filename = f"log_import_lokasi_{timestamp}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=download_filename
        )
        
    except Exception as exc:
        return jsonify({"status": 500, "message": f"Gagal generate log: {exc}"}), 500


# ---------- Menu 11: Check Order Status ----------


@app.get("/menu/check-order-status")
def menu_check_order_status():
    return render_template("check_order_status.html")


def find_orders_by_faktur_id(faktur_id: str) -> List[Dict[str, Any]]:
    """Cari semua order berdasarkan faktur_id dengan urutan kolom yang spesifik."""
    sql = '''SELECT 
        order_id,
        faktur_id,
        faktur_date,
        delivery_date,
        do_number,
        status,
        skip_count,
        created_date,
        created_by,
        updated_date,
        updated_by,
        notes,
        customer_id,
        warehouse_id,
        delivery_type_id,
        order_integration_id,
        origin_name,
        origin_address_1,
        origin_address_2,
        origin_city,
        origin_zipcode,
        origin_phone,
        origin_email,
        destination_name,
        destination_address_1,
        destination_address_2,
        destination_city,
        destination_zip_code,
        destination_phone,
        destination_email,
        client_id,
        cancel_reason,
        rdo_integration_id,
        address_change,
        divisi,
        pre_status,
        atena_sorting_code
    FROM "order" 
    WHERE faktur_id = %s 
    ORDER BY order_id DESC'''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (faktur_id,))
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]


@app.get("/api/check-order-status/orders")
def api_check_order_status_orders():
    faktur_id = request.args.get("faktur_id", "").strip()
    if not faktur_id:
        return jsonify({"status": 400, "message": "faktur_id wajib diisi"}), 400
    
    try:
        rows = find_orders_by_faktur_id(faktur_id)
        return jsonify({"status": 200, "data": rows})
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


def find_order_details_by_order_id(order_id: int) -> List[Dict[str, Any]]:
    """Cari semua order_detail berdasarkan order_id dengan urutan kolom yang spesifik."""
    sql = '''SELECT 
        order_detail_id,
        quantity_faktur,
        net_price,
        quantity_wms,
        quantity_delivery,
        quantity_loading,
        quantity_unloading,
        status,
        cancel_reason,
        notes,
        order_id,
        product_id,
        unit_id,
        pack_id,
        line_id,
        unloading_latitude,
        unloading_longitude,
        origin_uom,
        origin_qty,
        total_ctn,
        total_pcs
    FROM order_detail 
    WHERE order_id = %s 
    ORDER BY order_detail_id'''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (order_id,))
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]


@app.get("/api/check-order-status/order-details")
def api_check_order_status_order_details():
    try:
        order_id = int(request.args.get("order_id", "0"))
    except (ValueError, TypeError):
        return jsonify({"status": 400, "message": "order_id invalid"}), 400
    
    if not order_id:
        return jsonify({"status": 400, "message": "order_id wajib diisi"}), 400
    
    try:
        rows = find_order_details_by_order_id(order_id)
        return jsonify({"status": 200, "data": rows})
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


def find_product_vs_inventory_by_faktur_id(faktur_id: str) -> List[Dict[str, Any]]:
    """Cari data Product VS Inventory berdasarkan faktur_id."""
    sql = '''SELECT
        mp.sku,
        od.product_id,
        mp.mst_product_id AS mp_product,
        od.quantity_faktur AS faktur_qty,
        mp.available_qty AS avail_qty,
        CASE
            WHEN mp.available_qty > od.quantity_faktur THEN 'Full Fill'
            ELSE 'Not Full Fill'
        END AS check_status
    FROM order_detail od
    LEFT JOIN "order" o ON o.order_id = od.order_id
    LEFT JOIN mst_product mp ON mp.mst_product_id = od.product_id
    WHERE o.faktur_id = %s
    ORDER BY od.product_id'''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (faktur_id,))
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]


@app.get("/api/check-order-status/product-vs-inventory")
def api_check_order_status_product_vs_inventory():
    faktur_id = request.args.get("faktur_id", "").strip()
    if not faktur_id:
        return jsonify({"status": 400, "message": "faktur_id wajib diisi"}), 400
    
    try:
        rows = find_product_vs_inventory_by_faktur_id(faktur_id)
        return jsonify({"status": 200, "data": rows})
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


# ---------- Menu 12: Ubah Order Status ----------


@app.get("/menu/ubah-order-status")
def menu_ubah_order_status():
    return render_template("ubah_order_status.html")


def find_orders_by_order_number(env: str, order_number: str) -> List[Dict[str, Any]]:
    """
    Cari semua order yang match order_number (do_number atau faktur_id) di environment tertentu.
    Returns list of dicts; bisa kosong jika tidak ada yang match.
    """
    sql = '''SELECT 
        order_id,
        faktur_id,
        faktur_date,
        delivery_date,
        do_number,
        status,
        skip_count,
        created_date,
        created_by,
        updated_date,
        updated_by,
        notes,
        customer_id,
        warehouse_id,
        delivery_type_id,
        order_integration_id,
        origin_name,
        origin_address_1,
        origin_address_2,
        origin_city,
        origin_zipcode,
        origin_phone,
        origin_email,
        destination_name,
        destination_address_1,
        destination_address_2,
        destination_city,
        destination_zip_code,
        destination_phone,
        destination_email,
        client_id,
        cancel_reason,
        rdo_integration_id,
        address_change,
        divisi,
        pre_status,
        atena_sorting_code
    FROM "order" 
    WHERE do_number = %s OR faktur_id = %s
    ORDER BY order_id DESC'''
    
    try:
        with get_db_connection_by_env(env) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (order_number, order_number))
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        logger.error(f"Error finding orders: {str(e)}")
        raise


@app.get("/api/ubah-order-status/search")
def api_ubah_order_status_search():
    env = request.args.get("env", "").strip().lower()
    order_number = request.args.get("order_number", "").strip()
    
    if not env:
        return jsonify({"status": 400, "message": "Environment wajib dipilih"}), 400
    if env not in ["preprod", "prod"]:
        return jsonify({"status": 400, "message": "Environment harus preprod atau prod"}), 400
    if not order_number:
        return jsonify({"status": 400, "message": "Order Number wajib diisi"}), 400
    
    try:
        orders = find_orders_by_order_number(env, order_number)
        if not orders:
            return jsonify({"status": 404, "message": f"Order Number {order_number} tidak ditemukan"}), 404
        # Selalu kirim array agar frontend menampilkan semua baris (bisa > 1 untuk 1 order number)
        return jsonify({"status": 200, "data": list(orders), "count": len(orders)})
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


def update_order_data(env: str, order_id: int, username: str = "system", 
                      status: str = None, order_integration_id: str = None, delivery_date: str = None) -> int:
    """
    Update data order di environment tertentu.
    Hanya field yang tidak None yang akan diupdate.
    Returns jumlah baris yang terpengaruh.
    """
    valid_statuses = ['new', 'loading', 'ready_to_deliver', 'in_delivery', 'completed', 
                     'skip', 'rejected', 'hold', 'failed', 'return_to_wms', 'inactive', 'in_optimization']
    
    # Build dynamic SET clause
    set_parts = []
    params = []
    
    if status is not None:
        if status not in valid_statuses:
            raise ValueError(f"Status tidak valid. Harus salah satu dari: {', '.join(valid_statuses)}")
        set_parts.append("status = %s")
        params.append(status)
    
    if order_integration_id is not None:
        set_parts.append("order_integration_id = %s")
        params.append(order_integration_id)
    
    if delivery_date is not None:
        set_parts.append("delivery_date = %s")
        params.append(delivery_date)
    
    if not set_parts:
        raise ValueError("Minimal satu field harus diisi untuk melakukan perubahan")
    
    # Always update updated_date and updated_by
    set_parts.append("updated_date = CURRENT_TIMESTAMP")
    set_parts.append("updated_by = %s")
    params.append(username)
    
    # Add order_id to params
    params.append(order_id)
    
    sql = f'''UPDATE "order" 
             SET {", ".join(set_parts)}
             WHERE order_id = %s'''
    
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/ubah-order-data/update")
def api_ubah_order_data_update():
    payload = request.get_json(silent=True) or {}
    env = str(payload.get("env", "")).strip().lower()
    order_id = payload.get("order_id")
    username = session.get("username", "system")
    
    # Optional fields
    new_status = payload.get("status")
    if new_status is not None:
        new_status = str(new_status).strip() or None
    
    new_order_integration_id = payload.get("order_integration_id")
    if new_order_integration_id is not None:
        new_order_integration_id = str(new_order_integration_id).strip() or None
    
    new_delivery_date = payload.get("delivery_date")
    if new_delivery_date is not None:
        new_delivery_date = str(new_delivery_date).strip() or None
    
    if not env:
        return jsonify({"status": 400, "message": "Environment wajib dipilih"}), 400
    if env not in ["preprod", "prod"]:
        return jsonify({"status": 400, "message": "Environment harus preprod atau prod"}), 400
    if not isinstance(order_id, int):
        return jsonify({"status": 400, "message": "order_id invalid"}), 400
    
    # Check if at least one field is provided
    if new_status is None and new_order_integration_id is None and new_delivery_date is None:
        return jsonify({"status": 400, "message": "Minimal satu field harus diisi untuk melakukan perubahan"}), 400
    
    try:
        affected = update_order_data(
            env=env, 
            order_id=order_id, 
            username=username,
            status=new_status,
            order_integration_id=new_order_integration_id,
            delivery_date=new_delivery_date
        )
        if affected == 0:
            return jsonify({"status": 404, "message": "Order tidak ditemukan"}), 404
        
        # Build success message
        changed_fields = []
        if new_status:
            changed_fields.append("Status")
        if new_order_integration_id:
            changed_fields.append("Order Integration ID")
        if new_delivery_date:
            changed_fields.append("Delivery Date")
        
        return jsonify({
            "status": 200, 
            "message": f"Data berhasil diubah ({', '.join(changed_fields)})", 
            "affected": affected
        })
    except ValueError as e:
        return jsonify({"status": 400, "message": str(e)}), 400
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


# ---------- Menu 13: Export Data to CSV ----------


def data_archive_order_dir() -> Path:
    base = Path(__file__).resolve().parent
    d = base / "data_archive_order"
    d.mkdir(parents=True, exist_ok=True)
    return d


@app.get("/menu/export-data-csv")
def menu_export_data_csv():
    return render_template("export_data_csv.html")


def export_data_product(env: str) -> List[Dict[str, Any]]:
    """Export data dari mst_product dengan kolom berbeda untuk preprod dan prod"""
    env_lower = env.lower()
    
    if env_lower == "preprod":
        # Query untuk Preprod dengan JOIN mst_product_stock
        sql = '''SELECT 
            mp.mst_product_id,
            mp.sku,
            mp.height,
            mp.width,
            mp.length,
            mp.name,
            mp.price,
            mp.type_product_id,
            mp.qty,
            mp.volume,
            mp.weight,
            mp.base_uom,
            mp.pack_id,
            mp.warehouse_id,
            mp.synced_at,
            mp.allocated_qty,
            mp.available_qty,
            mps.expired_date
        FROM mst_product mp
        LEFT JOIN mst_product_stock mps ON mps.product_id = mp.mst_product_id
        ORDER BY mp.mst_product_id'''
    else:
        # Kolom untuk Production (default - sama seperti sebelumnya)
        sql = '''SELECT 
            mst_product_id,
            sku,
            height,
            width,
            length,
            name,
            price,
            type_product_id,
            qty,
            volume,
            weight,
            base_uom,
            pack_id,
            warehouse_id,
            synced_at,
            allocated_qty,
            available_qty,
            expired_date,
            batch
        FROM mst_product
        ORDER BY mst_product_id'''
    
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def export_data_vehicle(env: str) -> List[Dict[str, Any]]:
    """Export data dari mst_vehicle"""
    sql = '''SELECT 
        mst_vehicle_id,
        plate_number,
        tax_expired,
        stnk_number,
        fuel_efficiency_km,
        opening_time,
        closing_time,
        status,
        created_date,
        created_by,
        updated_by,
        type,
        kir_expired,
        product_restriction_id,
        region_restriction_id,
        customer_type_restriction_id,
        specific_customer_restriction_id,
        pickup_location,
        max_trip_duration,
        updated_date,
        driver_id,
        co_driver_id,
        zona_restriction_id,
        code,
        route4me_vehicle_id
    FROM mst_vehicle
    ORDER BY mst_vehicle_id'''
    
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def export_data_lov_config(env: str) -> List[Dict[str, Any]]:
    """Export data dari mst_list_of_values"""
    sql = '''SELECT 
        lov_id,
        code,
        value,
        status,
        lov_parent_id
    FROM mst_list_of_values
    ORDER BY lov_id'''
    
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def export_data_master_location(env: str) -> List[Dict[str, Any]]:
    """Export data dari mst_location_parent"""
    sql = '''SELECT * FROM mst_location_parent ORDER BY mst_location_parent_id'''
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def export_data_child_location(env: str) -> List[Dict[str, Any]]:
    """Export data dari mst_location_child"""
    sql = '''SELECT * FROM mst_location_child ORDER BY mst_location_child_id'''
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.post("/api/export-data-csv/generate")
def api_export_data_csv_generate():
    payload = request.get_json(silent=True) or {}
    env = str(payload.get("env", "")).strip().lower()
    data_type = str(payload.get("data_type", "")).strip().lower()
    
    if not env:
        return jsonify({"status": 400, "message": "Environment wajib dipilih"}), 400
    if env not in ["preprod", "prod"]:
        return jsonify({"status": 400, "message": "Environment harus preprod atau prod"}), 400
    
    # Validasi environment config
    try:
        get_db_config_by_env(env)
    except (RuntimeError, ValueError) as e:
        return jsonify({"status": 400, "message": f"Konfigurasi environment {env.upper()} tidak valid: {str(e)}"}), 400
    
    if not data_type:
        return jsonify({"status": 400, "message": "Tipe data wajib dipilih"}), 400
    
    valid_data_types = {
        "dataproduct": ("Data Product", export_data_product),
        "datavehicle": ("Data Vehicle", export_data_vehicle),
        "lovconfig": ("Lov Config", export_data_lov_config),
        "masterlocation": ("Master Location", export_data_master_location),
        "childlocation": ("Child Location", export_data_child_location),
    }
    
    if data_type not in valid_data_types:
        return jsonify({"status": 400, "message": f"Tipe data tidak valid. Harus salah satu dari: {', '.join(valid_data_types.keys())}"}), 400
    
    try:
        # Get data
        data_label, export_func = valid_data_types[data_type]
        data = export_func(env)
        
        if not data:
            return jsonify({"status": 404, "message": "Tidak ada data ditemukan"}), 404
        
        # Generate CSV using pandas
        import pandas as pd
        
        df = pd.DataFrame(data)
        
        # Generate filename dengan format: data_dataproduct_020120251300.csv
        timestamp = datetime.now().strftime("%d%m%Y%H%M")
        filename = f"data_{data_type}_{timestamp}.csv"
        
        # Save to folder
        archive_dir = data_archive_order_dir()
        filepath = archive_dir / filename
        
        # Write CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        return jsonify({
            "status": 200,
            "message": f"Data berhasil di-export",
            "filename": filename,
            "row_count": len(data)
        })
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


@app.get("/api/export-data-csv/download")
def api_export_data_csv_download():
    """Download CSV file"""
    try:
        filename = request.args.get("filename")
        if not filename:
            return jsonify({"status": 400, "message": "Parameter filename diperlukan"}), 400
        
        archive_dir = data_archive_order_dir()
        filepath = archive_dir / filename
        
        if not filepath.exists():
            return jsonify({"status": 404, "message": "File tidak ditemukan"}), 404
        
        return send_file(
            str(filepath),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


# ---------- Menu 14: Update Order on Route ----------


@app.get("/menu/update-order-on-route")
def menu_update_order_on_route():
    return render_template("update_order_on_route.html")


def find_orders_by_manifest_reference(env: str, manifest_reference: str) -> List[Dict[str, Any]]:
    """
    Cari semua order yang ada di manifest_reference (join order, route_detail, route).
    Returns list of dicts dengan kolom: order_id, faktur_id, order_status, route_status,
    manifest_reference, manifest_integration_id.
    """
    sql = """SELECT
        od.order_id,
        od.faktur_id,
        od.status AS order_status,
        ro.status AS route_status,
        ro.manifest_reference,
        ro.manifest_integration_id
    FROM "order" od
    LEFT JOIN route_detail rd ON rd.order_id = od.order_id
    LEFT JOIN route ro ON ro.route_id = rd.route_id
    WHERE ro.manifest_reference = %s
    ORDER BY od.order_id"""
    try:
        with get_db_connection_by_env(env) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (manifest_reference,))
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.error(f"Error finding orders by manifest_reference: {str(e)}")
        raise


@app.get("/api/update-order-on-route/search")
def api_update_order_on_route_search():
    env = request.args.get("env", "").strip().lower()
    manifest_reference = request.args.get("manifest_reference", "").strip()
    if not env:
        return jsonify({"status": 400, "message": "Environment wajib dipilih"}), 400
    if env not in ["preprod", "prod"]:
        return jsonify({"status": 400, "message": "Environment harus preprod atau prod"}), 400
    if not manifest_reference:
        return jsonify({"status": 400, "message": "Manifest Reference wajib diisi"}), 400
    try:
        rows = find_orders_by_manifest_reference(env, manifest_reference)
        if not rows:
            return jsonify({"status": 404, "message": f"Manifest Reference '{manifest_reference}' tidak ditemukan"}), 404
        return jsonify({"status": 200, "data": list(rows), "count": len(rows)})
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


def update_route_manifest(
    env: str,
    manifest_reference: str,
    status: str = None,
    manifest_integration_id: str = None,
) -> int:
    """
    Update route: status dan/atau manifest_integration_id untuk semua route yang match manifest_reference.
    Hanya field yang tidak None yang di-SET.
    Returns jumlah baris route yang terpengaruh.
    """
    valid_statuses = [
        "new", "loading", "ready_to_deliver", "in_delivery",
        "delivery_success", "delivery_completed", "rejected"
    ]
    if status is not None and status not in valid_statuses:
        raise ValueError(f"Status tidak valid. Harus salah satu dari: {', '.join(valid_statuses)}")
    set_parts = []
    params = []
    if status is not None:
        set_parts.append("ro.status = %s")
        params.append(status)
    if manifest_integration_id is not None:
        set_parts.append("ro.manifest_integration_id = %s")
        params.append(manifest_integration_id)
    if not set_parts:
        raise ValueError("Minimal satu field (status atau manifest_integration_id) harus diisi")
    params.append(manifest_reference)
    sql = f"""UPDATE route ro
    SET {", ".join(set_parts)}
    FROM route_detail rd
    JOIN "order" od ON od.order_id = rd.order_id
    WHERE ro.route_id = rd.route_id
    AND ro.manifest_reference = %s"""
    with get_db_connection_by_env(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            affected = cur.rowcount
        conn.commit()
        return affected


@app.post("/api/update-order-on-route/update")
def api_update_order_on_route_update():
    payload = request.get_json(silent=True) or {}
    env = str(payload.get("env", "")).strip().lower()
    manifest_reference = str(payload.get("manifest_reference", "")).strip()
    status = payload.get("status")
    if status is not None:
        status = str(status).strip() or None
    manifest_integration_id = payload.get("manifest_integration_id")
    if manifest_integration_id is not None:
        manifest_integration_id = str(manifest_integration_id).strip() or None
    if not env:
        return jsonify({"status": 400, "message": "Environment wajib dipilih"}), 400
    if env not in ["preprod", "prod"]:
        return jsonify({"status": 400, "message": "Environment harus preprod atau prod"}), 400
    if not manifest_reference:
        return jsonify({"status": 400, "message": "Manifest Reference wajib diisi"}), 400
    if status is None and manifest_integration_id is None:
        return jsonify({"status": 400, "message": "Minimal satu field (status atau manifest_integration_id) harus diisi"}), 400
    try:
        affected = update_route_manifest(
            env=env,
            manifest_reference=manifest_reference,
            status=status,
            manifest_integration_id=manifest_integration_id,
        )
        changed = []
        if status is not None:
            changed.append("Status Manifest")
        if manifest_integration_id is not None:
            changed.append("Integration ID")
        return jsonify({
            "status": 200,
            "message": f"Data manifest berhasil diubah ({', '.join(changed)}). {affected} route terpengaruh.",
            "affected": affected,
        })
    except ValueError as e:
        return jsonify({"status": 400, "message": str(e)}), 400
    except Exception as e:
        return jsonify({"status": 500, "message": f"Error: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)


