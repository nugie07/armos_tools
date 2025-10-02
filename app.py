import os
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template, request, url_for, session
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
from sync.manager import run_sync as sync_run, get_sync_status as sync_get_status, create_sync_log_table
from sync.db import DatabaseManager


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

    results = []
    for row in data:
        if not isinstance(row, dict):
            continue
        if _match(row.get("event"), q_event) and _match(row.get("request"), q_request):
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


@app.get("/menu/sync-dashboard")
def menu_sync_dashboard():
    return render_template("sync_dashboard.html")


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
    rows = sync_get_status(DatabaseManager(), sync_type=sync_type, limit=limit)
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
    total = len(rows)
    success = len([1 for r in rows if r[3] == "SUCCESS"])
    failed = len([1 for r in rows if r[3] == "FAILED"])
    last_sync = data[0]["start_time"] if data else None
    return jsonify({"status": 200, "stats": {"total_syncs": total, "successful_syncs": success, "failed_syncs": failed, "last_sync": last_sync}, "sync_history": data})


@app.get("/api/sync/job/<job_id>")
def api_sync_job(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": 404, "message": "job not found"}), 404
    return jsonify({"status": 200, "job": job})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)


