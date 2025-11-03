"""
Script untuk import lokasi dari file Excel ke database.
Mendukung import ke mst_location_parent dan mst_location_child.
"""
import os
import pandas as pd
import psycopg2
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re


def get_db_config(env: str = "preprod") -> Dict[str, Any]:
    """
    Ambil konfigurasi database berdasarkan environment.
    env: 'preprod' atau 'prod'
    
    Raises:
        RuntimeError: Jika konfigurasi database tidak lengkap
    """
    env_lower = env.lower()
    if env_lower not in ["preprod", "prod"]:
        raise ValueError(f"Environment harus 'preprod' atau 'prod', mendapat: {env}")
    
    prefix = "DATABASE_PREPROD" if env_lower == "preprod" else "DATABASE_PROD"
    
    def _env(name: str, fallback: Optional[str] = None, default: Optional[str] = None) -> Optional[str]:
        v = os.getenv(name)
        if v is None and fallback is not None:
            v = os.getenv(fallback)
        if v is None:
            v = default
        return v
    
    # Ambil konfigurasi dengan prefix khusus untuk env ini
    config = {
        "host": _env(f"{prefix}_HOST"),
        "port": _env(f"{prefix}_PORT", default="5432"),
        "dbname": _env(f"{prefix}_NAME"),
        "user": _env(f"{prefix}_USERNAME"),
        "password": _env(f"{prefix}_PASS"),
    }
    
    # Validasi konfigurasi lengkap
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
            f"Variabel environment yang diperlukan: {', '.join(missing)}. "
            f"Pastikan variabel tersebut sudah di-set di file .env"
        )
    
    # Convert port ke int
    try:
        config["port"] = int(config["port"] or "5432")
    except (ValueError, TypeError):
        config["port"] = 5432
    
    return config


def get_db_connection(env: str = "preprod"):
    """
    Buat koneksi database berdasarkan environment.
    
    Args:
        env: 'preprod' atau 'prod'
    
    Returns:
        psycopg2.connection: Koneksi database
    
    Raises:
        RuntimeError: Jika konfigurasi database tidak lengkap
        psycopg2.Error: Jika gagal koneksi ke database
    """
    config = get_db_config(env)
    
    try:
        return psycopg2.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
        )
    except psycopg2.Error as e:
        # Tambahkan informasi konfigurasi yang digunakan (tanpa password)
        config_safe = {k: v for k, v in config.items() if k != "password"}
        config_safe["password"] = "***"
        raise RuntimeError(
            f"Gagal koneksi ke database {env.upper()}: {str(e)}\n"
            f"Konfigurasi yang digunakan: {config_safe}"
        ) from e


def lookup_lov_id(conn: psycopg2.extensions.connection, value: str) -> Optional[int]:
    """
    Cari lov_id dari mst_list_of_values berdasarkan value.
    Returns None jika tidak ditemukan.
    """
    if not value or pd.isna(value):
        return None
    
    value_str = str(value).strip()
    if not value_str:
        return None
    
    with conn.cursor() as cur:
        sql = "SELECT lov_id FROM mst_list_of_values WHERE value = %s LIMIT 1"
        cur.execute(sql, (value_str,))
        row = cur.fetchone()
        if row:
            return int(row[0])
    return None


def check_parent_exists(conn: psycopg2.extensions.connection, code: str) -> bool:
    """
    Cek apakah parent dengan code tertentu sudah ada.
    """
    if not code or pd.isna(code):
        return False
    
    code_str = str(code).strip()
    if not code_str:
        return False
    
    with conn.cursor() as cur:
        sql = "SELECT COUNT(*) FROM mst_location_parent WHERE code = %s"
        cur.execute(sql, (code_str,))
        row = cur.fetchone()
        return row[0] > 0 if row else False


def check_child_exists(conn: psycopg2.extensions.connection, code: str) -> bool:
    """
    Cek apakah child dengan code tertentu sudah ada.
    """
    if not code or pd.isna(code):
        return False
    
    code_str = str(code).strip()
    if not code_str:
        return False
    
    with conn.cursor() as cur:
        sql = "SELECT COUNT(*) FROM mst_location_child WHERE code = %s"
        cur.execute(sql, (code_str,))
        row = cur.fetchone()
        return row[0] > 0 if row else False


def get_parent_id_by_code(conn: psycopg2.extensions.connection, code: str) -> Optional[int]:
    """
    Cari mst_location_parent_id berdasarkan code.
    Query sesuai requirement: SELECT mlp.mst_location_parent_id, mlp.code FROM mst_location_parent mlp
           LEFT JOIN mst_location_child mlc ON mlc.code = mlp.code 
           WHERE mlc.code = %s
    Jika tidak ditemukan di join, cari langsung di parent dengan code yang sama.
    """
    if not code or pd.isna(code):
        return None
    
    code_str = str(code).strip()
    if not code_str:
        return None
    
    with conn.cursor() as cur:
        # Query sesuai requirement: cari parent yang punya child dengan code ini
        sql = """
            SELECT mlp.mst_location_parent_id 
            FROM mst_location_parent mlp
            LEFT JOIN mst_location_child mlc ON mlc.code = mlp.code 
            WHERE mlc.code = %s
            LIMIT 1
        """
        cur.execute(sql, (code_str,))
        row = cur.fetchone()
        if row:
            return int(row[0])
        
        # Jika tidak ditemukan di join (tidak ada child dengan code ini), 
        # cari parent yang punya code sama dengan code di excel
        sql = "SELECT mst_location_parent_id FROM mst_location_parent WHERE code = %s LIMIT 1"
        cur.execute(sql, (code_str,))
        row = cur.fetchone()
        if row:
            return int(row[0])
    
    return None


def format_available_drop_days(days_str: str) -> Optional[str]:
    """
    Format available_drop_days ke format {senin,selasa,...}
    """
    if not days_str or pd.isna(days_str):
        return None
    
    days_str = str(days_str).strip()
    if not days_str:
        return None
    
    # Jika sudah dalam format {senin,selasa,...}, return as is
    if days_str.startswith("{") and days_str.endswith("}"):
        return days_str
    
    # Split by comma atau space, kemudian format
    days = [d.strip().lower() for d in re.split(r'[,;\s]+', days_str) if d.strip()]
    if not days:
        return None
    
    return "{" + ",".join(days) + "}"


def format_time_to_hhmmss(time_str: Any) -> Optional[str]:
    """
    Format waktu ke format HH:MM:SS.
    Mendukung berbagai format input.
    """
    if not time_str or pd.isna(time_str):
        return None
    
    time_str = str(time_str).strip()
    if not time_str:
        return None
    
    # Jika sudah dalam format HH:MM:SS, return as is
    if re.match(r'^\d{2}:\d{2}:\d{2}$', time_str):
        return time_str
    
    # Coba parse berbagai format
    formats = [
        '%H:%M:%S',
        '%H:%M',
        '%I:%M:%S %p',
        '%I:%M %p',
        '%H.%M.%S',
        '%H.%M',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.strftime('%H:%M:%S')
        except ValueError:
            continue
    
    # Jika gagal parse, coba extract angka
    numbers = re.findall(r'\d+', time_str)
    if len(numbers) >= 2:
        h = numbers[0].zfill(2)
        m = numbers[1].zfill(2)
        s = numbers[2].zfill(2) if len(numbers) >= 3 else "00"
        return f"{h}:{m}:{s}"
    
    return None


def safe_get(df: pd.DataFrame, row_idx: int, col_name: str, default: Any = None) -> Any:
    """Safe get value dari DataFrame dengan handling NaN."""
    try:
        val = df.at[row_idx, col_name]
        if pd.isna(val):
            return default
        return val
    except (KeyError, IndexError):
        return default


def import_location_from_excel(file_path: str, env: str = "preprod") -> Tuple[bool, List[str], Dict[str, Any]]:
    """
    Import lokasi dari file Excel.
    
    Returns:
        (success: bool, messages: List[str], result_data: Dict[str, Any])
        result_data berisi:
        - parent_results: List[Dict] dengan keys: row_num, code, name, status, remark
        - child_results: List[Dict] dengan keys: row_num, code, name, status, remark
    """
    messages = []
    parent_results = []
    child_results = []
    
    try:
        # Baca Excel
        df = pd.read_excel(file_path)
        messages.append(f"File berhasil dibaca: {len(df)} baris")
        
        # Validasi kolom minimal
        required_cols = ["is_parent", "code", "name"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, [f"Kolom wajib tidak ditemukan: {', '.join(missing_cols)}"]
        
        # Koneksi database dengan validasi
        env_lower = env.lower()
        prefix = "DATABASE_PREPROD" if env_lower == "preprod" else "DATABASE_PROD"
        try:
            # Ambil konfigurasi database untuk ditampilkan
            db_config = get_db_config(env)
            conn = get_db_connection(env)
            
            # Tampilkan informasi database yang akan digunakan
            messages.append("\n" + "="*60)
            messages.append(f"INFORMASI DATABASE YANG AKAN DIGUNAKAN:")
            messages.append(f"  Environment: {env.upper()}")
            messages.append(f"  Host: {db_config['host']}")
            messages.append(f"  Port: {db_config['port']}")
            messages.append(f"  Database: {db_config['dbname']}")
            messages.append(f"  User: {db_config['user']}")
            messages.append("="*60)
            messages.append("")
        except RuntimeError as e:
            return False, [
                f"Error konfigurasi database untuk {env.upper()}:",
                str(e),
                f"\nPastikan variabel environment berikut sudah di-set di file .env:",
                f"- {prefix}_HOST",
                f"- {prefix}_PORT (opsional, default: 5432)",
                f"- {prefix}_NAME",
                f"- {prefix}_USERNAME",
                f"- {prefix}_PASS"
            ]
        
        created_date = datetime.now().date()
        # Set created_by dengan UUID
        created_by = "d6208eb3-bd20-4550-8fa3-6c3277763916"
        
        success_count = 0
        error_count = 0
        
        # Pisahkan baris menjadi parent dan child
        parent_rows = []
        child_rows = []
        
        for idx in df.index:
            is_parent_val = safe_get(df, idx, "is_parent", "")
            is_parent = str(is_parent_val).strip().upper() == "Y"
            code = safe_get(df, idx, "code", "")
            name = safe_get(df, idx, "name", "")
            
            if not code or not name:
                error_count += 1
                messages.append(f"Baris {idx + 1}: code atau name kosong, dilewati")
                continue
            
            if is_parent:
                parent_rows.append(idx)
            else:
                child_rows.append(idx)
        
        messages.append(f"Baris Parent (IS_PARENT=Y): {len(parent_rows)} baris")
        messages.append(f"Baris Child (IS_PARENT!=Y): {len(child_rows)} baris")
        
        # STEP 1: Insert semua parent terlebih dahulu
        messages.append("\n" + "="*60)
        messages.append(f"=== STEP 1: Insert Parent (IS_PARENT=Y) ===")
        messages.append(f"  Environment: {env.upper()}")
        messages.append(f"  Host: {db_config['host']}")
        messages.append(f"  Database: {db_config['dbname']}")
        messages.append("="*60)
        parent_success = 0
        parent_error = 0
        parent_skipped = 0
        for idx in parent_rows:
            code = safe_get(df, idx, "code", "")
            name = safe_get(df, idx, "name", "")
            row_data = {
                "row_num": idx + 1,
                "code": str(code) if code else "",
                "name": str(name) if name else "",
                "status": "",
                "remark": ""
            }
            
            try:
                # Cek apakah parent dengan code ini sudah ada
                if check_parent_exists(conn, code):
                    parent_skipped += 1
                    row_data["status"] = "SKIP"
                    row_data["remark"] = f"Parent dengan code '{code}' sudah ada di database"
                    messages.append(f"Baris {idx + 1}: Skip - Parent dengan code '{code}' sudah ada di database")
                    parent_results.append(row_data)
                    continue
                
                # Insert ke mst_location_parent
                with conn.cursor() as cur:
                    sql = """
                        INSERT INTO mst_location_parent (code, name, created_by, created_date)
                        VALUES (%s, %s, %s, %s)
                    """
                    cur.execute(sql, (str(code), str(name), created_by, created_date))
                    parent_success += 1
                    row_data["status"] = "SUKSES"
                    row_data["remark"] = "Berhasil insert ke mst_location_parent"
                    messages.append(f"Baris {idx + 1}: Insert ke mst_location_parent: {code}")
                    parent_results.append(row_data)
            except Exception as e:
                parent_error += 1
                row_data["status"] = "GAGAL"
                row_data["remark"] = str(e)
                messages.append(f"Baris {idx + 1}: Error insert parent - {str(e)}")
                parent_results.append(row_data)
        
        # Commit parent terlebih dahulu agar child bisa menemukan parent_id
        conn.commit()
        success_count += parent_success
        error_count += parent_error
        messages.append(f"✓ Parent: Berhasil {parent_success} baris, Gagal {parent_error} baris, Skip {parent_skipped} baris (sudah ada)")
        
        # STEP 2: Insert semua child setelah parent sudah diinsert
        messages.append("\n" + "="*60)
        messages.append(f"=== STEP 2: Insert Child (IS_PARENT!=Y) ===")
        messages.append(f"  Environment: {env.upper()}")
        messages.append(f"  Host: {db_config['host']}")
        messages.append(f"  Database: {db_config['dbname']}")
        messages.append("="*60)
        child_success = 0
        child_error = 0
        child_skipped = 0
        for idx in child_rows:
            code = safe_get(df, idx, "code", "")
            name = safe_get(df, idx, "name", "")
            row_data = {
                "row_num": idx + 1,
                "code": str(code) if code else "",
                "name": str(name) if name else "",
                "status": "",
                "remark": ""
            }
            
            try:
                # Cek apakah child dengan code ini sudah ada
                if check_child_exists(conn, code):
                    child_skipped += 1
                    row_data["status"] = "SKIP"
                    row_data["remark"] = f"Child dengan code '{code}' sudah ada di database"
                    messages.append(f"Baris {idx + 1}: Skip - Child dengan code '{code}' sudah ada di database")
                    child_results.append(row_data)
                    continue
                
                # Insert ke mst_location_child
                tipe_child = safe_get(df, idx, "tipe_child", "")
                channel = safe_get(df, idx, "channel", "")
                availability = safe_get(df, idx, "availability")
                alamat = safe_get(df, idx, "alamat")
                longitude = safe_get(df, idx, "longitude")
                latitude = safe_get(df, idx, "latitude")
                unloading_duration = safe_get(df, idx, "unloading_duration")
                frequency_drop = safe_get(df, idx, "frequency_drop", "")
                available_drop_days = safe_get(df, idx, "available_drop_days", "")
                loading_dock = safe_get(df, idx, "loading_dock")
                priority = safe_get(df, idx, "priority", "")
                open_hour = safe_get(df, idx, "open_hour")
                closed_hour = safe_get(df, idx, "closed_hour")
                
                # Lookup LOV IDs
                location_type_id = lookup_lov_id(conn, tipe_child) if tipe_child else None
                channel_id = lookup_lov_id(conn, channel) if channel else None
                frequency_drop_id = lookup_lov_id(conn, frequency_drop) if frequency_drop else None
                priority_id = lookup_lov_id(conn, priority) if priority else None
                
                # Format available_drop_days
                available_drop_days_formatted = format_available_drop_days(available_drop_days)
                
                # Format waktu
                open_hour_formatted = format_time_to_hhmmss(open_hour)
                closed_hour_formatted = format_time_to_hhmmss(closed_hour)
                
                # Validasi longitude dan latitude (NOT NULL constraint)
                # Jika kosong, gunakan default 0.0
                longitude_val = float(longitude) if longitude and not pd.isna(longitude) else 0.0
                latitude_val = float(latitude) if latitude and not pd.isna(latitude) else 0.0
                
                # Cari parent_id berdasarkan code (sekarang parent sudah diinsert)
                parent_id = get_parent_id_by_code(conn, code)
                
                if not parent_id:
                    child_error += 1
                    row_data["status"] = "GAGAL"
                    row_data["remark"] = f"Parent dengan code '{code}' tidak ditemukan"
                    messages.append(f"Baris {idx + 1}: Error insert child - Parent dengan code '{code}' tidak ditemukan")
                    child_results.append(row_data)
                    continue
                
                # Insert ke mst_location_child
                # Handle alamat: jika kosong, gunakan empty string untuk memenuhi NOT NULL constraint
                alamat_str = str(alamat) if alamat and not pd.isna(alamat) else ""
                
                # Gunakan savepoint untuk setiap row agar error di satu row tidak menghentikan row berikutnya
                with conn.cursor() as cur:
                    # Buat savepoint untuk row ini
                    savepoint_name = f"sp_child_{idx}"
                    cur.execute(f"SAVEPOINT {savepoint_name}")
                    
                    try:
                        sql = """
                            INSERT INTO mst_location_child (
                                code, name, location_type_id, channel_id, availability,
                                address, address_text, longitude, latitude, unloading_duration,
                                frequency_drop_id, available_drop_days, loading_dock,
                                priority, open_hour, closed_hour, created_by, created_date, mst_location_parent_id
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        cur.execute(sql, (
                            str(code), str(name), location_type_id, channel_id, availability,
                            alamat_str,  # address (NOT NULL)
                            alamat_str if alamat_str else None,  # address_text (nullable)
                            longitude_val,  # longitude (NOT NULL, default 0.0 jika kosong)
                            latitude_val,  # latitude (NOT NULL, default 0.0 jika kosong)
                            int(unloading_duration) if unloading_duration and not pd.isna(unloading_duration) else None,
                            frequency_drop_id, available_drop_days_formatted,
                            str(loading_dock) if loading_dock else None,
                            priority_id, open_hour_formatted, closed_hour_formatted,
                            created_by, created_date, parent_id
                        ))
                        # Release savepoint jika sukses
                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        child_success += 1
                        row_data["status"] = "SUKSES"
                        row_data["remark"] = "Berhasil insert ke mst_location_child"
                        messages.append(f"Baris {idx + 1}: Insert ke mst_location_child: {code}")
                        child_results.append(row_data)
                    except Exception as e:
                        # Rollback ke savepoint untuk row ini saja, agar row lain bisa lanjut
                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        # Re-raise exception untuk ditangkap di except luar
                        raise
            except Exception as e:
                child_error += 1
                row_data["status"] = "GAGAL"
                row_data["remark"] = str(e)
                messages.append(f"Baris {idx + 1}: Error insert child - {str(e)}")
                child_results.append(row_data)
        
        conn.commit()
        conn.close()
        
        success_count += child_success
        error_count += child_error
        messages.append(f"\n✓ Child: Berhasil {child_success} baris, Gagal {child_error} baris, Skip {child_skipped} baris (sudah ada)")
        messages.append(f"\n=== RINGKASAN TOTAL ===")
        messages.append(f"Selesai: Total Berhasil {success_count} baris, Total Gagal {error_count} baris")
        
        result_data = {
            "parent_results": parent_results,
            "child_results": child_results
        }
        return True, messages, result_data
        
    except Exception as e:
        return False, [f"Error: {str(e)}"], {"parent_results": [], "child_results": []}


if __name__ == "__main__":
    # Test
    import sys
    if len(sys.argv) >= 2:
        file_path = sys.argv[1]
        env = sys.argv[2] if len(sys.argv) >= 3 else "preprod"
        success, messages, result_data = import_location_from_excel(file_path, env)
        print("\n".join(messages))
        sys.exit(0 if success else 1)
    else:
        print("Usage: python import_lokasi.py <file_path> [env=preprod|prod]")
        sys.exit(1)

