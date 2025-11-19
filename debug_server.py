#!/usr/bin/env python3
"""
Script untuk debugging Flask server di production
Jalankan script ini di server production untuk check status dan error
"""

import os
import sys
import socket
import subprocess
import psutil
from pathlib import Path

def check_port_listening(port: int, host: str = "0.0.0.0") -> bool:
    """Check apakah port sedang listening"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Error checking port: {e}")
        return False

def find_flask_processes():
    """Cari proses Flask yang sedang running"""
    flask_procs = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'status', 'create_time']):
        try:
            cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
            if 'app.py' in cmdline or 'flask' in cmdline.lower() or 'python' in cmdline.lower():
                flask_procs.append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'cmdline': cmdline,
                    'status': proc.info['status'],
                    'create_time': proc.info['create_time']
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return flask_procs

def check_env_vars():
    """Check environment variables yang diperlukan"""
    required_vars = [
        'DATABASE_MAIN_HOST', 'DATABASE_MAIN_PORT', 'DATABASE_MAIN_NAME',
        'DATABASE_MAIN_USERNAME', 'DATABASE_MAIN_PASS', 'WH_TYPE',
        'SECRET_KEY', 'SUPABASE_URL', 'SUPABASE_KEY'
    ]
    missing = []
    present = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Jangan print value yang sensitive
            if 'PASS' in var or 'KEY' in var or 'SECRET' in var:
                present.append(f"{var}=***")
            else:
                present.append(f"{var}={value}")
        else:
            missing.append(var)
    return present, missing

def test_db_connection():
    """Test koneksi database"""
    try:
        import psycopg2
        DB_HOST = os.getenv("DATABASE_MAIN_HOST") or os.getenv("DB_HOST")
        DB_PORT = int(os.getenv("DATABASE_MAIN_PORT") or os.getenv("DB_PORT") or "5432")
        DB_NAME = os.getenv("DATABASE_MAIN_NAME") or os.getenv("DB_NAME")
        DB_USER = os.getenv("DATABASE_MAIN_USERNAME") or os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DATABASE_MAIN_PASS") or os.getenv("DB_PASSWORD")
        
        if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
            return False, "Missing database environment variables"
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5
        )
        conn.close()
        return True, "Database connection OK"
    except Exception as e:
        return False, f"Database connection failed: {str(e)}"

def check_recent_logs():
    """Check log files terbaru"""
    log_files = []
    base_dir = Path(__file__).parent
    
    # Check common log locations
    log_dirs = [
        base_dir / "logs",
        base_dir / "data_log",
        base_dir,
        Path("/var/log"),
    ]
    
    for log_dir in log_dirs:
        if log_dir.exists() and log_dir.is_dir():
            for log_file in log_dir.glob("*.log"):
                try:
                    stat = log_file.stat()
                    log_files.append({
                        'path': str(log_file),
                        'size': stat.st_size,
                        'modified': stat.st_mtime
                    })
                except:
                    pass
    
    # Sort by modified time, newest first
    log_files.sort(key=lambda x: x['modified'], reverse=True)
    return log_files[:10]  # Return top 10

def main():
    print("=" * 60)
    print("FLASK SERVER DEBUGGING TOOL")
    print("=" * 60)
    print()
    
    # 1. Check Port
    print("1. CHECKING PORT 5000...")
    port_ok = check_port_listening(5000)
    if port_ok:
        print("   ✓ Port 5000 is LISTENING")
    else:
        print("   ✗ Port 5000 is NOT listening")
    print()
    
    # 2. Check Flask Processes
    print("2. CHECKING FLASK PROCESSES...")
    flask_procs = find_flask_processes()
    if flask_procs:
        print(f"   Found {len(flask_procs)} Flask/Python process(es):")
        for proc in flask_procs:
            print(f"   - PID: {proc['pid']}, Status: {proc['status']}")
            print(f"     Cmd: {proc['cmdline'][:100]}...")
    else:
        print("   ✗ No Flask process found")
    print()
    
    # 3. Check Environment Variables
    print("3. CHECKING ENVIRONMENT VARIABLES...")
    present, missing = check_env_vars()
    if present:
        print("   Present variables:")
        for var in present:
            print(f"   ✓ {var}")
    if missing:
        print("   Missing variables:")
        for var in missing:
            print(f"   ✗ {var}")
    else:
        print("   ✓ All required variables present")
    print()
    
    # 4. Test Database Connection
    print("4. TESTING DATABASE CONNECTION...")
    db_ok, db_msg = test_db_connection()
    if db_ok:
        print(f"   ✓ {db_msg}")
    else:
        print(f"   ✗ {db_msg}")
    print()
    
    # 5. Check Log Files
    print("5. CHECKING LOG FILES...")
    log_files = check_recent_logs()
    if log_files:
        print(f"   Found {len(log_files)} recent log file(s):")
        for log_file in log_files[:5]:
            from datetime import datetime
            mod_time = datetime.fromtimestamp(log_file['modified']).strftime("%Y-%m-%d %H:%M:%S")
            print(f"   - {log_file['path']}")
            print(f"     Size: {log_file['size']} bytes, Modified: {mod_time}")
    else:
        print("   No log files found")
    print()
    
    # 6. Recommendations
    print("=" * 60)
    print("RECOMMENDATIONS:")
    print("=" * 60)
    
    if not port_ok:
        print("• Port 5000 tidak listening - server mungkin tidak running")
        print("  Jalankan: python app.py atau gunakan systemd/service")
    
    if not flask_procs:
        print("• Tidak ada proses Flask ditemukan")
        print("  Start server dengan: python app.py")
    
    if missing:
        print(f"• Ada {len(missing)} environment variable yang missing")
        print("  Check file .env atau export variables")
    
    if not db_ok:
        print("• Database connection gagal")
        print("  Check database credentials dan network connectivity")
    
    print()
    print("Untuk melihat error detail, check:")
    print("1. Browser Developer Tools (F12) -> Network tab -> lihat response error")
    print("2. Server console output (jika running di foreground)")
    print("3. System logs: journalctl -u <service-name> (jika pakai systemd)")
    print("4. Check recent log files di atas")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError running debug script: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

