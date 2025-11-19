# Panduan Debugging Error di Production Server

## Server Info
- **URL**: http://34.50.67.24:5000/
- **Error**: Request URL error (tidak ada perubahan code)

## Langkah-langkah Debugging

### 1. Check dari Browser (Client Side)
1. Buka browser Developer Tools (F12)
2. Buka tab **Network**
3. Refresh halaman http://34.50.67.24:5000/
4. Klik request yang error (biasanya request pertama ke `/`)
5. Lihat tab **Response** dan **Headers**:
   - Status code berapa? (500, 502, 503, 404?)
   - Error message apa yang muncul?
   - Response body berisi apa?

### 2. SSH ke Server Production
```bash
ssh user@34.50.67.24
# atau sesuai dengan user dan method SSH yang digunakan
```

### 3. Check Apakah Server Masih Running

#### A. Check Port 5000
```bash
# Check apakah port 5000 listening
netstat -tuln | grep 5000
# atau
ss -tuln | grep 5000
# atau
lsof -i :5000
```

#### B. Check Proses Flask
```bash
# Cari proses Python/Flask
ps aux | grep python
ps aux | grep flask
ps aux | grep app.py

# Atau gunakan script debug_server.py
python3 debug_server.py
```

### 4. Check Log Aplikasi

#### A. Jika Running dengan systemd/service
```bash
# Check status service
sudo systemctl status armos-tools
# atau nama service yang sesuai

# Lihat log
sudo journalctl -u armos-tools -n 100 --no-pager
# atau
sudo journalctl -u armos-tools -f  # follow log real-time
```

#### B. Jika Running Manual (foreground/background)
```bash
# Cari file log
find . -name "*.log" -type f -mtime -1
ls -lah *.log 2>/dev/null

# Check output jika running di screen/tmux
screen -ls
tmux ls

# Jika running di background dengan nohup
ls -lah nohup.out
tail -f nohup.out
```

#### C. Check Python Error
```bash
# Jika ada traceback, biasanya muncul di console
# Check stderr output
```

### 5. Test Koneksi Database
```bash
# Test koneksi database dari server
python3 -c "
import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DATABASE_MAIN_HOST'),
        port=int(os.getenv('DATABASE_MAIN_PORT', 5432)),
        dbname=os.getenv('DATABASE_MAIN_NAME'),
        user=os.getenv('DATABASE_MAIN_USERNAME'),
        password=os.getenv('DATABASE_MAIN_PASS'),
        connect_timeout=5
    )
    print('✓ Database connection OK')
    conn.close()
except Exception as e:
    print(f'✗ Database connection failed: {e}')
"
```

### 6. Check Environment Variables
```bash
# Check apakah .env file ada
ls -lah .env

# Check environment variables penting
echo $DATABASE_MAIN_HOST
echo $DATABASE_MAIN_NAME
echo $SECRET_KEY
# (jangan print password di console)

# Atau gunakan script debug_server.py
python3 debug_server.py
```

### 7. Restart Server (Jika Perlu)

#### A. Jika Pakai systemd
```bash
sudo systemctl restart armos-tools
sudo systemctl status armos-tools
```

#### B. Jika Manual
```bash
# Kill proses lama
pkill -f "app.py"
# atau
kill <PID>

# Start ulang
cd /path/to/app
python3 app.py
# atau dengan nohup
nohup python3 app.py > app.log 2>&1 &
```

### 8. Check Error Common Issues

#### A. Import Error / Module Not Found
```bash
# Check apakah semua dependencies terinstall
pip3 list | grep -E "flask|psycopg2|pandas"

# Install ulang jika perlu
pip3 install -r requirements.txt
```

#### B. Permission Error
```bash
# Check permission file
ls -lah app.py
ls -lah data_log/
ls -lah templates/

# Fix permission jika perlu
chmod +x app.py
chmod -R 755 data_log/
```

#### C. Port Already in Use
```bash
# Check process yang pakai port 5000
lsof -i :5000
# Kill jika perlu
kill -9 <PID>
```

#### D. Database Connection Timeout
```bash
# Test network connectivity ke database
ping <DATABASE_HOST>
telnet <DATABASE_HOST> <DATABASE_PORT>
```

### 9. Enable Detailed Logging (Temporary)

Edit `app.py` untuk enable debug logging:

```python
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app_debug.log'),
        logging.StreamHandler()
    ]
)
```

### 10. Test Manual dari Server

```bash
# Test HTTP request dari server sendiri
curl http://localhost:5000/
curl -v http://localhost:5000/

# Test dengan IP external
curl http://34.50.67.24:5000/
```

## Script Otomatis

Gunakan script `debug_server.py` untuk check semua di atas sekaligus:

```bash
# Upload debug_server.py ke server
# Jalankan
python3 debug_server.py
```

Script ini akan check:
- ✓ Port listening status
- ✓ Flask processes
- ✓ Environment variables
- ✓ Database connection
- ✓ Recent log files

## Informasi yang Diperlukan untuk Debugging

Saat melaporkan error, siapkan informasi berikut:

1. **Error Message dari Browser**:
   - Status code (500, 502, 503, dll)
   - Response body/error message
   - Screenshot jika ada

2. **Server Status**:
   - Output dari `python3 debug_server.py`
   - Output dari `ps aux | grep python`
   - Output dari `netstat -tuln | grep 5000`

3. **Log Files**:
   - 50-100 baris terakhir dari log file
   - Error traceback jika ada

4. **Environment**:
   - OS version
   - Python version (`python3 --version`)
   - Apakah ada perubahan di server (update, restart, dll)

## Quick Fix Commands

```bash
# 1. Check dan restart service
sudo systemctl status armos-tools
sudo systemctl restart armos-tools

# 2. Check log real-time
sudo journalctl -u armos-tools -f

# 3. Test database
python3 -c "import psycopg2; print('psycopg2 OK')"

# 4. Run debug script
python3 debug_server.py
```

