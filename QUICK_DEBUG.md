# Quick Debug Guide - Production Server Error

## Server: http://34.50.67.24:5000/

### Step 1: Check Error dari Browser
1. Buka http://34.50.67.24:5000/ di browser
2. Tekan **F12** (Developer Tools)
3. Buka tab **Network**
4. Refresh halaman
5. Klik request pertama (biasanya `/`)
6. Lihat:
   - **Status Code**: 500? 502? 503? 404?
   - **Response**: Error message apa?
   - Screenshot jika perlu

### Step 2: SSH ke Server
```bash
ssh user@34.50.67.24
# atau sesuai dengan konfigurasi SSH Anda
```

### Step 3: Jalankan Debug Script
```bash
# Upload debug_server.py ke server (jika belum)
# Jalankan
python3 debug_server.py
```

Script ini akan check:
- ✓ Port 5000 listening?
- ✓ Flask process running?
- ✓ Environment variables OK?
- ✓ Database connection OK?
- ✓ Log files location

### Step 4: Check Log File
```bash
# Check log file terbaru
tail -100 app.log

# Atau follow log real-time
tail -f app.log
```

### Step 5: Check Server Status
```bash
# Check apakah proses Flask running
ps aux | grep python
ps aux | grep app.py

# Check port 5000
netstat -tuln | grep 5000
# atau
lsof -i :5000
```

### Step 6: Restart Server (Jika Perlu)
```bash
# Jika pakai systemd
sudo systemctl restart armos-tools
sudo systemctl status armos-tools

# Jika manual
pkill -f "app.py"
cd /path/to/app
python3 app.py
```

## Informasi yang Diperlukan

Saat melaporkan error, siapkan:
1. **Status code** dari browser (Network tab)
2. **Error message** dari browser response
3. **Output** dari `python3 debug_server.py`
4. **50 baris terakhir** dari `app.log`
5. **Output** dari `ps aux | grep python`

## Common Issues

### Port 5000 Not Listening
- Server tidak running
- **Fix**: Start server dengan `python3 app.py`

### Database Connection Failed
- Database credentials salah
- Database server down
- Network issue
- **Fix**: Check `.env` file, test database connection

### Module Not Found
- Dependencies tidak terinstall
- **Fix**: `pip3 install -r requirements.txt`

### Permission Denied
- File permission issue
- **Fix**: `chmod +x app.py`, check folder permissions

## Quick Commands

```bash
# 1. Check semua status
python3 debug_server.py

# 2. Lihat log
tail -100 app.log

# 3. Test database
python3 -c "import psycopg2; print('OK')"

# 4. Restart
sudo systemctl restart armos-tools
# atau
pkill -f app.py && python3 app.py
```

