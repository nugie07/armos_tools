# Panduan Gunicorn - Start, Stop, Reload

## Status Gunicorn

Gunicorn bisa mati sendiri karena beberapa alasan:
- **Crash/Error**: Aplikasi error yang tidak tertangani
- **Out of Memory**: Server kehabisan memory
- **System Reboot**: Server restart
- **Manual Kill**: Process di-kill oleh user/admin lain

## Check Status Gunicorn

### 1. Check Process
```bash
# Check apakah gunicorn running
ps aux | grep gunicorn

# Check PID dari file
cat gunicorn.pid

# Check apakah PID tersebut masih running
ps -p $(cat gunicorn.pid) 2>/dev/null && echo "Running" || echo "Not running"
```

### 2. Check Port
```bash
# Check apakah port 5000 listening
lsof -i :5000
# atau
netstat -tuln | grep 5000
```

### 3. Check Log
```bash
# Lihat log terakhir
tail -50 gunicorn.log

# Follow log real-time
tail -f gunicorn.log

# Check error di log
grep -i error gunicorn.log | tail -20
```

## Start Gunicorn

### Method 1: Menggunakan Script (Recommended)
```bash
# Berikan permission execute
chmod +x start_gunicorn.sh

# Jalankan script
./start_gunicorn.sh
```

### Method 2: Manual Start
```bash
# Aktifkan virtual environment
source .venv/bin/activate
# atau
source venv/bin/activate

# Start gunicorn
nohup gunicorn -w 2 -b 0.0.0.0:5000 app:app > gunicorn.log 2>&1 & echo $! > gunicorn.pid

# Verify
ps aux | grep gunicorn
tail -f gunicorn.log
```

### Method 3: Dengan systemd (Production)
Jika menggunakan systemd service:
```bash
sudo systemctl start armos-tools
sudo systemctl status armos-tools
```

## Stop Gunicorn

### Method 1: Menggunakan PID File
```bash
# Stop gracefully
kill $(cat gunicorn.pid)

# Force stop jika tidak merespon
kill -9 $(cat gunicorn.pid)

# Remove PID file setelah stop
rm -f gunicorn.pid
```

### Method 2: Manual Kill
```bash
# Cari PID
ps aux | grep gunicorn

# Kill dengan PID
kill <PID>
# atau force
kill -9 <PID>
```

### Method 3: Kill All Gunicorn Processes
```bash
# Hati-hati dengan command ini!
pkill -f gunicorn
```

## Reload Gunicorn (Setelah Update Code)

```bash
# Reload tanpa down time (graceful restart)
kill -HUP $(cat gunicorn.pid)

# Verify reload
tail -f gunicorn.log
```

**Catatan**: Reload hanya bekerja jika gunicorn masih running. Jika sudah mati, gunakan start ulang.

## Deploy (Pull + Reload)

Script `deploy.sh` menjalankan **git pull origin main** lalu **kill -HUP** ke gunicorn (graceful reload). Progress deploy ditampilkan di layar.

```bash
# Beri permission sekali saja
chmod +x deploy.sh

# Jalankan deploy
./deploy.sh
```

Output contoh:
```
==========================================
  DEPLOY - 2025-02-15 10:30:00
==========================================

[1/2] Git pull origin main...
------------------------------------------
From https://github.com/...
Already up to date.
------------------------------------------
[1/2] Git pull selesai.

[2/2] Reload Gunicorn (kill -HUP)...
------------------------------------------
Signal HUP terkirim ke PID: 12345
[2/2] Gunicorn reload selesai.
------------------------------------------

==========================================
  DEPLOY SELESAI - 2025-02-15 10:30:05
==========================================
```

Jika `gunicorn.pid` tidak ada atau proses sudah tidak berjalan, reload akan dilewati; jalankan `./start_gunicorn.sh` untuk start server.

## Troubleshooting

### Error: "No such process"
**Penyebab**: Process sudah tidak ada, tapi PID file masih ada.

**Solusi**:
```bash
# Remove stale PID file
rm -f gunicorn.pid

# Start ulang
./start_gunicorn.sh
# atau manual
nohup gunicorn -w 2 -b 0.0.0.0:5000 app:app > gunicorn.log 2>&1 & echo $! > gunicorn.pid
```

### Error: "Address already in use"
**Penyebab**: Port 5000 sudah digunakan oleh process lain.

**Solusi**:
```bash
# Check process yang pakai port 5000
lsof -i :5000

# Kill process tersebut
kill <PID>

# Atau gunakan port lain
gunicorn -w 2 -b 0.0.0.0:5001 app:app
```

### Gunicorn Mati Terus Menerus
**Penyebab**: Kemungkinan ada error di aplikasi yang menyebabkan crash.

**Solusi**:
```bash
# Check log untuk error
tail -100 gunicorn.log | grep -i error

# Check app.log juga
tail -100 app.log

# Run app.py langsung untuk lihat error
python3 app.py
```

### Out of Memory
**Penyebab**: Server kehabisan memory.

**Solusi**:
```bash
# Check memory usage
free -h

# Kurangi jumlah workers
gunicorn -w 1 -b 0.0.0.0:5000 app:app

# Atau tambah swap space
```

## Auto-Start dengan systemd (Recommended untuk Production)

Buat file `/etc/systemd/system/armos-tools.service`:

```ini
[Unit]
Description=ARMOS Tools Flask App
After=network.target

[Service]
Type=notify
User=your_user
WorkingDirectory=/path/to/armos_preprod
Environment="PATH=/path/to/armos_preprod/.venv/bin"
ExecStart=/path/to/armos_preprod/.venv/bin/gunicorn -w 2 -b 0.0.0.0:5000 app:app
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable dan start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable armos-tools
sudo systemctl start armos-tools
sudo systemctl status armos-tools
```

## Monitoring

### Check Health
```bash
# Test HTTP request
curl http://localhost:5000/

# Check response time
time curl http://localhost:5000/
```

### Check Resource Usage
```bash
# CPU dan Memory usage
top -p $(cat gunicorn.pid)

# Atau dengan htop
htop -p $(cat gunicorn.pid)
```

## Quick Commands Summary

```bash
# Start
./start_gunicorn.sh

# Stop
kill $(cat gunicorn.pid)

# Reload
kill -HUP $(cat gunicorn.pid)

# Check status
ps aux | grep gunicorn

# View log
tail -f gunicorn.log

# Check port
lsof -i :5000
```

