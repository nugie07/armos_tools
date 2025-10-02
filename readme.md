# ARMoS Utilities

Dokumentasi singkat untuk utilitas yang ada di repo ini: web app Flask, skrip integrasi WMS, dan skrip konversi.

## Persiapan Lingkungan

Buat file `.env` di root project berisi variabel berikut sesuai lingkungan Anda:

```
DATABASE_MAIN_HOST=localhost
DATABASE_MAIN_PORT=5432
DATABASE_MAIN_NAME=armos
DATABASE_MAIN_USERNAME=armos
DATABASE_MAIN_PASS=secret
WH_TYPE=9
PORT=5000

# Opsional untuk integrasi WMS (digunakan oleh get_inventory.py)
WMS_PROD_URL=api-wms.example.com/v2/auth/login
WMS_API_KEY=... 
WMS_SECRET=...
WMS_LIST_INV=api-wms.example.com/v2/inventory/list
WMS_AUTH_HEADER_PREFIX=Bearer 
WMS_LIST_INV_METHOD=POST

# Supabase untuk login
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_KEY=service-role-or-anon-key
```

Install dependency:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Menjalankan Web App

```bash
python3 app.py
```

Buka halaman utama dan gunakan menu:
- Update Lokasi Customer
- Update Uncheck Document Reconciliation
- Log Viewer
- PRODUCT to ROUTE
- Update WMS Integrasi
 - Upload Order (Convert & Send)
 - Sync Manager (termasuk dashboard ringkas)

### Menjalankan di background (tanpa systemd) dengan Gunicorn

Untuk run cepat di background dan bisa menutup terminal:

```bash
source .venv/bin/activate
pip install gunicorn
nohup gunicorn -w 2 -b 0.0.0.0:5000 app:app > gunicorn.log 2>&1 & echo $! > gunicorn.pid
# Cek log
tail -f gunicorn.log
# Reload setelah update kode/template
kill -HUP $(cat gunicorn.pid)
# Stop
kill $(cat gunicorn.pid)
```

Catatan: Perubahan file (termasuk `templates/login.html`) tidak otomatis terdeteksi. Gunakan perintah reload di atas atau stop+start agar perubahan terapply. Untuk produksi, gunakan systemd + Nginx (bagian deploy/SSL di bawah).

## Integrasi WMS – Ambil Inventory

Script: `get_inventory.py`

- Login ke `WMS_PROD_URL` menggunakan `api_key` dan `api_secret` dari `.env`
- Ambil inventory dari `WMS_LIST_INV` dengan header `Authorization: <WMS_AUTH_HEADER_PREFIX><token>`
- Simpan hasil ke `inventory_wms.json`

Jalankan:
```bash
python3 get_inventory.py
```

## Konversi Excel Order ke JSON

Script: `konversi.py`

- Membaca Excel `order_data` dan `order_detail`
- Normalisasi SKU, pack_id, dan UOM
- Menulis `output_order.json`

Jalankan:
```bash
python3 konversi.py
```

## Sync Manager & Dashboard (TMS Data Warehouse)

Fitur ini melakukan sinkronisasi data dari Database A (sumber) ke Database B (target) untuk dua fakta: `fact_order` dan `fact_delivery`, sekaligus menyediakan monitoring.

### Konfigurasi ENV Tambahan

Tambahkan ke `.env`:

```
# Database A (sumber)
DB_A_HOST=...
DB_A_PORT=5432
DB_A_NAME=...
DB_A_USER=...
DB_A_PASSWORD=...
DB_A_SCHEMA=public

# Database B (target)
DB_B_HOST=...
DB_B_PORT=5432
DB_B_NAME=...
DB_B_USER=...
DB_B_PASSWORD=...
DB_B_SCHEMA=public
```

Install deps tambahan (sudah di requirements.txt):
```
pip install -r requirements.txt
```

### UI

- Menu: `Sync Manager` → jalankan sinkronisasi (`fact_order` / `fact_delivery` / `both`) dan lihat ringkasan statistik (total/success/failed/last-sync) serta riwayat sinkronisasi terbaru dalam satu halaman.

### API

- `POST /api/sync/run`
  - Body: `{ "sync_type": "fact_order|fact_delivery|both", "date_from?": "YYYY-MM-DD", "date_to?": "YYYY-MM-DD" }`
  - Response: `{ "status": 200, "job_id": "..." }`

- `GET /api/sync/job/<job_id>` → status job in-memory (RUNNING/SUCCESS/FAILED)

- `GET /api/sync/status?sync_type?=&limit=20` → data riwayat dari tabel `tms_sync_log`

### Catatan Teknis

- Tabel `tms_sync_log` dibuat otomatis di Database B.
- Tabel target: `tms_fact_order`, `tms_fact_delivery` akan dibuat otomatis jika belum ada.
- Upsert dilakukan via SQLAlchemy engine dengan table sementara.

## Ekspor Log API ke JSON

- `log_konversi.py`: ekspor log HARI INI ke `data_log/DDMMYYYY_log.json` (overwrite jika ada)
- `konversi_30hari.py`: buat file per hari untuk 30 hari terakhir

Contoh jalan:
```bash
python3 log_konversi.py
python3 konversi_30hari.py
```

Cron setiap 30 menit:
```cron
*/30 * * * * /usr/bin/python3 /path/ke/armos_preprod/log_konversi.py >> /var/log/log_konversi.log 2>&1
```

## SSL/HTTPS Otomatis (Let's Encrypt)

Gunakan script `setup_ssl.sh` untuk memasang sertifikat gratis (masa berlaku ~90 hari) dan menyiapkan pembaruan otomatis.

Langkah:

1) Pastikan Nginx terpasang dan domain mengarah ke server Anda (DNS A record). Lalu jalankan:
```bash
sudo bash setup_ssl.sh app.example.com you@example.com
```

2) Script akan:
- Menginstal `nginx`, `certbot`, dan plugin `python3-certbot-nginx`.
- Mengeluarkan sertifikat untuk domain Anda dan mengonfigurasi Nginx.
- Menambahkan cron fallback untuk `certbot renew` (2x sehari) dan reload nginx setelah perpanjangan. Certbot juga memasang systemd timer auto-renew.

3) Cek jadwal auto-renew bawaan Certbot:
```bash
systemctl status certbot.timer
```
    