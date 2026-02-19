# Panduan Setup / Pindah Server – ARMoS Utilities

Dokumen ini menjelaskan hal yang perlu diperhatikan saat setup aplikasi di server baru (pindah server) dan cara menjalankan script setup.

---

## 1. Yang Perlu Diperhatikan Saat Pindah Server

### 1.1 Persyaratan Server
- **OS**: Linux (Ubuntu 20.04+ / Debian 11+ disarankan)
- **Python**: 3.10 atau 3.11 (`python3 --version`)
- **Git**: untuk clone dan deploy (`git pull`)
- **PostgreSQL client**: aplikasi mengakses database eksternal via jaringan; tidak wajib install PostgreSQL di server ini selama koneksi ke host DB bisa dilakukan (port 5432 tidak di-firewall)

### 1.2 Konfigurasi Environment (`.env`)
Semua konfigurasi sensitif dan spesifik environment ada di file **`.env`** di root project. File ini **tidak** di-commit ke Git.

| Variabel | Wajib | Keterangan |
|----------|--------|------------|
| **Database utama (menu umum)** | | |
| `DATABASE_MAIN_HOST` | Ya | Host PostgreSQL |
| `DATABASE_MAIN_PORT` | Ya | Biasanya 5432 |
| `DATABASE_MAIN_NAME` | Ya | Nama database |
| `DATABASE_MAIN_USERNAME` | Ya | User DB |
| `DATABASE_MAIN_PASS` | Ya | Password DB |
| **Database Preprod** (menu Ubah Order Data, Export CSV, Update Order on Route, dll) | | |
| `DATABASE_PREPROD_HOST` | Ya | Host DB Preprod |
| `DATABASE_PREPROD_PORT` | Ya | Port (default 5432) |
| `DATABASE_PREPROD_NAME` | Ya | Nama database |
| `DATABASE_PREPROD_USERNAME` | Ya | User |
| `DATABASE_PREPROD_PASS` | Ya | Password |
| **Database Production** | | |
| `DATABASE_PROD_HOST` | Ya | Host DB Production |
| `DATABASE_PROD_PORT` | Ya | Port |
| `DATABASE_PROD_NAME` | Ya | Nama database |
| `DATABASE_PROD_USERNAME` | Ya | User |
| `DATABASE_PROD_PASS` | Ya | Password |
| **Session & Login** | | |
| `SECRET_KEY` atau `SUPABASE_KEY` | Ya | Untuk Flask session signing; wajib ada salah satu |
| `SUPABASE_URL` | Ya (jika pakai login Supabase) | URL project Supabase |
| `SUPABASE_KEY` | Ya (jika pakai login) | Key Supabase |
| **Lain-lain** | | |
| `PORT` | Tidak | Port app (default 5000) |
| `LOG_LEVEL` | Tidak | INFO / DEBUG / WARNING |
| **Sync Manager** (jika dipakai) | | |
| `DB_A_HOST`, `DB_A_PORT`, `DB_A_NAME`, `DB_A_USER`, `DB_A_PASSWORD`, `DB_A_SCHEMA` | Jika pakai Sync | Database sumber |
| `DB_B_HOST`, `DB_B_PORT`, `DB_B_NAME`, `DB_B_USER`, `DB_B_PASSWORD`, `DB_B_SCHEMA` | Jika pakai Sync | Database target |
| **WMS / Send Order** (jika dipakai) | | |
| `WMS_PROD_URL`, `WMS_API_KEY`, `WMS_SECRET`, `WMS_LIST_INV`, dll | Opsional | Sesuai integrasi WMS |
| `SEND_ORDER_USERNAME`, `SEND_ORDER_PASSWORD` | Opsional | Untuk fitur send order |

### 1.3 Folder yang Dibuat Otomatis oleh Aplikasi
Aplikasi membuat folder berikut di root project jika belum ada (tanpa perlu aksi Anda):
- `data_log/` – log viewer, ekspor log
- `data_archive_order/` – file CSV hasil Export Data to CSV
- Folder lain untuk import lokasi dll (jika ada)

Pastikan user yang menjalankan app (gunicorn) punya **izin tulis** di direktori project.

### 1.4 Firewall & Jaringan
- **Port aplikasi**: Buka port yang dipakai (default **5000**) jika akses dari luar (atau pakai Nginx reverse proxy dan buka 80/443).
- **Keluar ke database**: Server harus bisa **TCP ke host/port PostgreSQL** (Preprod, Prod, Main). Jika DB di cloud, pastikan security group / firewall mengizinkan IP server ini.

### 1.5 Hal yang Harus Dilakukan Manual di Server Baru
1. **Clone repo** (atau copy kode) ke direktori yang dipakai untuk deploy, misalnya `/opt/armos_preprod` atau `~/armos_preprod`.
2. **Buat/copy file `.env`** dari backup atau dari template `env.example` lalu edit nilai sesuai server/DB baru.
3. **Jalankan script setup** (lihat bawah) untuk venv, dependency, dan permission script.
4. **Start aplikasi**: `./start_gunicorn.sh` atau pakai systemd (lihat GUNICORN_GUIDE.md).
5. (Opsional) **Nginx + SSL**: gunakan `setup_ssl.sh` jika pakai domain dan HTTPS.

---

## 2. Script Setup: `setup_server.sh`

Script ini mempersiapkan environment di server baru (setelah repo sudah ada di server).

**Yang dilakukan script:**
- Cek Python 3 dan Git
- Buat virtual environment `.venv` jika belum ada
- Install dependency dari `requirements.txt`
- Jika `.env` belum ada: copy dari `env.example` ke `.env` dan mengingatkan Anda untuk mengedit
- Buat folder `data_log`, `data_archive_order` jika belum ada
- Set permission execute untuk `start_gunicorn.sh`, `deploy.sh`, `setup_server.sh`

**Cara pakai:**

```bash
# Di root project (setelah clone/copy)
chmod +x setup_server.sh
./setup_server.sh
```

Setelah selesai:
1. **Edit `.env`** dengan nilai yang benar (host DB, user, password, SECRET_KEY, SUPABASE_*, dll).
2. Jalankan **`./start_gunicorn.sh`** untuk menjalankan aplikasi.
3. Untuk update kode ke depan: **`./deploy.sh`** (git pull + reload gunicorn).

---

## 3. Ringkasan Alur Pindah Server

| Langkah | Deskripsi |
|--------|-----------|
| 1 | Clone repo ke server: `git clone <url> armos_preprod && cd armos_preprod` |
| 2 | Jalankan setup: `chmod +x setup_server.sh && ./setup_server.sh` |
| 3 | Edit `.env`: database (Main, Preprod, Prod), SECRET_KEY/SUPABASE_*, port jika perlu |
| 4 | Test koneksi DB (bisa lewat menu di web setelah app jalan, atau script kecil) |
| 5 | Start app: `./start_gunicorn.sh` |
| 6 | Cek: `curl http://localhost:5000` atau buka dari browser |
| 7 | (Opsional) Nginx + SSL: `sudo bash setup_ssl.sh your-domain.com email@example.com` |
| 8 | Deploy berikutnya: `./deploy.sh` |

---

## 4. Troubleshooting Singkat

- **App tidak start**: Cek `gunicorn.log`, pastikan `.env` ada dan variabel wajib terisi, port 5000 tidak dipakai proses lain.
- **Error koneksi database**: Cek `DATABASE_*_HOST`, firewall, dan credentials di `.env`.
- **Login tidak jalan**: Pastikan `SUPABASE_URL`, `SUPABASE_KEY`, dan `SECRET_KEY` (atau `SUPABASE_KEY`) benar.
- **Export CSV / menu error**: Pastikan konfigurasi `DATABASE_PREPROD_*` dan `DATABASE_PROD_*` lengkap dan bisa diakses dari server baru.

Untuk panduan Gunicorn (start/stop/reload), lihat **GUNICORN_GUIDE.md**.
