# Panduan Perubahan Log Viewer

## Setelah deploy – yang harus dijalankan

1. **Generate log format baru (hari ini)**  
   Jalankan sekali (atau andalkan cron yang sudah jalan):
   ```bash
   python log_konversi.py
   ```
   Ini akan membuat folder `data_log/DDMMYYYY/` dan file `{slug}.db` di dalamnya untuk hari ini.

2. **(Opsional) Backfill 30 hari**  
   Jika ingin isi log 30 hari terakhir dalam format baru:
   ```bash
   python konversi_30hari.py
   ```

3. **Batch delete (clean old logs)**  
   Tetap jalan lewat cron/scheduler seperti sebelumnya (isi `log_konversi.py` di bagian `if __name__ == "__main__"`). Tidak perlu perintah tambahan.

Tanpa menjalankan **log_konversi** (minimal sekali), dropdown "Pilih Folder" di Log Viewer akan kosong sampai ada folder tanggal yang terbentuk.

---

## Ringkasan

Log viewer diubah dari **satu file SQLite per hari** (`data_log/DDMMYYYY_log.db`) menjadi **satu folder per tanggal** berisi **satu file SQLite per tipe event** (`data_log/DDMMYYYY/{slug}.db`). Aplikasi sekarang meminta user **memilih folder (tanggal) dan event dulu**, baru data diload. Tambah **loading full-screen** yang memblok klik sampai selesai. **Batch delete** (clean old logs) disesuaikan untuk menghapus **folder** lama, bukan file `*_log.db` di root.

---

## 1. Struktur folder & file log (baru)

**Sebelum:**
```
data_log/
  20022026_log.db    # semua event hari itu dalam satu file
  21022026_log.db
  ...
```

**Sesudah:**
```
data_log/
  20022026/
    syncing_inventory.db
    synchronizing_order_manifest.db
    synchronizing_route_manifest_generation.db
    patch_order_status_sql.db
    patch_order_status_atena.db
    picklist_route.db
    feed_order_v2_sql_tms.db
    feed_order_v2_atena_tms.db
    webhook_good_issue_results.db
    other.db
  21022026/
    ...
```

- **Folder** = tanggal dalam format **DDMMYYYY** (mis. `20022026` = 20 Feb 2026).
- **File** = satu file per **event slug** (hanya file yang punya data yang dibuat; tidak ada file kosong).

---

## 2. File yang berubah

### 2.1 Baru

| File | Isi |
|------|-----|
| `log_config.py` | Mapping event → slug, daftar event untuk dropdown, konfigurasi kolom "Cari Request" per slug. Dipakai oleh `app.py` dan `log_konversi.py`. |

### 2.2 Diubah

| File | Perubahan |
|------|-----------|
| **log_konversi.py** | • Import `log_config` (LOG_EVENT_SLUGS, event_to_slug).<br>• `ensure_data_dir_date(date_folder)` → buat `data_log/DDMMYYYY/`.<br>• `write_logs_to_sqlite()` diganti: data di-**group by slug** lalu tulis per file dengan `_write_one_sqlite()`.<br>• `write_logs_to_sqlite_per_event(data, file_part)` → return **list path** (satu per slug).<br>• `write_sqlite_today()` return `List[str]` (path yang ditulis).<br>• **clean_old_logs()**: hapus **folder** `data_log/DDMMYYYY` yang lebih lama dari `retention_days` (bukan file `*_log.db`). |
| **app.py** | • Import `log_config`: LOG_EVENT_SLUGS, LOG_EVENT_REQUEST_CONFIG.<br>• **API**: `/api/log/files` dihapus.<br>• **API baru**: `/api/log/folders` (daftar folder DDMMYYYY), `/api/log/events` (slug + label + request_config).<br>• **API** `/api/log/search`: parameter `file` + `event` (label) diganti jadi **folder** + **event** (slug). Path jadi `data_log/{folder}/{event_slug}.db`. Query hanya filter request (karena satu DB = satu event). |
| **templates/log_viewer.html** | • **Loading**: overlay full-screen (tengah, memblok klik) saat load folder/event atau search.<br>• **Tidak auto-load**: halaman tidak load log di awal; user wajib pilih **folder** lalu **event**, lalu klik Search.<br>• Dropdown "Pilih File Log" → **"Pilih Folder (Tanggal)"** (isi dari `/api/log/folders`).<br>• Dropdown event dari `/api/log/events` (slug + label).<br>• Tombol Search disabled sampai folder dan event dipilih.<br>• Request field tetap per event (label/placeholder dari `request_config`). |
| **konversi_30hari.py** | • Pakai `write_logs_to_sqlite_per_event` (bukan `write_logs_to_sqlite`).<br>• `write_for_day()` return **list path**; print jumlah file per hari. |

---

## 3. Batch delete (clean old logs)

- **Lokasi**: `log_konversi.py` → fungsi `clean_old_logs(retention_days=7)`.
- **Perilaku lama**: Hapus file `data_log/DDMMYYYY_log.db` yang tanggalnya lebih lama dari `retention_days`.
- **Perilaku baru**: Hapus **folder** `data_log/DDMMYYYY` yang tanggalnya lebih lama dari `retention_days` (isi folder dihapus dulu, lalu folder di-rmdir).
- **Cara jalan**: Tetap lewat `if __name__ == "__main__"` di `log_konversi.py` atau panggil `clean_old_logs(retention_days=7)` dari script/cron yang sama seperti sebelumnya.

Tidak ada file lain yang memanggil batch delete; cukup sesuaikan cron/job yang menjalankan `log_konversi.py` (dan optional `konversi_30hari.py`) seperti biasa.

---

## 4. Alur user di aplikasi

1. Buka halaman Log Viewer.
2. **Loading** full-screen tampil saat load daftar folder + event (hanya sekali di awal).
3. Pilih **folder (tanggal)** dari dropdown.
4. Pilih **event** dari dropdown.
5. (Opsional) Isi kolom **Cari Request** sesuai event (Do Reference, Manifest Reference, dll.).
6. Klik **Search** → loading full-screen lagi sampai hasil keluar.
7. Hasil tampil di tabel; double klik baris untuk detail.

---

## 5. Migrasi data lama

- File lama `data_log/*_log.db` **tidak** dipakai lagi oleh aplikasi (API hanya baca `data_log/DDMMYYYY/{slug}.db`).
- Untuk hari-hari ke depan: jalankan **log_konversi** (atau konversi_30hari) seperti biasa; struktur baru akan terisi.
- Jika ingin memakai data lama: bisa buat script one-off yang baca setiap `*_log.db`, group by `event_to_slug(event)`, lalu tulis ke `data_log/DDMMYYYY/{slug}.db` sesuai format baru. Script tersebut tidak disertakan di repo; bisa ditambah terpisah jika diperlukan.

---

## 6. Referensi singkat

- **Event → slug**: `log_config.event_to_slug(event)`.
- **Daftar event + config**: `log_config.LOG_EVENT_SLUGS`, `log_config.LOG_EVENT_REQUEST_CONFIG`.
- **Path file log untuk tanggal + event**: `data_log/{DDMMYYYY}/{slug}.db`.
