#!/bin/bash
# Setup aplikasi ARMoS Utilities di server baru (setelah clone/copy repo)
# Usage: chmod +x setup_server.sh && ./setup_server.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "  ARMoS Utilities - Setup Server"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

# --- Cek Python 3 ---
echo "[1/6] Cek Python 3..."
if ! command -v python3 &>/dev/null; then
    echo "  Error: python3 tidak ditemukan. Install Python 3.10+ terlebih dahulu."
    exit 1
fi
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "  Python: $(python3 --version)"
echo ""

# --- Cek Git (opsional, untuk deploy) ---
echo "[2/6] Cek Git..."
if command -v git &>/dev/null; then
    echo "  Git: $(git --version)"
else
    echo "  Warning: git tidak ditemukan. Install git jika akan pakai ./deploy.sh"
fi
echo ""

# --- Virtual environment ---
echo "[3/6] Virtual environment..."
if [ -d ".venv" ]; then
    echo "  .venv sudah ada."
else
    echo "  Membuat .venv..."
    python3 -m venv .venv
    echo "  .venv dibuat."
fi
echo "  Mengaktifkan .venv dan install dependency..."
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install -r requirements.txt
echo "  Dependency terinstall."
echo ""

# --- File .env ---
echo "[4/6] Konfigurasi .env..."
if [ -f ".env" ]; then
    echo "  File .env sudah ada. Tidak menimpa."
else
    if [ -f "env.example" ]; then
        cp env.example .env
        echo "  File .env dibuat dari env.example."
        echo "  PENTING: Edit .env dan isi nilai yang benar (database, SECRET_KEY, SUPABASE_*, dll)."
    else
        echo "  Warning: env.example tidak ditemukan. Buat file .env secara manual."
        echo "  Lihat SETUP_SERVER.md untuk daftar variabel yang diperlukan."
    fi
fi
echo ""

# --- Folder data ---
echo "[5/6] Folder data..."
for dir in data_log data_archive_order; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "  Folder $dir dibuat."
    else
        echo "  Folder $dir sudah ada."
    fi
done
echo ""

# --- Permission script ---
echo "[6/6] Permission script..."
for script in setup_server.sh start_gunicorn.sh deploy.sh; do
    if [ -f "$script" ]; then
        chmod +x "$script"
        echo "  chmod +x $script"
    fi
done
echo ""

echo "=========================================="
echo "  SETUP SELESAI"
echo "=========================================="
echo ""
echo "Langkah berikutnya:"
echo "  1. Edit .env dengan nilai yang benar (host DB, user, password, SECRET_KEY, SUPABASE_*, dll)."
echo "  2. Jalankan aplikasi: ./start_gunicorn.sh"
echo "  3. Cek: curl http://localhost:5000 atau buka dari browser."
echo "  4. Untuk update kode nanti: ./deploy.sh"
echo ""
echo "Dokumentasi: SETUP_SERVER.md, GUNICORN_GUIDE.md, readme.md"
echo ""
