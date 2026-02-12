#!/bin/bash
# Deploy script: git pull + graceful reload gunicorn
# Usage: ./deploy.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "  DEPLOY - $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

# --- Step 1: Git pull ---
echo "[1/2] Git pull origin main..."
echo "------------------------------------------"
if git pull origin main; then
    echo "------------------------------------------"
    echo "[1/2] Git pull selesai."
else
    echo "------------------------------------------"
    echo "[1/2] Git pull gagal. Deploy dihentikan."
    exit 1
fi
echo ""

# --- Step 2: Reload Gunicorn ---
echo "[2/2] Reload Gunicorn (kill -HUP)..."
echo "------------------------------------------"
if [ ! -f "gunicorn.pid" ]; then
    echo "File gunicorn.pid tidak ditemukan. Lewat reload."
    echo "Jalankan start_gunicorn.sh jika server belum berjalan."
else
    GUNICORN_PID=$(cat gunicorn.pid)
    if kill -0 "$GUNICORN_PID" 2>/dev/null; then
        kill -HUP "$GUNICORN_PID"
        echo "Signal HUP terkirim ke PID: $GUNICORN_PID"
        echo "[2/2] Gunicorn reload selesai."
    else
        echo "Proses gunicorn (PID $GUNICORN_PID) tidak berjalan. Lewat reload."
        echo "Jalankan start_gunicorn.sh untuk start server."
    fi
fi
echo "------------------------------------------"
echo ""

echo "=========================================="
echo "  DEPLOY SELESAI - $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
