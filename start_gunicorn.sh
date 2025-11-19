#!/bin/bash
# Script untuk start gunicorn server
# Usage: ./start_gunicorn.sh

# Get directory where script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activate virtual environment if exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
fi

# Check if gunicorn is installed
if ! command -v gunicorn &> /dev/null; then
    echo "Error: gunicorn tidak ditemukan. Install dengan: pip install gunicorn"
    exit 1
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "Warning: File .env tidak ditemukan"
fi

# Check if port 5000 is already in use
if lsof -Pi :5000 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "Warning: Port 5000 sudah digunakan"
    echo "Process yang menggunakan port 5000:"
    lsof -i :5000
    echo ""
    read -p "Lanjutkan start gunicorn? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Remove old PID file if exists
if [ -f "gunicorn.pid" ]; then
    OLD_PID=$(cat gunicorn.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo "Warning: Process dengan PID $OLD_PID masih running"
        echo "Stop process tersebut terlebih dahulu dengan: kill $OLD_PID"
        exit 1
    else
        echo "Removing stale PID file..."
        rm -f gunicorn.pid
    fi
fi

# Start gunicorn
echo "Starting gunicorn..."
echo "Workers: 2"
echo "Bind: 0.0.0.0:5000"
echo "Log: gunicorn.log"
echo ""

nohup gunicorn -w 2 -b 0.0.0.0:5000 app:app > gunicorn.log 2>&1 & 
GUNICORN_PID=$!
echo $GUNICORN_PID > gunicorn.pid

# Wait a moment to check if it started successfully
sleep 2

if ps -p $GUNICORN_PID > /dev/null 2>&1; then
    echo "✓ Gunicorn started successfully!"
    echo "  PID: $GUNICORN_PID"
    echo "  PID file: gunicorn.pid"
    echo "  Log file: gunicorn.log"
    echo ""
    echo "Commands:"
    echo "  Check status: ps aux | grep gunicorn"
    echo "  View log: tail -f gunicorn.log"
    echo "  Reload: kill -HUP \$(cat gunicorn.pid)"
    echo "  Stop: kill \$(cat gunicorn.pid)"
else
    echo "✗ Gunicorn failed to start!"
    echo "Check gunicorn.log for errors:"
    tail -20 gunicorn.log
    rm -f gunicorn.pid
    exit 1
fi

