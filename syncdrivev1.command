#!/bin/bash
# SyncDrive V1 - Double-click to run

cd "$(dirname "$0")"
SCRIPT_DIR="$(pwd)"
SRC_DIR="$SCRIPT_DIR/syncdrivev1_src"

echo "========================================"
echo "  SyncDrive V1"
echo "========================================"
echo ""

# Setup if needed
if [ ! -d "$SRC_DIR/.venv" ]; then
    echo "First-time setup..."
    echo ""
    echo "Step 1/2: Creating Python environment..."
    python3 -m venv "$SRC_DIR/.venv"
    echo "Done."
    echo ""
    echo "Step 2/2: Installing Flask..."
    "$SRC_DIR/.venv/bin/pip" install flask
    echo ""
    echo "Setup complete!"
    echo ""
fi

# Kill any existing
pkill -f 'syncdrivev1.py' 2>/dev/null

echo "Starting server..."
"$SRC_DIR/.venv/bin/python" "$SRC_DIR/syncdrivev1.py" &
SERVER_PID=$!

sleep 2
echo ""
echo "Opening browser..."
open "http://localhost:5050"

echo ""
echo "========================================"
echo "  Server running at http://localhost:5050"
echo "  Press Enter to stop..."
echo "========================================"
read

kill $SERVER_PID 2>/dev/null
pkill -f 'syncdrivev1.py' 2>/dev/null
echo "Server stopped."
