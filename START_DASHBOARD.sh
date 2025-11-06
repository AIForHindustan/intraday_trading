#!/bin/bash
# Quick start script for the trading dashboard

echo "🚀 Starting Intraday Trading Dashboard..."
echo ""

# Check if backend is running (using port 5001 - port 5000 is blocked by AirPlay on macOS)
if curl -s http://localhost:5001/api/alerts/stats/summary > /dev/null 2>&1; then
    echo "✅ Backend is already running on port 5001"
else
    echo "📡 Starting backend server..."
    cd "$(dirname "$0")"
    python3 -m uvicorn alerts.optimized_main:app --host 0.0.0.0 --port 5001 --reload > backend.log 2>&1 &
    BACKEND_PID=$!
    echo "   Backend PID: $BACKEND_PID"
    echo "   Logs: backend.log"
    sleep 3
    if curl -s http://localhost:5001/api/alerts/stats/summary > /dev/null 2>&1; then
        echo "✅ Backend started successfully"
    else
        echo "⚠️  Backend may still be starting..."
    fi
fi

echo ""
echo "🌐 Starting frontend development server..."
cd "$(dirname "$0")/frontend"
/opt/homebrew/bin/npm run dev

