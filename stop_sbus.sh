#!/bin/bash
# stop_sbus_cluster.sh
# Kills all S-Bus instances started by start_sbus_cluster.sh

PID_FILE="logs/sbus_pids.txt"

if [ ! -f "$PID_FILE" ]; then
    echo "No PID file found at $PID_FILE"
    echo "Kill manually: pkill -f 'sbus'"
    exit 1
fi

echo "Stopping S-Bus cluster..."
while read -r pid; do
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "  Killed PID $pid"
    else
        echo "  PID $pid already stopped"
    fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "Done. All instances stopped."