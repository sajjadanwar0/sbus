#!/bin/bash
# stop_raft_cluster.sh — Stop all S-Bus Raft nodes

echo "Stopping S-Bus Raft cluster..."
for port in 7000 7001 7002; do
    pid=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pid" ]; then
        kill -TERM $pid 2>/dev/null || true
        echo "  Stopped node on port $port (PID $pid)"
    fi
done
pkill -f sbus-server 2>/dev/null || true
sleep 1
echo "Cluster stopped."