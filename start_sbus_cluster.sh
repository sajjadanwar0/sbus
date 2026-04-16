#!/bin/bash
# start_sbus_cluster.sh
# Starts N S-Bus instances on consecutive ports.
#
# USAGE:
#   ./start_sbus_cluster.sh [N] [BASE_PORT] [SBUS_BIN]
#
# EXAMPLES:
#   ./start_sbus_cluster.sh 5  7010 /home/neo/RustroverProjects/sbus/target/release/sbus-server
#   ./start_sbus_cluster.sh 20 7010 /home/neo/RustroverProjects/sbus/target/release/sbus-server
#
# STOP ALL:
#   ./stop_sbus_cluster.sh

N="${1:-5}"
BASE_PORT="${2:-7010}"
# Pass full absolute path as third argument — no ~ expansion needed
SBUS_BIN="${3:-/home/neo/RustroverProjects/sbus/target/release/sbus-server}"

echo "========================================"
echo "Starting S-Bus Cluster"
echo "========================================"
echo "Instances:  $N"
echo "Base port:  $BASE_PORT"
echo "Binary:     $SBUS_BIN"
echo ""

if [ ! -f "$SBUS_BIN" ]; then
    echo "ERROR: binary not found at $SBUS_BIN"
    echo ""
    echo "Check the exact binary name:"
    BINDIR=$(dirname "$SBUS_BIN")
    if [ -d "$BINDIR" ]; then
        echo "  Files in $BINDIR:"
        ls -la "$BINDIR" | grep -v "\.d$" | grep -v "\.rlib$"
    else
        echo "  Directory $BINDIR does not exist"
        echo "  Build: cd /home/neo/RustroverProjects/sbus && cargo build --release"
    fi
    exit 1
fi

mkdir -p logs
> logs/sbus_pids.txt
> logs/sbus_ports.txt

echo "Starting instances..."
for i in $(seq 0 $((N-1))); do
    PORT=$((BASE_PORT + i))
    SBUS_PORT="$PORT" \
    SBUS_ADMIN_ENABLED="1" \
    SBUS_SESSION_TTL="3600" \
    RUST_LOG="warn" \
    "$SBUS_BIN" > "logs/sbus_${PORT}.log" 2>&1 &
    PID=$!
    echo "$PID" >> logs/sbus_pids.txt
    echo "$PORT" >> logs/sbus_ports.txt
    echo "  Port $PORT: PID $PID"
done

echo ""
echo "Waiting for instances to be ready..."
sleep 3

ALL_OK=true
for i in $(seq 0 $((N-1))); do
    PORT=$((BASE_PORT + i))
    OK=false
    for attempt in $(seq 1 20); do
        if curl -s --max-time 1 "http://localhost:${PORT}/stats" > /dev/null 2>&1; then
            OK=true
            break
        fi
        sleep 1
    done
    if $OK; then
        echo "  Port $PORT: ready"
    else
        echo "  Port $PORT: FAILED — check logs/sbus_${PORT}.log"
        ALL_OK=false
    fi
done

echo ""
if $ALL_OK; then
    echo "All $N instances running on ports $BASE_PORT-$((BASE_PORT+N-1))"
    echo ""
    echo "To stop all: ./stop_sbus_cluster.sh"
else
    echo "Some instances failed. Check logs/"
    exit 1
fi