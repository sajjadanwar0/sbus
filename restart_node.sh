#!/bin/bash
# restart_node.sh — Restart a single node after DR-6 kills it
# Usage: ./restart_node.sh <node_id>  (0, 1, or 2)
# Example: ./restart_node.sh 2

NODE_ID="${1:-2}"
SBUS_ROOT="${2:-$HOME/RustroverProjects/sbus}"
BINARY="$SBUS_ROOT/target/release/sbus-server"
PEERS="0=http://localhost:7000,1=http://localhost:7001,2=http://localhost:7002"
PORT=$((7000 + NODE_ID))

echo "Restarting Node $NODE_ID on port $PORT..."

# Kill anything still on the port
pid=$(lsof -ti :$PORT 2>/dev/null || true)
[ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
sleep 0.5

SBUS_PORT=$PORT SBUS_RAFT_NODE_ID=$NODE_ID \
SBUS_RAFT_PEERS="$PEERS" SBUS_ADMIN_ENABLED=1 RUST_LOG=warn \
"$BINARY" > "$SBUS_ROOT/logs/node${NODE_ID}.log" 2>&1 &

sleep 2
if curl -sf "http://localhost:$PORT/admin/health" > /dev/null 2>&1; then
    echo "Node $NODE_ID restarted on port $PORT ✅"
else
    echo "Node $NODE_ID failed to start ❌ — check logs/node${NODE_ID}.log"
fi