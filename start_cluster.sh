#!/bin/bash
# start_cluster.sh — Start a 2-node S-Bus cluster for Exp. DR
# Usage: ./start_cluster.sh /path/to/sbus-server

SBUS_BIN=${1:-./target/release/sbus-server}
NODES="http://localhost:7000,http://localhost:7001"
mkdir -p logs

echo "Starting S-Bus cluster (2 nodes)..."

SBUS_PORT=7000 SBUS_NODE_ID=0 \
SBUS_CLUSTER_NODES="$NODES" \
SBUS_ADMIN_ENABLED=1 RUST_LOG=warn \
"$SBUS_BIN" > logs/node0.log 2>&1 &
echo "  Node 0: PID $! (port 7000)"

SBUS_PORT=7001 SBUS_NODE_ID=1 \
SBUS_CLUSTER_NODES="$NODES" \
SBUS_ADMIN_ENABLED=1 RUST_LOG=warn \
"$SBUS_BIN" > logs/node1.log 2>&1 &
echo "  Node 1: PID $! (port 7001)"

sleep 3

for port in 7000 7001; do
    if curl -sf "http://localhost:$port/admin/health" > /dev/null; then
        echo "  Port $port: ready ✓"
    else
        echo "  Port $port: FAILED — check logs/node$(( port - 7000 )).log"
    fi
done

echo ""
echo "Cluster status:"
curl -s "http://localhost:7000/cluster/status" | python3 -c \
  "import json,sys; d=json.load(sys.stdin); \
   c=d.get('cluster',{}); \
   print(f'  Nodes: {c.get(\"num_nodes\",\"?\")}  Routing: {c.get(\"routing\",\"?\")}'); \
   [print(f'  {p[\"node_id\"]}: {p[\"url\"]} [{p[\"status\"]}]') for p in c.get('peers',[])]"

echo ""
echo "Run experiment:"
echo "  python3 exp_distributed.py --output results/exp_dr.json"