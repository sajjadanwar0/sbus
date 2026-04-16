#!/bin/bash
# ============================================================
# start_raft_cluster.sh — S-Bus v37 (Raft + sled persistence)
# Usage: ./start_raft_cluster.sh [sbus_root_path]
# Example: ./start_raft_cluster.sh ~/RustroverProjects/sbus
# ============================================================

set -e

SBUS_ROOT="${1:-$HOME/RustroverProjects/sbus}"
BINARY="$SBUS_ROOT/target/release/sbus-server"
PEERS="0=http://localhost:7000,1=http://localhost:7001,2=http://localhost:7002"

# ── Colors ───────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERR]${NC}  $1"; exit 1; }

echo -e "${BOLD}S-Bus v37 — Raft + sled Cluster Bootstrap${NC}"
echo "==========================================="

# ── Step 1: Binary ───────────────────────────────────────────
info "Step 1: Checking binary..."
if [ ! -f "$BINARY" ]; then
    warn "Binary not found at $BINARY"
    info "Building release binary (sled is pure Rust — no system deps)..."
    cd "$SBUS_ROOT"
    cargo build --release 2>&1 | tail -3
    [ -f "$BINARY" ] || error "Build failed. Check cargo output."
fi
success "Binary: $BINARY"

# ── Step 2: Kill existing processes ──────────────────────────
info "Step 2: Clearing ports 7000-7002..."
for port in 7000 7001 7002; do
    pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "$pids" | xargs kill -9 2>/dev/null || true
        warn "Killed existing process on port $port"
    fi
done
pkill -f "sbus-server" 2>/dev/null || true
sleep 1
success "Ports cleared"

# ── Step 3: Create directories ───────────────────────────────
info "Step 3: Creating directories..."
mkdir -p "$SBUS_ROOT/logs"
mkdir -p "$SBUS_ROOT/results"
# sled data directories — one per node (persists across restarts)
mkdir -p "$SBUS_ROOT/data/node0"
mkdir -p "$SBUS_ROOT/data/node1"
mkdir -p "$SBUS_ROOT/data/node2"
success "Directories ready"

# ── Step 4: Start 3 nodes ─────────────────────────────────────
info "Step 4: Starting nodes..."

SBUS_PORT=7000 SBUS_RAFT_NODE_ID=0 \
SBUS_RAFT_PEERS="$PEERS" \
SBUS_DATA_DIR="$SBUS_ROOT/data/node0" \
SBUS_ADMIN_ENABLED=1 RUST_LOG=warn \
"$BINARY" >> "$SBUS_ROOT/logs/node0.log" 2>&1 &
NODE0_PID=$!
echo $NODE0_PID > /tmp/sbus_node0.pid
success "Node 0 started (PID $NODE0_PID, port 7000, data: data/node0)"

SBUS_PORT=7001 SBUS_RAFT_NODE_ID=1 \
SBUS_RAFT_PEERS="$PEERS" \
SBUS_DATA_DIR="$SBUS_ROOT/data/node1" \
SBUS_ADMIN_ENABLED=1 RUST_LOG=warn \
"$BINARY" >> "$SBUS_ROOT/logs/node1.log" 2>&1 &
NODE1_PID=$!
echo $NODE1_PID > /tmp/sbus_node1.pid
success "Node 1 started (PID $NODE1_PID, port 7001, data: data/node1)"

SBUS_PORT=7002 SBUS_RAFT_NODE_ID=2 \
SBUS_RAFT_PEERS="$PEERS" \
SBUS_DATA_DIR="$SBUS_ROOT/data/node2" \
SBUS_ADMIN_ENABLED=1 RUST_LOG=warn \
"$BINARY" >> "$SBUS_ROOT/logs/node2.log" 2>&1 &
NODE2_PID=$!
echo $NODE2_PID > /tmp/sbus_node2.pid
success "Node 2 started (PID $NODE2_PID, port 7002, data: data/node2)"

# ── Step 5: Wait for all 3 nodes to be ready ─────────────────
info "Step 5: Waiting for nodes to be ready..."
for port in 7000 7001 7002; do
    for i in $(seq 1 20); do
        if curl -sf "http://localhost:$port/admin/health" > /dev/null 2>&1; then
            success "Node on port $port is UP"
            break
        fi
        [ $i -eq 20 ] && error "Node $port did not start. Check: $SBUS_ROOT/logs/node$(( port-7000 )).log"
        sleep 0.5
    done
done

# ── Step 6: Bootstrap Raft cluster ───────────────────────────
info "Step 6: Bootstrapping Raft..."
sleep 1

INIT=$(curl -sf -X POST http://localhost:7000/raft/init 2>&1 || echo "{}")
if echo "$INIT" | grep -q '"initialized"\|AlreadyInitialized\|already'; then
    success "Raft initialized on node 0"
else
    success "Raft init response: $INIT"
fi
sleep 1

info "  Adding node 1 as learner..."
curl -sf -X POST http://localhost:7000/raft/add-learner \
    -H 'Content-Type: application/json' \
    -d '{"node_id":1,"addr":"http://localhost:7001"}' > /dev/null
success "Node 1 added"
sleep 1

info "  Adding node 2 as learner..."
curl -sf -X POST http://localhost:7000/raft/add-learner \
    -H 'Content-Type: application/json' \
    -d '{"node_id":2,"addr":"http://localhost:7002"}' > /dev/null
success "Node 2 added"
sleep 1

info "  Changing membership to [0, 1, 2] voters..."
curl -sf -X POST http://localhost:7000/raft/change-membership \
    -H 'Content-Type: application/json' \
    -d '{"members":[0,1,2]}' > /dev/null
success "3-node voting cluster configured"
sleep 2

# ── Step 7: Verify cluster status ────────────────────────────
info "Step 7: Verifying cluster health..."
echo ""
echo -e "${BOLD}Cluster Status:${NC}"
echo "-------------------------------------------"
for port in 7000 7001 7002; do
    RESP=$(curl -sf "http://localhost:$port/raft/leader" 2>/dev/null || echo '{}')
    STATE=$(echo "$RESP" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(d.get('state','?'))" 2>/dev/null || echo "?")
    IS_LEADER=$(echo "$RESP" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print('★ LEADER' if d.get('is_leader') else '  follower')" 2>/dev/null || echo "?")
    echo -e "  Node $((port-7000)) (port $port)  ${GREEN}$STATE${NC}  $IS_LEADER"
done

LEADER=$(curl -sf http://localhost:7000/raft/leader 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('current_leader','?'))" 2>/dev/null || echo "?")
echo ""
if [ "$LEADER" != "?" ] && [ -n "$LEADER" ]; then
    success "Cluster is healthy!  Current leader = Node $LEADER"
else
    warn "Leader not yet elected — wait 2s then: curl http://localhost:7000/raft/leader"
fi

# ── Usage ─────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}Data directories (sled — persist across restarts):${NC}"
echo "  data/node0/   data/node1/   data/node2/"
echo ""
echo -e "${BOLD}Quick test (shard replicates to all 3 nodes via Raft):${NC}"
echo "  curl -X POST http://localhost:7000/shard \\"
echo "       -H 'Content-Type: application/json' \\"
echo "       -d '{\"key\":\"hello\",\"content\":\"world\",\"goal_tag\":\"test\"}'"
echo "  curl http://localhost:7001/shard/hello   # follower — served from Raft replica"
echo "  curl http://localhost:7002/shard/hello   # follower"
echo ""
echo -e "${BOLD}Run experiments:${NC}"
echo "  python3 exp_distributed.py --all \\"
echo "    --node0 http://localhost:7000 \\"
echo "    --node1 http://localhost:7001 \\"
echo "    --node2 http://localhost:7002 \\"
echo "    --output results/exp_all.json"
echo ""
echo "  # DR-8 dynamic node addition:"
echo "  python3 exp_distributed.py --dr8 \\"
echo "    --node0 http://localhost:7000 \\"
echo "    --node1 http://localhost:7001 \\"
echo "    --node2 http://localhost:7002 \\"
echo "    --node3 http://localhost:7003 \\"
echo "    --output results/exp_dr8.json"
echo ""
echo -e "${BOLD}Logs:${NC}  $SBUS_ROOT/logs/node{0,1,2}.log"
echo -e "${BOLD}Stop:${NC}  ./stop_raft_cluster.sh"
echo -e "${BOLD}Reset sled data:${NC}  rm -rf data/   (wipes persistence — clean slate)"