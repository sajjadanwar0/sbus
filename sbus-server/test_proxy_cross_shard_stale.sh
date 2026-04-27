#!/usr/bin/env bash
set -u

SBUS_URL="${SBUS_URL:-http://localhost:7000}"

bold()  { printf "\n\033[1m%s\033[0m\n" "$*"; }
ok()    { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail()  { printf "  \033[31m✗\033[0m %s\n" "$*"; }
info()  { printf "    %s\n" "$*"; }
hr()    { printf '  ───────────────────────────────────────────\n'; }

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required tool '$1' not found on PATH"
    exit 2
  fi
}

reset_state() {
  local r
  r=$(curl -sS -X POST "$SBUS_URL/admin/reset") || { fail "reset failed"; exit 2; }
  info "reset: $r"
}

create_shard() {
  local key="$1"
  curl -sS -X POST "$SBUS_URL/shard" \
       -H 'content-type: application/json' \
       -d "{\"key\":\"$key\",\"content\":\"v0\",\"goal_tag\":\"test\"}" \
    >/dev/null
}

commit() {
  local key="$1"; local ev="$2"; local agent="$3"; local rs="$4"
  curl -sS -X POST "$SBUS_URL/commit/v2" \
       -H 'content-type: application/json' \
       -d "{
            \"key\":\"$key\",
            \"expected_version\":$ev,
            \"delta\":\"test-write\",
            \"agent_id\":\"$agent\",
            \"read_set\":$rs
           }"
}

proxy_register() {
  local agent="$1"; local shard="$2"
  curl -sS -X POST "$SBUS_URL/delivery_log/register" \
       -H 'content-type: application/json' \
       -d "{
            \"agent_id\":\"$agent\",
            \"session_id\":\"test\",
            \"shards_used\":[\"$shard\"],
            \"source\":\"proxy\",
            \"timestamp_ms\":0
           }"
}

require curl
require jq

bold "preflight"
if ! curl -sSf "$SBUS_URL/stats" >/dev/null 2>&1; then
  fail "cannot reach S-Bus at $SBUS_URL (is the server running?)"
  exit 2
fi
ok "S-Bus is reachable at $SBUS_URL"

PASSED=0
FAILED=0

bold "case A (positive): proxy-recorded read → staleness → CrossShardStale"

info "step 1: reset"
reset_state

info "step 2: create shards models_state, orm_query at v=0"
create_shard models_state
create_shard orm_query

info "step 3: proxy simulates extraction — records worker-demo reading models_state"
pr=$(proxy_register worker-demo models_state)
info "register response: $pr"
pr_ok=$(echo "$pr" | jq -r '.ok // false')
pr_registered=$(echo "$pr" | jq -r '.registered // 0')
if [[ "$pr_ok" != "true" || "$pr_registered" != "1" ]]; then
  fail "expected ok=true registered=1, got: $pr"
  FAILED=$((FAILED+1))
fi
ok "proxy register recorded 1 entry"

info "step 4: other-agent advances models_state 0 → 1"
c1=$(commit models_state 0 other-agent '[]')
c1_nv=$(echo "$c1" | jq -r '.new_version // empty')
if [[ "$c1_nv" != "1" ]]; then
  fail "expected other-agent commit to advance to v=1, got: $c1"
  FAILED=$((FAILED+1))
fi
ok "other-agent commit landed (new_version=$c1_nv)"

info "step 5: worker-demo tries to commit orm_query with empty explicit read_set"
info "        expected behaviour: ORI consults DeliveryLog, finds models_state@v=0"
info "        stale against current v=1, returns CrossShardStale (HTTP 409)"
c2=$(commit orm_query 0 worker-demo '[]')
info "response: $c2"
c2_err=$(echo "$c2" | jq -r '.error // empty')

if [[ "$c2_err" == "CrossShardStale" ]]; then
  ok "CASE A passed: commit correctly rejected as CrossShardStale"
  PASSED=$((PASSED+1))
else
  fail "CASE A FAILED: expected CrossShardStale, got: $c2"
  FAILED=$((FAILED+1))
fi

bold "case B (negative control): no proxy-recorded read → commit succeeds"

info "step 1: reset"
reset_state

info "step 2: create shards models_state, orm_query at v=0"
create_shard models_state
create_shard orm_query

info "step 3: [no proxy register — this is the control]"

info "step 4: other-agent advances models_state 0 → 1"
c3=$(commit models_state 0 other-agent '[]')
c3_nv=$(echo "$c3" | jq -r '.new_version // empty')
if [[ "$c3_nv" != "1" ]]; then
  fail "expected other-agent commit to advance to v=1, got: $c3"
  FAILED=$((FAILED+1))
fi
ok "other-agent commit landed (new_version=$c3_nv)"

info "step 5: worker-demo commits orm_query — should SUCCEED because"
info "        worker-demo has no DeliveryLog entry for models_state"
c4=$(commit orm_query 0 worker-demo '[]')
info "response: $c4"
c4_nv=$(echo "$c4" | jq -r '.new_version // empty')
c4_err=$(echo "$c4" | jq -r '.error // empty')

if [[ "$c4_nv" == "1" && -z "$c4_err" ]]; then
  ok "CASE B passed: commit correctly accepted (new_version=$c4_nv)"
  PASSED=$((PASSED+1))
else
  fail "CASE B FAILED: expected successful commit to v=1, got: $c4"
  FAILED=$((FAILED+1))
fi

hr
bold "summary"
info "passed: $PASSED   failed: $FAILED"

if [[ $FAILED -eq 0 ]]; then
  bold " ALL CASES PASSED"
  bold "  The proxy-recorded DeliveryLog entry participates in ORI"
  bold "  cross-shard validation identically to HTTP-recorded entries."
  exit 0
else
  bold " $FAILED CASE(S) FAILED"
  exit 1
fi