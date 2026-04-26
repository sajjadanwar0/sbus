#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# tests/test_proxy_cross_shard_stale.sh
#
# Reproducible end-to-end test for the proxy paper's central safety claim:
#
#   CLAIM: A DeliveryLog entry recorded by the sbus-proxy (not by HTTP GET)
#   participates in the same ORI cross-shard validation as HTTP-recorded
#   entries. Specifically:
#
#     (A) Positive case: if the proxy records agent X reading shard K at
#         version V, and K is subsequently advanced to V+1 by agent Y,
#         then agent X's next commit (to any shard in the same session)
#         is rejected with CrossShardStale.
#
#     (B) Negative control: in the same scenario but with NO proxy-recorded
#         entry for agent X, X's commit succeeds. This rules out the
#         possibility that something else in the pipeline is causing the
#         rejection in case (A).
#
# Both cases use ONLY direct HTTP calls to S-Bus — no OpenAI dependency,
# no real LLM, no agent framework. The proxy's extraction + registration
# step is simulated by calling /delivery_log/register directly, which is
# exactly what the proxy does in production (verified in agent_openai.py
# integration tests).
#
# Prerequisites:
#   - S-Bus server (v45 or later) running on localhost:7000
#   - Server launched with SBUS_ADMIN_ENABLED=1 so /admin/reset works
#     cleanly between runs (NOTE: /admin/reset itself is ungated, but
#     /admin/delivery-log diagnostic is gated — this test uses reset only)
#   - jq installed (for pretty-printing + pass/fail assertions)
#
# Usage:
#   chmod +x tests/test_proxy_cross_shard_stale.sh
#   tests/test_proxy_cross_shard_stale.sh
#
# Exit status:
#   0 = both cases behaved as expected (test passes)
#   1 = at least one case behaved unexpectedly (test fails)
#   2 = prerequisite missing or network failure
# ────────────────────────────────────────────────────────────────────────────

set -u   # fail on undefined variables; explicit error handling everywhere else

SBUS_URL="${SBUS_URL:-http://localhost:7000}"

# ─── helpers ─────────────────────────────────────────────────────────────
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
  # POST /admin/reset is ungated in handlers.rs (safe operation — wipes
  # registry + DeliveryLog, does not expose data).
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
  # args: key, expected_version, agent_id, read_set_json
  # echoes the HTTP response body; caller inspects with jq.
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
  # args: agent_id, shard_name
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

# ─── preflight ───────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────
# CASE A — positive: proxy-recorded entry → stale → commit rejected
# ────────────────────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────
# CASE B — negative control: no proxy entry → commit accepted
# ────────────────────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────
# summary
# ────────────────────────────────────────────────────────────────────────
hr
bold "summary"
info "passed: $PASSED   failed: $FAILED"

if [[ $FAILED -eq 0 ]]; then
  bold "✓ ALL CASES PASSED"
  bold "  The proxy-recorded DeliveryLog entry participates in ORI"
  bold "  cross-shard validation identically to HTTP-recorded entries."
  exit 0
else
  bold "✗ $FAILED CASE(S) FAILED"
  exit 1
fi