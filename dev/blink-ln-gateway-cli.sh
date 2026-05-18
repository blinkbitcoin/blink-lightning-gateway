#!/bin/bash

# blink-lightning-gateway CLI — interactive menu for exercising the
# gateway against the local dev stack brought up by `tilt up`.
#
# Two endpoints in play:
#   * Apollo Router (http://localhost:4455/graphql) — federation entry
#     point. `userLogin` + `me { defaultAccount.wallets }` land on
#     galoy-core; `lnInvoiceCreate` is federated to the gateway
#     subgraph. Use this when you want to exercise the full flow with
#     auth.
#   * Gateway subgraph direct (http://localhost:6691/graphql) — bypasses
#     Apollo + galoy entirely. No auth, takes a raw walletId. Faster
#     loop when you just want to verify `LndApi::add_invoice` is wired.

set +e

# ── Config ────────────────────────────────────────────────────────────
APOLLO_ENDPOINT="${APOLLO_ENDPOINT:-http://localhost:4455/graphql}"
GATEWAY_ENDPOINT="${GATEWAY_ENDPOINT:-http://localhost:6691/graphql}"
DEFAULT_CODE="${DEFAULT_CODE:-000000}"

# Tilt's docker_compose() names the upstream stack after the directory
# of the first compose file — `vendor/blink-quickstart/` — so every
# quickstart container is `blink-quickstart-*-1`, regardless of what
# .envrc says. Hardcoded to match.
QUICKSTART_PROJECT="blink-quickstart"

# Resolve repo root from script location (script lives in dev/).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUICKSTART_BIN="${REPO_ROOT}/vendor/blink-quickstart/bin"

# ── Colors ────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Global session state
AUTH_TOKEN=""
BTC_WALLET_ID=""

# ── Helpers ───────────────────────────────────────────────────────────
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_error()   { echo -e "${RED}✗ $1${NC}"; }
print_info()    { echo -e "${YELLOW}ℹ $1${NC}"; }

random_uuid() {
    if [[ -e /proc/sys/kernel/random/uuid ]]; then
        cat /proc/sys/kernel/random/uuid
    else
        uuidgen | tr 'A-Z' 'a-z'
    fi
}

# exec_graphql <endpoint> <token-or-empty> <query-json-escaped> <variables-json>
exec_graphql() {
    local endpoint=$1
    local token=$2
    local query=$3
    local variables=${4:-"{}"}

    local auth_arg=()
    if [[ -n "$token" ]]; then
        auth_arg=(-H "Authorization: Bearer ${token}")
    fi

    curl -s \
        -X POST \
        "${auth_arg[@]}" \
        -H "Content-Type: application/json" \
        -H "Accept: application/json" \
        -H "X-Idempotency-Key: $(random_uuid)" \
        -d "{\"query\": \"$query\", \"variables\": $variables}" \
        "$endpoint"
}

# escape_for_json_string <multi-line-graphql>
escape_for_json_string() {
    tr '\n' ' ' | sed 's/"/\\"/g'
}

# ── 1. Login (Apollo Router → galoy) ──────────────────────────────────
login() {
    print_header "Login (galoy via Apollo Router)"

    read -rp "Enter phone (e.g., +16505554328): " phone
    if [[ -z "$phone" ]]; then
        print_error "Phone is required"
        return 1
    fi

    print_info "POST $APOLLO_ENDPOINT (userLogin, code=$DEFAULT_CODE)"

    local query='mutation UserLogin($phone: Phone!, $code: OneTimeAuthCode!) { userLogin(input: { phone: $phone, code: $code }) { errors { message } authToken totpRequired } }'
    local variables="{\"phone\": \"$phone\", \"code\": \"$DEFAULT_CODE\"}"

    local response
    response=$(exec_graphql "$APOLLO_ENDPOINT" "" "$query" "$variables")

    local errors
    errors=$(echo "$response" | jq -r '.data.userLogin.errors // [] | length')
    if [[ "$errors" != "0" ]]; then
        print_error "Login failed:"
        echo "$response" | jq '.data.userLogin.errors'
        return 1
    fi

    AUTH_TOKEN=$(echo "$response" | jq -r '.data.userLogin.authToken')
    if [[ -z "$AUTH_TOKEN" || "$AUTH_TOKEN" == "null" ]]; then
        print_error "Login response had no authToken:"
        echo "$response" | jq '.'
        AUTH_TOKEN=""
        return 1
    fi

    print_success "Logged in — token: ${AUTH_TOKEN:0:24}…"
}

# ── 2. Get default BTC wallet ─────────────────────────────────────────
get_default_wallet() {
    print_header "Get Default BTC Wallet"
    if [[ -z "$AUTH_TOKEN" ]]; then
        print_error "Login first (option 1)"
        return 1
    fi

    local query='query Me { me { id defaultAccount { id wallets { id walletCurrency balance } } } }'

    local response
    response=$(exec_graphql "$APOLLO_ENDPOINT" "$AUTH_TOKEN" "$query" "{}")

    if echo "$response" | jq -e '.errors' >/dev/null 2>&1; then
        print_error "GraphQL error:"
        echo "$response" | jq '.errors'
        return 1
    fi

    local user_id account_id
    user_id=$(echo "$response" | jq -r '.data.me.id // empty')
    account_id=$(echo "$response" | jq -r '.data.me.defaultAccount.id // empty')
    BTC_WALLET_ID=$(echo "$response" | jq -r '.data.me.defaultAccount.wallets[] | select(.walletCurrency == "BTC") | .id')
    local btc_balance
    btc_balance=$(echo "$response" | jq -r '.data.me.defaultAccount.wallets[] | select(.walletCurrency == "BTC") | .balance')

    if [[ -z "$BTC_WALLET_ID" || "$BTC_WALLET_ID" == "null" ]]; then
        print_error "No BTC wallet found"
        echo "$response" | jq '.'
        return 1
    fi

    print_success "Default account loaded"
    echo "  User ID:        $user_id"
    echo "  Account ID:     $account_id"
    echo "  BTC Wallet ID:  $BTC_WALLET_ID"
    echo "  BTC Balance:    ${btc_balance:-0} sats"
}

# ── 3. Create LN invoice via Apollo Router (federated, auth) ──────────
create_invoice_via_apollo() {
    print_header "lnInvoiceCreate (Apollo Router — federated)"
    if [[ -z "$AUTH_TOKEN" ]]; then
        print_error "Login first (option 1)"
        return 1
    fi
    if [[ -z "$BTC_WALLET_ID" ]]; then
        print_info "BTC wallet not loaded yet — fetching…"
        get_default_wallet || return 1
    fi

    read -rp "Amount (sats) [default 1000]: " amount
    amount=${amount:-1000}
    read -rp "Memo (optional): " memo

    local memo_json="null"
    if [[ -n "$memo" ]]; then
        memo_json="\"$memo\""
    fi

    local query='mutation LnInvoiceCreate($input: LnInvoiceCreateInput!) { lnInvoiceCreate(input: $input) { errors { message } invoice { paymentHash paymentRequest paymentSecret satoshis } } }'
    local variables
    variables=$(jq -nc --arg w "$BTC_WALLET_ID" --argjson a "$amount" --argjson m "$memo_json" \
        '{input: {walletId: $w, amount: $a, memo: $m}}')

    print_info "POST $APOLLO_ENDPOINT (walletId=$BTC_WALLET_ID, amount=$amount sats)"
    local response
    response=$(exec_graphql "$APOLLO_ENDPOINT" "$AUTH_TOKEN" "$query" "$variables")
    render_invoice_response "$response"
}

# ── 4. Create LN invoice direct to gateway (no auth, faster) ──────────
create_invoice_direct() {
    print_header "lnInvoiceCreate (gateway subgraph — direct, no auth)"

    read -rp "Wallet ID (UUID) [default 11111111-1111-1111-1111-111111111111]: " wallet_id
    wallet_id=${wallet_id:-11111111-1111-1111-1111-111111111111}
    read -rp "Amount (sats) [default 1000]: " amount
    amount=${amount:-1000}
    read -rp "Memo (optional): " memo

    local memo_json="null"
    if [[ -n "$memo" ]]; then
        memo_json="\"$memo\""
    fi

    local query='mutation LnInvoiceCreate($input: LnInvoiceCreateInput!) { lnInvoiceCreate(input: $input) { errors { message } invoice { paymentHash paymentRequest paymentSecret satoshis } } }'
    local variables
    variables=$(jq -nc --arg w "$wallet_id" --argjson a "$amount" --argjson m "$memo_json" \
        '{input: {walletId: $w, amount: $a, memo: $m}}')

    print_info "POST $GATEWAY_ENDPOINT (walletId=$wallet_id, amount=$amount sats)"
    local response
    response=$(exec_graphql "$GATEWAY_ENDPOINT" "" "$query" "$variables")
    render_invoice_response "$response"
}

# ── 5. Pay invoice direct to gateway ──────────────────────────────────
pay_invoice_direct() {
    print_header "lnInvoicePaymentSend (gateway subgraph — direct, no auth)"

    read -rp "Wallet ID (UUID) [default 11111111-1111-1111-1111-111111111111]: " wallet_id
    wallet_id=${wallet_id:-11111111-1111-1111-1111-111111111111}
    read -rp "BOLT11 payment request: " bolt11
    if [[ -z "$bolt11" ]]; then
        print_error "BOLT11 is required"
        return 1
    fi

    local query='mutation LnInvoicePaymentSend($input: LnInvoicePaymentInput!) { lnInvoicePaymentSend(input: $input) { status errors { message } transaction { id } } }'
    local variables
    variables=$(jq -nc --arg w "$wallet_id" --arg pr "$bolt11" \
        '{input: {walletId: $w, paymentRequest: $pr}}')

    print_info "POST $GATEWAY_ENDPOINT (walletId=$wallet_id)"
    local response
    response=$(exec_graphql "$GATEWAY_ENDPOINT" "" "$query" "$variables")

    if echo "$response" | jq -e '.errors' >/dev/null 2>&1; then
        print_error "GraphQL transport errors:"
        echo "$response" | jq '.errors'
        return 1
    fi

    local payload
    payload=$(echo "$response" | jq -r '.data.lnInvoicePaymentSend')
    if [[ "$payload" == "null" ]]; then
        print_error "Empty payload"
        echo "$response" | jq '.'
        return 1
    fi

    local err_count
    err_count=$(echo "$payload" | jq -r '.errors | length')
    if [[ "$err_count" != "0" ]]; then
        print_error "Resolver errors:"
        echo "$payload" | jq '.errors'
        return 1
    fi

    print_success "Payment dispatched"
    echo "$payload" | jq '.'
}

# ── 6. Fee probe direct to gateway ────────────────────────────────────
fee_probe_direct() {
    print_header "lnInvoiceFeeProbe (gateway subgraph — direct, no auth)"

    read -rp "Wallet ID (UUID) [default 11111111-1111-1111-1111-111111111111]: " wallet_id
    wallet_id=${wallet_id:-11111111-1111-1111-1111-111111111111}
    read -rp "BOLT11 payment request: " bolt11
    if [[ -z "$bolt11" ]]; then
        print_error "BOLT11 is required"
        return 1
    fi

    local query='mutation LnInvoiceFeeProbe($input: LnInvoiceFeeProbeInput!) { lnInvoiceFeeProbe(input: $input) { amount errors { message } } }'
    local variables
    variables=$(jq -nc --arg w "$wallet_id" --arg pr "$bolt11" \
        '{input: {walletId: $w, paymentRequest: $pr}}')

    print_info "POST $GATEWAY_ENDPOINT (walletId=$wallet_id)"
    local response
    response=$(exec_graphql "$GATEWAY_ENDPOINT" "" "$query" "$variables")

    if echo "$response" | jq -e '.errors' >/dev/null 2>&1; then
        print_error "GraphQL transport errors:"
        echo "$response" | jq '.errors'
        return 1
    fi

    local payload
    payload=$(echo "$response" | jq -r '.data.lnInvoiceFeeProbe')
    if [[ "$payload" == "null" ]]; then
        print_error "Empty payload"
        echo "$response" | jq '.'
        return 1
    fi

    print_success "Fee probe result:"
    echo "$payload" | jq '.'
}

# Run a quickstart init script against the upstream compose stack.
run_quickstart_init() {
    local script_name=$1
    local script_path="${QUICKSTART_BIN}/${script_name}"

    if [[ ! -f "$script_path" ]]; then
        print_error "Init script not found: $script_path"
        return 1
    fi

    print_info "Running $script_name with COMPOSE_PROJECT_NAME=$QUICKSTART_PROJECT"
    COMPOSE_PROJECT_NAME="$QUICKSTART_PROJECT" bash "$script_path"
}

# ── 8. Init blockchain (mine 200 regtest blocks + create wallets) ────
init_blockchain() {
    print_header "Init Blockchain (mine 200 regtest blocks)"
    print_info "Required before option 4 (lnInvoiceCreate) works."
    print_info "Without it: bitcoind has 0 blocks → LND unsynced → add_invoice hangs."
    echo ""
    run_quickstart_init "init-onchain.sh"
    if [[ $? -eq 0 ]]; then
        print_success "Blockchain initialized"
        docker exec "${QUICKSTART_PROJECT}-lnd1-1" lncli --network regtest getinfo 2>/dev/null \
            | jq '{block_height, synced_to_chain}'
    else
        print_error "init-onchain.sh failed"
    fi
}

# ── 9. Init lightning channel (lnd-outside-1 → lnd1, 10M sats) ───────
init_lightning() {
    print_header "Init Lightning Channel (lnd-outside-1 → lnd1)"
    print_info "Required before options 5 (lnInvoicePaymentSend) and 6 (lnInvoiceFeeProbe)."
    print_info "Opens a 10M-sat channel and mines 3 confirmations."
    echo ""
    run_quickstart_init "init-lightning.sh"
    if [[ $? -eq 0 ]]; then
        print_success "Lightning channel opened"
    else
        print_error "init-lightning.sh failed (often: blockchain not initialized first — try option 8)"
    fi
}

# ── 7. Ping gateway ───────────────────────────────────────────────────
ping_gateway() {
    print_header "Gateway health + ping"

    print_info "GET http://localhost:8080/health/ready"
    local health_code
    health_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health/ready)
    if [[ "$health_code" == "200" ]]; then
        print_success "HTTP health: 200"
    else
        print_error "HTTP health: $health_code"
    fi

    print_info "POST $GATEWAY_ENDPOINT (query { ping })"
    local response
    response=$(exec_graphql "$GATEWAY_ENDPOINT" "" "query { ping }" "{}")
    echo "$response" | jq '.'
}

# Shared invoice-response renderer used by both create paths.
render_invoice_response() {
    local response=$1
    if echo "$response" | jq -e '.errors' >/dev/null 2>&1; then
        print_error "GraphQL transport errors:"
        echo "$response" | jq '.errors'
        return 1
    fi

    local payload
    payload=$(echo "$response" | jq -r '.data.lnInvoiceCreate')
    if [[ "$payload" == "null" ]]; then
        print_error "Empty payload"
        echo "$response" | jq '.'
        return 1
    fi

    local err_count
    err_count=$(echo "$payload" | jq -r '.errors | length')
    if [[ "$err_count" != "0" ]]; then
        print_error "Resolver errors:"
        echo "$payload" | jq '.errors'
        return 1
    fi

    print_success "Invoice created"
    echo "  paymentHash:    $(echo "$payload" | jq -r '.invoice.paymentHash')"
    echo "  satoshis:       $(echo "$payload" | jq -r '.invoice.satoshis')"
    echo "  paymentRequest:"
    echo "    $(echo "$payload" | jq -r '.invoice.paymentRequest')"
}

# ── Menu ──────────────────────────────────────────────────────────────
show_menu() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║      blink-lightning-gateway CLI — Main Menu      ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  ${YELLOW}--- Apollo Router (federated, auth) ---${NC}"
    echo "  1) Login (galoy)"
    echo "  2) Get default BTC wallet"
    echo "  3) lnInvoiceCreate (via Apollo Router)"
    echo ""
    echo -e "  ${YELLOW}--- Gateway subgraph (direct, no auth) ---${NC}"
    echo "  4) lnInvoiceCreate (direct to gateway)"
    echo "  5) lnInvoicePaymentSend (direct to gateway)"
    echo "  6) lnInvoiceFeeProbe (direct to gateway)"
    echo ""
    echo -e "  ${YELLOW}--- Diagnostics ---${NC}"
    echo "  7) Ping gateway + health probe"
    echo ""
    echo -e "  ${YELLOW}--- Stack init (run after 'tilt up') ---${NC}"
    echo "  8) Init blockchain (mine 200 blocks — required for option 4)"
    echo "  9) Init lightning channel (open + fund — required for options 5/6)"
    echo ""
    echo "  0) Exit"
    echo ""

    if [[ -n "$AUTH_TOKEN" ]]; then
        print_success "Logged in (token: ${AUTH_TOKEN:0:20}…)"
    else
        print_info "Not logged in"
    fi
    if [[ -n "$BTC_WALLET_ID" ]]; then
        print_info "BTC wallet: $BTC_WALLET_ID"
    fi
    echo ""
}

main() {
    print_header "blink-lightning-gateway CLI (local dev)"
    print_info "Apollo Router: $APOLLO_ENDPOINT"
    print_info "Gateway:       $GATEWAY_ENDPOINT"

    for cmd in jq curl; do
        if ! command -v "$cmd" >/dev/null; then
            print_error "$cmd is required but not installed"
            exit 1
        fi
    done

    while true; do
        show_menu
        read -rp "Select option (0-9): " choice
        case "$choice" in
            1) login || true ;;
            2) get_default_wallet || true ;;
            3) create_invoice_via_apollo || true ;;
            4) create_invoice_direct || true ;;
            5) pay_invoice_direct || true ;;
            6) fee_probe_direct || true ;;
            7) ping_gateway || true ;;
            8) init_blockchain || true ;;
            9) init_lightning || true ;;
            0) print_info "Goodbye!"; exit 0 ;;
            *) print_error "Invalid option" ;;
        esac
        sleep 1
    done
}

main
