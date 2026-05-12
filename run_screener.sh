#!/bin/zsh
# run_screener.sh
#
# Wrapper for local screener runs — syncs CSVs with the repo before and after.
#
# Usage (from the Trading-Screener project root with venv active):
#   source venv/bin/activate
#   ./run_screener.sh

set -euo pipefail

# Load local env vars if .env exists
if [[ -f ".env" ]]; then
    set -a
    source .env
    set +a
fi

# ── Colours for terminal output ──────────────────────────────────────────────
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RESET='\033[0m'

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { print -P "%F{cyan}[screener]%f $1"; }
warn()  { print -P "%F{yellow}[screener] ⚠️  $1%f"; }
ok()    { print -P "%F{green}[screener] ✅ $1%f"; }
fail()  { print -P "%F{red}[screener] ❌ $1%f"; }

# ── 1. Confirm we're inside the project root ──────────────────────────────────
if [[ ! -f "screener.py" ]]; then
    fail "screener.py not found. Run this script from the Trading-Screener project root."
    exit 1
fi

# ── 2. Confirm the venv is active ─────────────────────────────────────────────
# VIRTUAL_ENV is set by 'source venv/bin/activate' — if it's empty, the wrong
# Python will be used and packages like yfinance won't be available.
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    fail "No virtual environment active."
    echo "  Run:  source venv/bin/activate"
    echo "  Then: ./run_screener.sh"
    exit 1
fi

info "Using Python: $(which python) ($(python --version 2>&1))"

# ── 3. Pull latest CSVs from the repo ────────────────────────────────────────
info "Pulling latest data from remote …"
if git pull --rebase 2>&1; then
    ok "Pull complete."
else
    warn "git pull failed — running on local data. Push manually after the run if needed."
    # Don't exit — a pull failure (e.g. no internet) should never block a trading run.
fi

echo ""

# ── 4. Run the screener ───────────────────────────────────────────────────────
info "Starting screener …"
echo ""
python screener.py
SCREENER_EXIT=$?
echo ""

if [[ $SCREENER_EXIT -ne 0 ]]; then
    warn "Screener exited with code $SCREENER_EXIT — CSVs may be incomplete. Skipping commit."
    exit $SCREENER_EXIT
fi

# ── 5. Commit and push any changed CSVs ──────────────────────────────────────
info "Checking for CSV changes to commit …"

git add "*.csv" "wheel_monthly/" "stock_monthly/" 2>/dev/null || true

if git diff --cached --quiet; then
    info "No CSV changes — nothing to commit."
else
    TIMESTAMP=$(date +'%Y-%m-%d')
    git commit -m "screener: manual run $TIMESTAMP"

    info "Pushing to remote …"
    if git push 2>&1; then
        ok "Push complete. Remote is up to date."
    else
        warn "git push failed. Your local CSVs are saved but not synced."
        warn "Run 'git push' manually when you have a connection."
    fi
fi

echo ""
ok "Done."
