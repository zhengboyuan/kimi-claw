#!/bin/bash
# QA Gate - Quality Assurance Script
# Must pass before any code can be committed

set -e

echo "========================================"
echo "Running QA Gate"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

FAILED=0

# Test 1: Python Syntax Check
echo ""
echo "[1/3] Python Syntax Check..."
if python3 -m py_compile taienergy-analytics/workflows/daily_v5.py 2>/dev/null; then
    echo -e "${GREEN}✓${NC} daily_v5.py syntax OK"
else
    echo -e "${RED}✗${NC} daily_v5.py syntax ERROR"
    FAILED=1
fi

if python3 -m py_compile taienergy-analytics/core/memory_system.py 2>/dev/null; then
    echo -e "${GREEN}✓${NC} memory_system.py syntax OK"
else
    echo -e "${RED}✗${NC} memory_system.py syntax ERROR"
    FAILED=1
fi

# Test 2: Import Check
echo ""
echo "[2/3] Import Check..."
cd taienergy-analytics
if python3 -c "from workflows.daily_v5 import DailyAssetManagementV5, run_daily_v5; print('Import OK')" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} daily_v5 imports OK"
else
    echo -e "${RED}✗${NC} daily_v5 import ERROR"
    FAILED=1
fi
cd ..

# Test 3: Basic Smoke Test
echo ""
echo "[3/3] Smoke Test..."
echo "  (Skipping full integration test - requires API access)"
echo -e "${YELLOW}⚠${NC} Smoke test skipped (manual verification required)"

# Summary
echo ""
echo "========================================"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}QA Gate PASSED${NC}"
    echo "========================================"
    exit 0
else
    echo -e "${RED}QA Gate FAILED${NC}"
    echo "========================================"
    echo "Fix errors before committing."
    exit 1
fi
