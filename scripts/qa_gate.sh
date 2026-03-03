#!/bin/bash
# QA Gate - Quality Assurance Script
# Must pass before any code can be committed

set -e

# 获取脚本所在目录的绝对路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TAIENERGY_DIR="$PROJECT_DIR/taienergy-analytics"

echo "========================================"
echo "Running QA Gate"
echo "========================================"
echo "Project dir: $PROJECT_DIR"
echo "Taienergy dir: $TAIENERGY_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

FAILED=0

# Test 1: Python Syntax Check
echo ""
echo "[1/4] Python Syntax Check..."
if python3 -m py_compile "$TAIENERGY_DIR/workflows/daily_v5.py" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} daily_v5.py syntax OK"
else
    echo -e "${RED}✗${NC} daily_v5.py syntax ERROR"
    FAILED=1
fi

if python3 -m py_compile "$TAIENERGY_DIR/core/memory_system.py" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} memory_system.py syntax OK"
else
    echo -e "${RED}✗${NC} memory_system.py syntax ERROR"
    FAILED=1
fi

# Test 2: Import Check
echo ""
echo "[2/4] Import Check..."
if python3 -c "import sys; sys.path.insert(0, '$TAIENERGY_DIR'); from workflows.daily_v5 import DailyAssetManagementV5, run_daily_v5; print('Import OK')" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} daily_v5 imports OK"
else
    echo -e "${RED}✗${NC} daily_v5 import ERROR"
    FAILED=1
fi

# Test 3: Unit Tests
echo ""
echo "[3/4] Unit Tests..."
if python3 "$TAIENERGY_DIR/tests/unit/test_data_cleaning.py" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Unit tests passed"
else
    echo -e "${RED}✗${NC} Unit tests failed"
    FAILED=1
fi

# Test 4: Smoke Test
echo ""
echo "[4/4] Smoke Test..."
if python3 -c "import sys; sys.path.insert(0, '$TAIENERGY_DIR'); from workflows.daily_v5 import clean_numeric_values; import pandas as pd; result = clean_numeric_values([1, None, pd.NA, 'abc']); print(f'Smoke test passed: {result}')" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Smoke test passed"
else
    echo -e "${RED}✗${NC} Smoke test failed"
    FAILED=1
fi

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
