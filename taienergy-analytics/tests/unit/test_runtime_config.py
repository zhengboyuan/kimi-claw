#!/usr/bin/env python3
"""
运行时配置测试
验证并发数、默认日期、阈值兜底可从配置调整
"""

import sys
from pathlib import Path
from datetime import datetime

TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from workflows.daily_v5 import MAX_WORKERS, DEFAULT_POWER_GAP_THRESHOLD, DEFAULT_TREND_THRESHOLD
from config.system_config import RUNTIME_CONFIG, THRESHOLD_CONFIG


def test_max_workers_config():
    """场景1: max_workers 读取生效"""
    # 验证从配置读取
    assert "max_workers" in RUNTIME_CONFIG, "RUNTIME_CONFIG 应有 max_workers"
    assert MAX_WORKERS == RUNTIME_CONFIG["max_workers"], "代码中应使用配置值"
    assert MAX_WORKERS > 0, "max_workers 必须为正数"
    print(f"✅ max_workers 配置: {MAX_WORKERS}")


def test_default_date_not_fixed():
    """场景2: 默认日期不是固定历史日期"""
    # 验证默认日期是动态计算的
    # 检查源码文件内容
    daily_v5_path = TAIENERGY_DIR / "workflows" / "daily_v5.py"
    with open(daily_v5_path, 'r') as f:
        source = f.read()
    
    # 不应包含固定历史日期如 '2025-08-15'
    assert '2025-08-15' not in source, "不应使用固定历史日期"
    
    # 验证使用 datetime.now()
    assert 'datetime.now()' in source, "应使用动态日期"
    
    print(f"✅ 默认日期动态化: 使用 datetime.now()")


def test_threshold_fallback_from_config():
    """场景3: 阈值兜底可从配置改变"""
    # 验证阈值从 THRESHOLD_CONFIG 读取
    expected_power_gap = THRESHOLD_CONFIG.get("power_gap_warning", 20.0)
    expected_trend = THRESHOLD_CONFIG.get("trend_warning", 30.0)
    
    assert DEFAULT_POWER_GAP_THRESHOLD == expected_power_gap, f"功率阈值应为 {expected_power_gap}"
    assert DEFAULT_TREND_THRESHOLD == expected_trend, f"趋势阈值应为 {expected_trend}"
    
    print(f"✅ 阈值兜底配置: power_gap={DEFAULT_POWER_GAP_THRESHOLD}, trend={DEFAULT_TREND_THRESHOLD}")


if __name__ == '__main__':
    print("=" * 60)
    print("运行时配置测试")
    print("=" * 60)
    
    test_max_workers_config()
    test_default_date_not_fixed()
    test_threshold_fallback_from_config()
    
    print("=" * 60)
    print("✅ 所有测试通过")
    print("=" * 60)
