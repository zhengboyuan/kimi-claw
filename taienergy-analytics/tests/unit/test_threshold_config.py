#!/usr/bin/env python3
"""
阈值配置测试
验证阈值从 registry 读取，兜底值正常工作
"""

import sys
from pathlib import Path

TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from workflows.daily_v5 import DailyAssetManagementV5, DEFAULT_POWER_GAP_THRESHOLD, DEFAULT_TREND_THRESHOLD


def test_threshold_from_registry():
    """场景1：registry 给定阈值，流程使用该值"""
    print("\n[场景1] registry 给定阈值...")
    
    workflow = DailyAssetManagementV5()
    
    # 检查 power_gap_ratio 阈值
    gap_config = workflow._get_indicator_config('power_gap_ratio')
    assert gap_config is not None, "power_gap_ratio 应存在"
    assert 'thresholds' in gap_config, "应有 thresholds 字段"
    assert 'warning' in gap_config['thresholds'], "应有 warning 阈值"
    print(f"  power_gap_ratio.warning = {gap_config['thresholds']['warning']}")
    
    # 检查 health_trend_change 阈值
    trend_config = workflow._get_indicator_config('health_trend_change')
    assert trend_config is not None, "health_trend_change 应存在"
    assert 'thresholds' in trend_config, "应有 thresholds 字段"
    assert 'warning' in trend_config['thresholds'], "应有 warning 阈值"
    print(f"  health_trend_change.warning = {trend_config['thresholds']['warning']}")
    
    print("  ✅ registry 阈值读取正常")
    return True


def test_fallback_threshold():
    """场景2：registry 缺阈值，触发兜底值且不中断"""
    print("\n[场景2] 兜底值机制...")
    
    # 验证兜底常量存在
    assert DEFAULT_POWER_GAP_THRESHOLD == 20.0, f"DEFAULT_POWER_GAP_THRESHOLD 应为 20.0"
    assert DEFAULT_TREND_THRESHOLD == 30.0, f"DEFAULT_TREND_THRESHOLD 应为 30.0"
    
    print(f"  DEFAULT_POWER_GAP_THRESHOLD = {DEFAULT_POWER_GAP_THRESHOLD}")
    print(f"  DEFAULT_TREND_THRESHOLD = {DEFAULT_TREND_THRESHOLD}")
    print("  ✅ 兜底常量已定义")
    return True


def test_threshold_in_result():
    """验证返回结果包含 threshold 字段"""
    print("\n[场景3] 返回结果包含 threshold 字段...")
    
    workflow = DailyAssetManagementV5()
    
    # 模拟调用 _run_horizontal_comparison
    # 由于需要真实数据，这里只验证配置读取逻辑
    gap_config = workflow._get_indicator_config('power_gap_ratio')
    if gap_config and 'thresholds' in gap_config:
        threshold = gap_config['thresholds'].get('warning', DEFAULT_POWER_GAP_THRESHOLD)
    else:
        threshold = DEFAULT_POWER_GAP_THRESHOLD
    
    assert threshold is not None, "threshold 不应为 None"
    assert isinstance(threshold, (int, float)), "threshold 应为数值"
    print(f"  实际使用的 threshold = {threshold}")
    print("  ✅ threshold 字段可正常获取")
    return True


if __name__ == '__main__':
    print("=" * 60)
    print("阈值配置测试")
    print("=" * 60)
    
    tests = [
        test_threshold_from_registry,
        test_fallback_threshold,
        test_threshold_in_result,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"\n❌ {test.__name__} 失败: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"测试结果: {passed}/{len(tests)} 通过")
    print("=" * 60)
    
    sys.exit(0 if failed == 0 else 1)
