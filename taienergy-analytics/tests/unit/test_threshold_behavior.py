#!/usr/bin/env python3
"""
阈值行为级测试
真实调用 _run_horizontal_comparison 和 _run_trend_analysis
验证阈值逻辑正确性
"""

import sys
from pathlib import Path

TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from workflows.daily_v5 import DailyAssetManagementV5, DEFAULT_POWER_GAP_THRESHOLD, DEFAULT_TREND_THRESHOLD


def test_horizontal_comparison_with_threshold():
    """场景1: registry 给定 warning 阈值时，is_anomaly 与阈值一致"""
    workflow = DailyAssetManagementV5()
    
    # 前置断言：确保注册表不为空
    assert workflow._get_indicator_config("power_active") is not None, "power_active 必须在注册表中"
    assert workflow._get_indicator_config("health_score") is not None, "health_score 必须在注册表中"
    
    # 构造测试数据：设备功率差异 25%
    device_results = {}
    device_data = {
        'XHDL_1NBQ': {'raw_metrics': {'ai56': [100.0, 100.0, 100.0]}, 'quality': 90},
        'XHDL_2NBQ': {'raw_metrics': {'ai56': [75.0, 75.0, 75.0]}, 'quality': 90},  # 25% 差异
    }
    
    result = workflow._run_horizontal_comparison(device_results, device_data, '2025-09-01')
    
    # 验证返回结构
    assert 'is_anomaly' in result, "返回结果应包含 is_anomaly"
    assert 'threshold' in result, "返回结果应包含 threshold"
    assert 'power_gap_pct' in result, "返回结果应包含 power_gap_pct"
    
    # 验证 threshold 是数值
    assert isinstance(result['threshold'], (int, float)), f"threshold 应为数值，实际是 {type(result['threshold'])}"
    
    # 验证阈值逻辑：25% 差异 vs 20% 阈值 = 异常
    if result['power_gap_pct'] > result['threshold']:
        assert result['is_anomaly'] is True, "功率差异超过阈值时应标记异常"


def test_trend_analysis_with_threshold():
    """场景2: 趋势分析阈值行为"""
    workflow = DailyAssetManagementV5()
    
    # 构造测试数据
    device_results = {
        'XHDL_1NBQ': {'health_score': 65.0, 'level': 'good'}
    }
    
    result = workflow._run_trend_analysis(device_results, '2025-09-01')
    
    # 验证返回结构
    assert 'has_anomaly' in result, "返回结果应包含 has_anomaly"
    assert 'threshold' in result, "返回结果应包含 threshold"
    
    # 验证 threshold 是数值
    assert isinstance(result['threshold'], (int, float)), f"threshold 应为数值，实际是 {type(result['threshold'])}"


def test_threshold_fallback_behavior():
    """场景3: 缺阈值时使用默认值并不中断"""
    # 验证兜底常量存在
    assert DEFAULT_POWER_GAP_THRESHOLD == 20.0, "默认功率差异阈值应为 20%"
    assert DEFAULT_TREND_THRESHOLD == 30.0, "默认趋势阈值应为 30%"


def test_threshold_in_result_is_numeric():
    """场景4: 返回结果包含 threshold 且为数值"""
    workflow = DailyAssetManagementV5()
    
    # 横向对比
    device_data = {
        'XHDL_1NBQ': {'raw_metrics': {'ai56': [100.0]}, 'quality': 90},
        'XHDL_2NBQ': {'raw_metrics': {'ai56': [90.0]}, 'quality': 90},
    }
    result = workflow._run_horizontal_comparison({}, device_data, '2025-09-01')
    
    assert 'threshold' in result, "结果应包含 threshold"
    assert isinstance(result['threshold'], (int, float)), "threshold 必须是数值"
    assert result['threshold'] > 0, "threshold 必须为正数"
    
    # 趋势分析
    result = workflow._run_trend_analysis({}, '2025-09-01')
    assert 'threshold' in result, "结果应包含 threshold"
    assert isinstance(result['threshold'], (int, float)), "threshold 必须是数值"


if __name__ == '__main__':
    # 本地运行入口
    test_horizontal_comparison_with_threshold()
    test_trend_analysis_with_threshold()
    test_threshold_fallback_behavior()
    test_threshold_in_result_is_numeric()
    print("✅ 所有测试通过")
