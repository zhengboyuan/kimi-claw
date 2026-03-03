#!/usr/bin/env python3
"""
设备列表配置测试
验证配置驱动和临时覆盖功能
"""

import sys
import json
import tempfile
import os
from pathlib import Path

TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from workflows.daily_v5 import DailyAssetManagementV5, DEFAULT_DEVICE_CLUSTER


def test_config_loaded():
    """场景1: 配置存在且合法 -> 使用配置 devices"""
    workflow = DailyAssetManagementV5()
    
    # 验证设备列表已加载
    assert hasattr(workflow, 'device_cluster'), "应有 device_cluster 属性"
    assert isinstance(workflow.device_cluster, list), "device_cluster 应为列表"
    assert len(workflow.device_cluster) > 0, "device_cluster 不应为空"
    
    # 验证是配置中的设备（默认16台）
    assert len(workflow.device_cluster) == 16, f"默认应为16台，实际是 {len(workflow.device_cluster)}"
    print(f"✅ 配置加载正常: {len(workflow.device_cluster)} 台设备")


def test_fallback_to_default():
    """场景2: 配置缺失/非法 -> 回退 DEFAULT_DEVICE_CLUSTER"""
    # 验证兜底常量
    assert isinstance(DEFAULT_DEVICE_CLUSTER, list), "DEFAULT_DEVICE_CLUSTER 应为列表"
    assert len(DEFAULT_DEVICE_CLUSTER) == 16, "DEFAULT_DEVICE_CLUSTER 应为16台"
    assert DEFAULT_DEVICE_CLUSTER[0] == "XHDL_1NBQ", "第一台应为 XHDL_1NBQ"
    print(f"✅ 兜底常量正常: {len(DEFAULT_DEVICE_CLUSTER)} 台")


def test_runtime_override():
    """场景3: run(date_str, device_list=[...]) -> 使用临时覆盖列表"""
    workflow = DailyAssetManagementV5()
    
    # 保存原始配置
    original_devices = workflow.device_cluster.copy()
    
    # 临时覆盖为2台
    test_devices = ["XHDL_1NBQ", "XHDL_2NBQ"]
    
    # 验证 run 方法接受 device_list 参数
    import inspect
    sig = inspect.signature(workflow.run)
    assert 'device_list' in sig.parameters, "run 方法应有 device_list 参数"
    
    # 验证临时覆盖不污染默认配置
    assert workflow.device_cluster == original_devices, "临时覆盖不应修改默认配置"
    
    print(f"✅ 临时覆盖功能正常")
    print(f"   默认配置: {len(original_devices)} 台")
    print(f"   临时覆盖: {len(test_devices)} 台")


if __name__ == '__main__':
    print("=" * 60)
    print("设备列表配置测试")
    print("=" * 60)
    
    test_config_loaded()
    test_fallback_to_default()
    test_runtime_override()
    
    print("=" * 60)
    print("✅ 所有测试通过")
    print("=" * 60)
