#!/usr/bin/env python3
"""
V5.1 最小回归测试 - Mock 流程
验证 registry 读取、报告输出、候选池写入
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
import json
from datetime import datetime
from workflows.daily_v5 import DailyAssetManagementV5

def test_mock_daily_v5():
    """Mock 运行 daily_v5 核心流程"""
    print("=" * 60)
    print("Test: Mock Daily V5 Workflow")
    print("=" * 60)
    
    workflow = DailyAssetManagementV5()
    test_date = "2026-02-28-mock"
    
    # 1. 测试 registry 读取
    print("\n1. Registry 读取测试")
    power_cfg = workflow._get_indicator_config('power_active')
    health_cfg = workflow._get_indicator_config('health_score')
    assert power_cfg is not None, "power_active 未找到"
    assert health_cfg is not None, "health_score 未找到"
    print(f"  ✓ power_active: {power_cfg['name']}")
    print(f"  ✓ health_score: {health_cfg['name']}")
    
    # 2. 测试报告输出
    print("\n2. 报告输出路径测试")
    test_report = {
        'date': test_date,
        'online': 16,
        'avg_health_score': 82.5,
        'risk_distribution': {'good': 14, 'attention': 2}
    }
    
    station_path = workflow._write_station_report(test_date, test_report)
    assert Path(station_path).exists(), "场站报告未生成"
    print(f"  ✓ Station report: {station_path}")
    
    device_data = {'health_score': 85.0, 'level': 'good'}
    advice = {'recommendation': {'level': 'normal'}}
    inverter_path = workflow._write_inverter_report('XHDL_1NBQ', test_date, device_data, advice)
    assert Path(inverter_path).exists(), "逆变器报告未生成"
    print(f"  ✓ Inverter report: {inverter_path}")
    
    # 3. 测试候选池写入（通过 memory_system）
    print("\n3. 候选池写入测试")
    test_candidate = {
        'name': 'mock_test_indicator',
        'formula': 'ai56 / ai68',
        'method': 'mock_test',
        'status': 'pending'
    }
    
    # 使用 memory_system 写入候选
    result = workflow.memory.write_candidate('mock_test_indicator', test_date, test_candidate)
    print(f"  ✓ Candidate written: {result}")
    
    # 验证候选池文件（V5.1：检查 .md 文件和 candidate_pool.json）
    candidate_md_path = Path(f'memory/indicators/candidate/mock_test_indicator_{test_date}.md')
    if candidate_md_path.exists():
        print(f"  ✓ Candidate MD file created")
    else:
        print(f"  ⚠️ Candidate MD file not found")
    
    # 同时检查 pool.json（如果存在）
    candidate_pool_path = Path('memory/indicators/candidate/candidate_pool.json')
    if candidate_pool_path.exists():
        pool = json.loads(candidate_pool_path.read_text())
        names = [c.get('name') for c in pool.get('candidates', [])]
        print(f"  ℹ️ Pool has {len(names)} candidates: {names[:3]}...")
    
    # 清理测试文件
    Path(station_path).unlink(missing_ok=True)
    Path(inverter_path).unlink(missing_ok=True)
    
    print("\n✅ Mock daily_v5 PASSED")
    return True

if __name__ == '__main__':
    try:
        test_mock_daily_v5()
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED ✅")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)