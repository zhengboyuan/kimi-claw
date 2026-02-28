#!/usr/bin/env python3
"""
V5.1 最小可运行测试
验证 registry 驱动逻辑和报表输出路径
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
import json
from workflows.daily_v5 import DailyAssetManagementV5

def test_registry_loading():
    """测试 registry 加载"""
    print("=" * 60)
    print("Test 1: Registry Loading")
    print("=" * 60)
    
    workflow = DailyAssetManagementV5()
    
    assert workflow.registry.get('version') == 'v5.1', "Registry version mismatch"
    assert len(workflow.indicators) >= 9, f"Expected >=9 indicators, got {len(workflow.indicators)}"
    
    # 检查关键指标
    required = ['power_active', 'health_score', 'power_gap_ratio', 'health_trend_change']
    for ind_id in required:
        assert ind_id in workflow.indicators, f"Missing indicator: {ind_id}"
        print(f"  ✓ {ind_id}: {workflow.indicators[ind_id]['name']}")
    
    print("\n✅ Registry loading PASSED\n")
    return workflow

def test_indicator_config_access(workflow):
    """测试指标配置读取"""
    print("=" * 60)
    print("Test 2: Indicator Config Access")
    print("=" * 60)
    
    # 测试 power_active
    power_config = workflow._get_indicator_config('power_active')
    assert power_config is not None, "power_active not found"
    assert power_config.get('inputs') == ['ai56'], f"Unexpected inputs: {power_config.get('inputs')}"
    print(f"  ✓ power_active.inputs = {power_config['inputs']}")
    
    # 测试 health_score
    health_config = workflow._get_indicator_config('health_score')
    assert health_config is not None, "health_score not found"
    assert 'formula' in health_config, "health_score missing formula"
    print(f"  ✓ health_score.formula = {health_config['formula'][:50]}...")
    
    print("\n✅ Indicator config access PASSED\n")

def test_report_output_paths(workflow):
    """测试报表输出路径"""
    print("=" * 60)
    print("Test 3: Report Output Paths")
    print("=" * 60)
    
    # 清理测试输出
    test_date = "2026-02-28-test"
    station_path = Path(f"memory/reports/daily/station/{test_date}.json")
    inverter_path = Path(f"memory/reports/daily/inverter/XHDL_1NBQ/{test_date}.json")
    
    # 测试写入场站报表
    test_report = {
        'date': test_date,
        'test': True,
        'avg_health_score': 85.5
    }
    
    output_path = workflow._write_station_report(test_date, test_report)
    assert Path(output_path).exists(), f"Station report not created: {output_path}"
    print(f"  ✓ Station report: {output_path}")
    
    # 验证内容
    saved = json.loads(Path(output_path).read_text())
    assert saved.get('version') == 'v5.1', "Report version mismatch"
    assert saved.get('report_type') == 'daily_station', "Report type mismatch"
    print(f"  ✓ Report metadata correct")
    
    # 测试写入逆变器报表
    test_device_data = {'health_score': 85.5, 'level': 'good'}
    test_advice = {'recommendation': {'level': 'normal'}}
    
    output_path2 = workflow._write_inverter_report('XHDL_1NBQ', test_date, test_device_data, test_advice)
    assert Path(output_path2).exists(), f"Inverter report not created: {output_path2}"
    print(f"  ✓ Inverter report: {output_path2}")
    
    # 清理测试文件
    station_path.unlink(missing_ok=True)
    inverter_path.unlink(missing_ok=True)
    
    print("\n✅ Report output paths PASSED\n")

def test_skills_registry():
    """测试 skills_registry.yaml"""
    print("=" * 60)
    print("Test 4: Skills Registry")
    print("=" * 60)
    
    import yaml
    
    registry_path = Path("skills_registry.yaml")
    assert registry_path.exists(), "skills_registry.yaml not found"
    
    with open(registry_path) as f:
        skills = yaml.safe_load(f)
    
    # 统计
    atomic = [s for s in skills if s.get('type') == 'atomic']
    workflows = [s for s in skills if s.get('type') == 'workflow']
    entrypoints = [s for s in skills if s.get('type') == 'entrypoint']
    
    print(f"  Atomic: {len(atomic)}")
    print(f"  Workflow: {len(workflows)}")
    print(f"  Entrypoint: {len(entrypoints)}")
    
    # 检查 daily_v5 相关
    assert any(s.get('id') == 'daily_asset_management_v5' for s in workflows), \
        "daily_asset_management_v5 not registered"
    print(f"  ✓ daily_asset_management_v5 registered")
    
    required_atomic = ['collect_device_data', 'calculate_health_score', 'run_horizontal_comparison']
    for aid in required_atomic:
        assert any(s.get('id') == aid for s in atomic), f"{aid} not registered"
        print(f"  ✓ {aid} registered")
    
    print("\n✅ Skills registry PASSED\n")

def main():
    print("\n" + "=" * 60)
    print("V5.1 Minimum Runnable Test")
    print("=" * 60 + "\n")
    
    try:
        workflow = test_registry_loading()
        test_indicator_config_access(workflow)
        test_report_output_paths(workflow)
        test_skills_registry()
        
        print("=" * 60)
        print("ALL TESTS PASSED ✅")
        print("=" * 60)
        return 0
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}\n")
        return 1
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    exit(main())