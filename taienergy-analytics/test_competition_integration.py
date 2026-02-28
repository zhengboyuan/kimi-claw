#!/usr/bin/env python3
"""
V5.1 竞赛指标集成测试
验证 daily_v5.py 中的竞赛指标计算
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from workflows.daily_v5 import DailyAssetManagementV5

def test_competition_metrics_integration():
    """测试竞赛指标集成"""
    print("=" * 70)
    print("V5.1 竞赛指标集成测试")
    print("=" * 70)
    
    workflow = DailyAssetManagementV5()
    
    # 模拟设备数据（带真实结构）
    import pandas as pd
    import numpy as np
    
    # 创建模拟数据：16台设备，每台有 ai56(功率) 和 ai68(发电量)
    mock_device_data = {}
    for i in range(1, 17):
        sn = f'XHDL_{i}NBQ'
        
        # 模拟一天的数据，5分钟一个点，共288个点
        # 早6点到晚6点发电（12小时 = 144个点）
        timestamps = pd.date_range('2025-08-20 00:00', periods=288, freq='5min')
        
        # 功率数据：夜间为0，白天有功率
        power = np.zeros(288)
        power[72:216] = np.random.uniform(400, 600, 144)  # 白天发电 400-600kW
        
        # 发电量累计
        generation = np.cumsum(power * 5 / 60)  # 5分钟间隔，转换为kWh
        
        mock_device_data[sn] = {
            'raw_data': {
                'ai56': pd.DataFrame({'timestamp': timestamps, 'value': power}),
                'ai68': pd.DataFrame({'timestamp': timestamps, 'value': generation})
            },
            'raw_metrics': {},
            'quality': 85.0
        }
    
    print(f"\n模拟数据: 16台设备，每台288个数据点")
    print(f"发电时段: 06:00 - 18:00 (12小时)")
    
    # 测试竞赛指标计算
    print("\n【竞赛指标计算】")
    competition_metrics = workflow._calculate_competition_metrics(mock_device_data, '2025-08-20')
    
    print("\n结果:")
    for metric_name, metric_data in competition_metrics.items():
        status = '✅' if metric_data.get('computable') else '⚠️'
        value = metric_data.get('value')
        unit = metric_data.get('unit', '')
        note = metric_data.get('note', '')
        
        if value is not None:
            print(f"  {status} {metric_name}: {value} {unit}")
            if note:
                print(f"     {note}")
        else:
            print(f"  {status} {metric_name}: 待计算 - {note}")
    
    # 验证关键指标
    print("\n【验证】")
    
    # 等效利用小时数应该在 3-5 小时之间（模拟了半天发电）
    util_hours = competition_metrics.get('equivalent_utilization_hours', {}).get('value')
    if util_hours:
        if 3 <= util_hours <= 6:
            print(f"✅ 等效利用小时数合理: {util_hours} h (预期 3-6h)")
        else:
            print(f"⚠️ 等效利用小时数异常: {util_hours} h (预期 3-6h)")
    
    # 发电时长应该约 12 小时
    gen_duration = competition_metrics.get('generation_duration', {}).get('value')
    if gen_duration:
        if 10 <= gen_duration <= 14:
            print(f"✅ 发电时长合理: {gen_duration} h (预期 ~12h)")
        else:
            print(f"⚠️ 发电时长异常: {gen_duration} h (预期 ~12h)")
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)

if __name__ == '__main__':
    test_competition_metrics_integration()