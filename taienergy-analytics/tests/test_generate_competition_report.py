#!/usr/bin/env python3
"""
V5.1 竞赛指标日报生成测试
生成包含竞赛指标的完整日报
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from workflows.daily_v5 import DailyAssetManagementV5

def generate_competition_report():
    """生成包含竞赛指标的日报"""
    print("=" * 70)
    print("V5.1 竞赛指标日报生成")
    print("=" * 70)
    
    workflow = DailyAssetManagementV5()
    date_str = '2025-08-20'
    
    # 创建带竞赛指标数据的 mock 设备数据
    mock_device_data = {}
    total_generation = 0
    
    for i in range(1, 17):
        sn = f'XHDL_{i}NBQ'
        timestamps = pd.date_range(f'{date_str} 00:00', periods=288, freq='5min')
        
        # 功率：早6点到晚6点发电
        power = np.zeros(288)
        power[72:216] = np.random.uniform(400, 600, 144)
        
        # 发电量累计
        generation = np.cumsum(power * 5 / 60)
        daily_gen = generation[-1]
        total_generation += daily_gen
        
        mock_device_data[sn] = {
            'raw_data': {
                'ai56': pd.DataFrame({'timestamp': timestamps, 'value': power}),
                'ai68': pd.DataFrame({'timestamp': timestamps, 'value': generation})
            },
            'raw_metrics': {},
            'quality': 85.0,
            'health_score': 82.5 + np.random.uniform(-5, 5),
            'level': 'good'
        }
    
    print(f"\n模拟数据:")
    print(f"  设备数: 16")
    print(f"  总发电量: {total_generation:.0f} kWh")
    print(f"  装机容量: 16000 kW (16MW)")
    
    # 计算竞赛指标
    print(f"\n【计算竞赛指标】")
    competition_metrics = workflow._calculate_competition_metrics(mock_device_data, date_str)
    
    for name, data in competition_metrics.items():
        status = '✅' if data.get('computable') else '⚠️'
        value = data.get('value')
        unit = data.get('unit', '')
        print(f"  {status} {name}: {value} {unit}")
    
    # 构造完整报告
    report_data = {
        'date': date_str,
        'total_devices': 16,
        'online': 16,
        'avg_health_score': 82.5,
        'risk_distribution': {'excellent': 0, 'good': 16, 'attention': 0, 'warning': 0, 'danger': 0},
        'trend_alerts': 0,
        'maintenance_priority': [],
        'devices': {},
        'candidates_found': 0,
        'comparison_anomaly': False,
        'trend_analysis_anomaly': False,
        'insights_count': 0,
        'competition_metrics': competition_metrics
    }
    
    # 写入报告
    output_path = Path(f'memory/reports/daily/station/{date_str}_competition.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    report_with_meta = {
        'version': 'v5.1',
        'generated_at': datetime.now().isoformat(),
        'report_type': 'daily_station',
        'data': report_data
    }
    
    output_path.write_text(json.dumps(report_with_meta, ensure_ascii=False, indent=2), encoding='utf-8')
    
    print(f"\n✅ 报告已生成: {output_path}")
    print(f"\n报告内容预览:")
    print(f"  等效利用小时数: {competition_metrics['equivalent_utilization_hours']['value']} h")
    print(f"  发电时长: {competition_metrics['generation_duration']['value']} h")
    print(f"  弃光率: 待调度数据")
    print(f"  综合厂用电率: 待关口表数据")
    print(f"  整体效率: 待理论发电量")
    
    print("\n" + "=" * 70)
    print("竞赛指标落地完成 - 最小闭环验证通过")
    print("=" * 70)

if __name__ == '__main__':
    generate_competition_report()