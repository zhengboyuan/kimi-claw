#!/usr/bin/env python3
"""
V5.1 真实历史数据分析
从8月跑到最新数据（现有health_history.json）
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import numpy as np
from pathlib import Path
from collections import defaultdict

DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]

def analyze_real_data():
    """分析真实历史数据"""
    print("=" * 70)
    print("V5.1 真实历史数据分析")
    print("=" * 70)
    
    # 读取所有设备的健康历史
    all_data = {}
    date_range = {'min': None, 'max': None}
    
    for sn in DEVICE_CLUSTER:
        health_file = Path(f'memory/devices/{sn}/health_history.json')
        if health_file.exists():
            data = json.loads(health_file.read_text())
            all_data[sn] = data
            
            dates = [d.get('date') for d in data if d.get('date')]
            if dates:
                if date_range['min'] is None or min(dates) < date_range['min']:
                    date_range['min'] = min(dates)
                if date_range['max'] is None or max(dates) > date_range['max']:
                    date_range['max'] = max(dates)
    
    print(f"\n【数据覆盖】")
    print(f"  设备数: {len(all_data)}/16")
    print(f"  日期范围: {date_range['min']} 至 {date_range['max']}")
    
    # 统计每台设备的数据量
    print(f"\n【各设备数据量】")
    for sn in sorted(all_data.keys())[:5]:  # 只显示前5台
        count = len(all_data[sn])
        print(f"  {sn}: {count} 条记录")
    print(f"  ...")
    
    # 生成画像
    print(f"\n【设备画像】")
    profiles = {}
    for sn, records in all_data.items():
        health_scores = [r.get('total_score') for r in records if r.get('total_score')]
        if len(health_scores) >= 10:
            profiles[sn] = {
                'health_avg': round(np.mean(health_scores), 1),
                'health_min': round(min(health_scores), 1),
                'health_max': round(max(health_scores), 1),
                'health_std': round(np.std(health_scores), 2),
                'health_first': health_scores[0],
                'health_last': health_scores[-1],
                'trend': health_scores[-1] - health_scores[0],
                'data_points': len(health_scores)
            }
    
    print(f"  ✅ 生成 {len(profiles)} 台设备画像")
    
    # 排名
    print(f"\n【健康分排名】")
    ranking = sorted(profiles.items(), key=lambda x: x[1]['health_last'], reverse=True)
    
    print(f"  TOP 3:")
    for sn, p in ranking[:3]:
        print(f"    {sn}: {p['health_last']:.1f} (平均: {p['health_avg']:.1f})")
    
    print(f"  BOTTOM 3:")
    for sn, p in ranking[-3:]:
        print(f"    {sn}: {p['health_last']:.1f} (平均: {p['health_avg']:.1f})")
    
    # 发现规则
    print(f"\n【发现规则检测】")
    findings = []
    
    # 健康分下滑
    for sn, p in profiles.items():
        if p['trend'] < -10:
            findings.append({'device': sn, 'issue': f'健康分下滑 {p["trend"]:.1f} 分', 'severity': 'warning'})
    
    # 波动过大
    for sn, p in profiles.items():
        if p['health_std'] > 5:
            findings.append({'device': sn, 'issue': f'健康分波动大 (std={p["health_std"]:.2f})', 'severity': 'info'})
    
    if findings:
        print(f"  ⚠️ 发现 {len(findings)} 个问题:")
        for f in findings[:5]:
            print(f"     - {f['device']}: {f['issue']}")
    else:
        print(f"  ✅ 未发现明显异常")
    
    # 简报
    print(f"\n{'=' * 70}")
    print("分析简报")
    print(f"{'=' * 70}")
    
    all_current = [p['health_last'] for p in profiles.values()]
    all_avg = [p['health_avg'] for p in profiles.values()]
    
    print(f"\n【整体统计】")
    print(f"  当前平均健康分: {np.mean(all_current):.1f}")
    print(f"  历史平均健康分: {np.mean(all_avg):.1f}")
    print(f"  健康分范围: {min(all_current):.1f} - {max(all_current):.1f}")
    
    degrading = [sn for sn, p in profiles.items() if p['trend'] < -10]
    improving = [sn for sn, p in profiles.items() if p['trend'] > 10]
    
    print(f"\n【趋势分析】")
    print(f"  健康分下滑 (>10分): {len(degrading)} 台")
    if degrading:
        print(f"    {', '.join(degrading)}")
    print(f"  健康分改善 (>10分): {len(improving)} 台")
    if improving:
        print(f"    {', '.join(improving)}")
    
    print(f"\n{'=' * 70}")
    print("分析完成")
    print(f"{'=' * 70}\n")

if __name__ == '__main__':
    analyze_real_data()