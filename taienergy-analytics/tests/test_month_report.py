#!/usr/bin/env python3
"""
V5.1 一个月数据测试 - 简化版
直接基于已有历史数据生成简报
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import numpy as np
from pathlib import Path

from core.unified_history import UnifiedHistoryStore
from core.aggregation_engine import AggregationEngine

def generate_month_report():
    """基于已存储的历史数据生成简报"""
    print("=" * 70)
    print("V5.1 一个月数据测试简报")
    print("=" * 70)
    
    history = UnifiedHistoryStore()
    aggregation = AggregationEngine(history)
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    # 检查数据覆盖
    print(f"\n【数据覆盖检查】")
    total_records = 0
    for sn in DEVICE_CLUSTER:
        dates = history.get_device_dates(sn)
        total_records += len(dates)
        if sn == 'XHDL_1NBQ':
            print(f"  {sn}: {len(dates)} 天 ({dates[0]} 至 {dates[-1]})")
    
    print(f"  总计: {total_records} 条记录")
    
    # 生成设备画像
    print(f"\n【生成设备画像】")
    profiles = {}
    for sn in DEVICE_CLUSTER:
        # 直接读取所有历史数据
        dates = history.get_device_dates(sn)
        if len(dates) >= 3:
            health_scores = []
            for d in dates:
                path = Path(f'memory/devices/{sn}/daily/{d}.json')
                if path.exists():
                    data = json.loads(path.read_text())
                    health = data.get('metrics', {}).get('health_score')
                    if health:
                        health_scores.append(health)
            
            if health_scores:
                profiles[sn] = {
                    'sn': sn,
                    'health': {
                        'current': health_scores[-1],
                        'avg': round(np.mean(health_scores), 1),
                        'min': round(min(health_scores), 1),
                        'max': round(max(health_scores), 1),
                        'trend': 'degrading' if health_scores[-1] < health_scores[0] - 5 else 'stable'
                    },
                    'data_points': len(health_scores)
                }
                # 保存画像
                history.update_device_profile(sn, profiles[sn])
    
    print(f"  ✅ 生成 {len(profiles)} 台设备画像")
    
    # 生成排名
    print(f"\n【场站排名】")
    health_ranking = sorted(
        [(sn, p['health']['current']) for sn, p in profiles.items()],
        key=lambda x: x[1], reverse=True
    )
    
    top3 = [sn for sn, _ in health_ranking[:3]]
    bottom3 = [sn for sn, _ in health_ranking[-3:]]
    
    print(f"  TOP 3: {', '.join(top3)}")
    print(f"  BOTTOM 3: {', '.join(bottom3)}")
    
    # 发现问题
    print(f"\n【发现规则检测】")
    findings = []
    
    # 健康分下滑
    for sn, p in profiles.items():
        if p['health']['trend'] == 'degrading':
            findings.append({'device': sn, 'issue': '健康分下滑'})
    
    # 持续垫底
    for sn in bottom3:
        findings.append({'device': sn, 'issue': '排名垫底'})
    
    if findings:
        print(f"  ⚠️ 发现 {len(findings)} 个问题:")
        for f in findings[:5]:
            print(f"     - {f['device']}: {f['issue']}")
    else:
        print(f"  ✅ 未发现异常")
    
    # 简报
    print(f"\n{'=' * 70}")
    print("测试简报")
    print(f"{'=' * 70}")
    
    all_health = [p['health']['current'] for p in profiles.values()]
    print(f"\n【整体统计】")
    print(f"  平均健康分: {np.mean(all_health):.1f}")
    print(f"  健康分范围: {min(all_health):.1f} - {max(all_health):.1f}")
    print(f"  标准差: {np.std(all_health):.1f}")
    
    print(f"\n【重点关注】")
    print(f"  健康分下滑: {len([f for f in findings if '下滑' in f['issue']])} 台")
    print(f"  排名垫底: {len([f for f in findings if '垫底' in f['issue']])} 台")
    
    print(f"\n【数据存储】")
    print(f"  设备画像: memory/devices/{{sn}}/profile/latest.json")
    print(f"  历史数据: memory/devices/{{sn}}/daily/*.json (30天)")
    
    print(f"\n{'=' * 70}")
    print("一个月数据测试完成")
    print(f"{'=' * 70}\n")

if __name__ == '__main__':
    generate_month_report()