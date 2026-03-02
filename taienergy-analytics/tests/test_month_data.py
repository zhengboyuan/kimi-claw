#!/usr/bin/env python3
"""
V5.1 一个月数据测试
模拟30天运行，生成设备画像和发现规则报告
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from workflows.daily_v5 import DailyAssetManagementV5
from core.unified_history import UnifiedHistoryStore

def generate_month_data():
    """生成一个月模拟数据并运行工作流"""
    print("=" * 70)
    print("V5.1 一个月数据测试")
    print("=" * 70)
    
    workflow = DailyAssetManagementV5()
    
    # 生成30天的日期
    dates = [(datetime(2025, 8, 1) + timedelta(days=i)).strftime('%Y-%m-%d') 
             for i in range(30)]
    
    print(f"\n测试期间: {dates[0]} 至 {dates[-1]} (30天)")
    print(f"设备: 16台逆变器")
    
    # 为每台设备生成30天的模拟数据
    all_device_data = {}
    
    for sn in workflow.DEVICE_CLUSTER:
        all_device_data[sn] = []
        
        # 基础健康分（每台设备不同）
        base_health = np.random.uniform(75, 90)
        
        # 模拟退化趋势（部分设备）
        degradation = 0
        if sn in ['XHDL_5NBQ', 'XHDL_12NBQ']:
            degradation = -0.5  # 这两台设备健康分下滑
        
        for i, date in enumerate(dates):
            # 健康分：基础值 + 随机波动 + 退化趋势
            health = base_health + np.random.uniform(-3, 3) + (degradation * i)
            health = max(40, min(100, health))  # 限制在40-100
            
            # 发电量：基于健康分
            base_gen = 4000 + (health - 75) * 50
            generation = base_gen + np.random.uniform(-500, 500)
            
            # 发电时长：8-12小时
            gen_duration = 10 + np.random.uniform(-1, 2)
            
            all_device_data[sn].append({
                'date': date,
                'health_score': round(health, 1),
                'level': 'good' if health > 75 else 'attention' if health > 60 else 'warning',
                'daily_generation': round(generation, 0),
                'generation_duration': round(gen_duration, 1)
            })
    
    print(f"\n【模拟数据生成完成】")
    print(f"  数据点: 16台 × 30天 = 480条记录")
    
    # 存储到统一历史存储
    print(f"\n【存储到统一历史存储】")
    history = UnifiedHistoryStore()
    
    for sn, daily_records in all_device_data.items():
        for record in daily_records:
            history.append_device_daily(sn, record['date'], {
                'health_score': record['health_score'],
                'level': record['level'],
                'daily_generation': record['daily_generation'],
                'generation_duration': record['generation_duration']
            })
    
    print(f"  ✅ 历史数据存储完成")
    
    # 生成设备画像
    print(f"\n【生成设备画像】")
    profiles = {}
    for sn in workflow.DEVICE_CLUSTER:
        profile = workflow.aggregation.generate_device_profile(sn, days=30)
        profiles[sn] = profile
    
    valid_profiles = [p for p in profiles.values() if p.get('status') != 'insufficient_data']
    print(f"  ✅ 生成 {len(valid_profiles)} 台设备画像")
    
    # 生成排名
    print(f"\n【生成场站排名】")
    ranking = workflow.aggregation.generate_station_ranking('2025-08-30', profiles)
    print(f"  ✅ 排名生成完成")
    
    # 运行发现规则
    print(f"\n【运行发现规则检测】")
    findings = []
    
    # 健康分下滑检测
    for sn, profile in profiles.items():
        if profile.get('health', {}).get('trend') == 'degrading':
            findings.append({
                'type': 'health_decline',
                'device': sn,
                'severity': 'warning',
                'message': f'{sn} 健康分趋势下滑'
            })
    
    # 持续垫底检测
    bottom3 = ranking.get('bottom_performers', {}).get('by_health', [])
    for sn in bottom3:
        findings.append({
            'type': 'consistent_bottom',
            'device': sn,
            'severity': 'warning',
            'message': f'{sn} 健康分排名垫底'
        })
    
    print(f"  ⚠️ 发现 {len(findings)} 个问题")
    
    # 生成简报
    print(f"\n{'=' * 70}")
    print("测试简报")
    print(f"{'=' * 70}")
    
    # 1. 整体统计
    all_health = [p['health']['current'] for p in profiles.values() if p.get('health', {}).get('current')]
    all_gen = [p['generation']['avg_daily'] for p in profiles.values() if p.get('generation', {}).get('avg_daily')]
    
    print(f"\n【整体统计】")
    print(f"  平均健康分: {np.mean(all_health):.1f} (标准差: {np.std(all_health):.1f})")
    print(f"  平均日发电量: {np.mean(all_gen):.0f} kWh")
    print(f"  健康分范围: {min(all_health):.1f} - {max(all_health):.1f}")
    
    # 2. 设备画像亮点
    print(f"\n【设备画像】")
    print(f"  TOP 3 健康设备: {', '.join(ranking.get('top_performers', {}).get('by_health', [])[:3])}")
    print(f"  BOTTOM 3 健康设备: {', '.join(ranking.get('bottom_performers', {}).get('by_health', [])[-3:])}")
    
    # 健康分下滑设备
    degrading = [sn for sn, p in profiles.items() if p.get('health', {}).get('trend') == 'degrading']
    if degrading:
        print(f"  健康分下滑设备: {', '.join(degrading)}")
    
    # 3. 发现规则结果
    print(f"\n【发现规则】")
    if findings:
        for f in findings[:5]:
            print(f"  ⚠️ {f['device']}: {f['message']}")
    else:
        print(f"  ✅ 未发现异常")
    
    # 4. 存储路径
    print(f"\n【数据存储】")
    print(f"  设备画像: memory/devices/{{sn}}/profile/latest.json")
    print(f"  历史数据: memory/devices/{{sn}}/daily/")
    print(f"  场站排名: memory/station/ranking/")
    
    print(f"\n{'=' * 70}")
    print("一个月数据测试完成")
    print(f"{'=' * 70}")
    
    return {
        'profiles': profiles,
        'ranking': ranking,
        'findings': findings
    }

if __name__ == '__main__':
    result = generate_month_data()