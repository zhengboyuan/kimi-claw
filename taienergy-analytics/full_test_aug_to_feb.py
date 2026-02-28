#!/usr/bin/env python3
"""
V4.4 全量测试：2025年8月 ~ 2026年2月
获取足够历史数据，避免使用默认值
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_asset_management import DailyAssetManagementWorkflow

def run_full_test():
    """全量测试"""
    
    print("="*80)
    print("V4.4 全量测试: 2025-08 ~ 2026-02")
    print("="*80)
    
    # 日期范围
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2026, 2, 26)
    
    # 检查已完成的日期
    completed = set()
    daily_path = '/root/.openclaw/workspace/taienergy-analytics/memory/daily'
    if os.path.exists(daily_path):
        for f in os.listdir(daily_path):
            if f.endswith('_report.json'):
                date_str = f.replace('_report.json', '')
                completed.add(date_str)
    
    print(f"已跳过 {len(completed)} 个已完成日期")
    
    # 运行每日资产管理
    daily_workflow = DailyAssetManagementWorkflow()
    
    current = start_date
    success_count = 0
    error_count = 0
    
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        
        # 跳过已完成的
        if date_str in completed:
            current += timedelta(days=1)
            continue
        
        print(f"\n[{date_str}] 每日资产管理...", flush=True)
        try:
            result = daily_workflow.run_daily(date_str)
            print(f"  ✅ 完成: {len(result['device_status'])} 台设备", flush=True)
            success_count += 1
            completed.add(date_str)
        except Exception as e:
            print(f"  ❌ 错误: {e}", flush=True)
            error_count += 1
        
        current += timedelta(days=1)
    
    print("\n" + "="*80)
    print("全量测试完成!")
    print(f"成功: {success_count} 天")
    print(f"错误: {error_count} 天")
    print(f"总计: {len(completed)} 天")
    print("="*80)

if __name__ == '__main__':
    run_full_test()