#!/usr/bin/env python3
"""
V5.1 测试：2025年9月全月数据
验证修改后的系统稳定性
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_v5 import DailyAssetManagementV5, run_daily_v5

def run_september_test():
    """跑2025年9月全月数据"""
    
    print("="*80)
    print("V5.1 测试: 2025年9月全月数据")
    print("="*80)
    
    # 日期范围：2025年9月1日 ~ 9月30日
    start_date = datetime(2025, 9, 1)
    end_date = datetime(2025, 9, 30)
    
    # 检查已完成的日期
    completed = set()
    reports_path = '/root/.openclaw/workspace/taienergy-analytics/memory/reports/daily/station'
    if os.path.exists(reports_path):
        for f in os.listdir(reports_path):
            if f.endswith('.json') and f.startswith('2025-09'):
                date_str = f.replace('.json', '')
                completed.add(date_str)
    
    print(f"已存在 {len(completed)} 个已完成日期")
    
    # 运行每日资产管理
    current = start_date
    success_count = 0
    error_count = 0
    
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        
        # 跳过已完成的
        if date_str in completed:
            print(f"\n[{date_str}] 已存在，跳过")
            current += timedelta(days=1)
            continue
        
        print(f"\n[{date_str}] 每日资产管理...", flush=True)
        try:
            result = run_daily_v5(date_str)
            print(f"  ✅ 完成: {result['online']}/16 台设备在线", flush=True)
            success_count += 1
            completed.add(date_str)
        except Exception as e:
            print(f"  ❌ 错误: {e}", flush=True)
            import traceback
            traceback.print_exc()
            error_count += 1
        
        current += timedelta(days=1)
    
    print("\n" + "="*80)
    print("V5.1 9月测试完成!")
    print(f"成功: {success_count} 天")
    print(f"错误: {error_count} 天")
    print(f"总计: {len(completed)} 天")
    print("="*80)

if __name__ == '__main__':
    run_september_test()
