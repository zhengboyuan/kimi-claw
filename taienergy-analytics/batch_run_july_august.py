#!/usr/bin/env python3
"""
V4.4 批量运行7-8月数据
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_asset_management import DailyAssetManagementWorkflow
from workflows.weekly_analysis import WeeklyAnalysisWorkflow
from workflows.monthly_review import MonthlyReviewWorkflow

def run_batch():
    """运行7-8月批量数据"""
    
    print("="*80)
    print("V4.4 批量运行: 2025年7-8月")
    print("="*80)
    
    # 日期范围
    start_date = datetime(2025, 7, 1)
    end_date = datetime(2025, 8, 31)
    
    # 运行每日资产管理
    daily_workflow = DailyAssetManagementWorkflow()
    weekly_workflow = WeeklyAnalysisWorkflow()
    
    current = start_date
    week_count = 0
    
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        
        # 每日资产管理
        print(f"\n[{date_str}] 每日资产管理...")
        try:
            result = daily_workflow.run_daily(date_str)
            print(f"  完成: {len(result['device_status'])} 台设备")
        except Exception as e:
            print(f"  错误: {e}")
        
        # 周日运行周度分析
        if current.weekday() == 6:  # 周日
            week_count += 1
            print(f"\n[{date_str}] 周度分析 (第{week_count}周)...")
            try:
                weekly_result = weekly_workflow.run_weekly(date_str)
                print(f"  退化趋势: {len(weekly_result['degradation_trends'])} 台")
                print(f"  新异常模式: {len(weekly_result['new_anomaly_patterns'])} 种")
                print(f"  候选指标: {len(weekly_result['candidate_indicators'])} 个")
            except Exception as e:
                print(f"  错误: {e}")
        
        # 每月1日运行月度评审
        if current.day == 1:
            month_str = current.strftime('%Y-%m-%d')
            print(f"\n[{month_str}] 月度评审...")
            try:
                from workflows.monthly_review import run_v44_monthly_review
                monthly_result = run_v44_monthly_review(month_str)
                print(f"  候选入库: {monthly_result['candidate_review'].get('promoted', 0)} 个")
                print(f"  临时转正: {monthly_result['temp_indicator_review'].get('approved', 0)} 个")
            except Exception as e:
                print(f"  错误: {e}")
        
        current += timedelta(days=1)
    
    print("\n" + "="*80)
    print("批量运行完成!")
    print("="*80)

if __name__ == '__main__':
    run_batch()