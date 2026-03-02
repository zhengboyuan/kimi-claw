#!/usr/bin/env python3
"""
V4.0 7-8月测试（约60天）
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from workflows.daily_evolution_v40 import run_daily_evolution_workflow


def run_july_august_test():
    """测试2025年7月15日-8月31日"""
    start_date = '2025-07-15'
    end_date = '2025-08-31'
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    day_count = 0
    total_registered = 0
    total_promoted = 0
    total_retired = 0
    
    print(f'\n{"="*70}')
    print(f'V4.0 7-8月测试: {start_date} ~ {end_date}')
    print(f'{"="*70}')
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        day_count += 1
        
        print(f'\n{"="*70}')
        print(f'[{day_count}] {date_str}')
        print(f'{"="*70}')
        
        try:
            summary = run_daily_evolution_workflow('XHDL_1NBQ', date_str)
            
            if summary:
                total_registered += summary.get('registered', 0)
                total_promoted += summary.get('promoted', 0)
                total_retired += summary.get('retired', 0)
                
        except Exception as e:
            print(f'错误: {str(e)[:60]}')
        
        current += timedelta(days=1)
    
    print(f'\n{"="*70}')
    print(f'7-8月测试完成!')
    print(f'  处理天数: {day_count}')
    print(f'  累计注册: {total_registered}')
    print(f'  累计晋升: {total_promoted}')
    print(f'  累计淘汰: {total_retired}')
    print(f'{"="*70}')


if __name__ == '__main__':
    run_july_august_test()
