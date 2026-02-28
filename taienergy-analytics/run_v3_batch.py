#!/usr/bin/env python3
"""
V3.0 分批回放脚本
每30天一批，避免长时间运行问题
"""
import sys
import gc
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from skills.skill_1_data_collector import DataCollector
from workflows.daily_inspection import DailyInspectionWorkflow


def run_batch(start_date: str, end_date: str, batch_num: int):
    """运行一批数据"""
    device_sn = 'XHDL_1NBQ'
    workflow = DailyInspectionWorkflow(device_sn)
    collector = DataCollector(device_sn)
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    day_count = 0
    total_anomalies = 0
    
    print(f'\n{"="*60}')
    print(f'批次 {batch_num}: {start_date} ~ {end_date}')
    print(f'{"="*60}')
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        day_count += 1
        
        print(f'\n[{day_count}] {date_str}')
        
        try:
            daily_data = collector.collect_daily_data(date_str)
            
            if not daily_data:
                print('  无数据')
                current += timedelta(days=1)
                continue
            
            # 运行Stage 4
            result = workflow.run_stage4_composite_evolution(date_str, daily_data)
            
            if result and result.get('diagnosed_anomalies'):
                total_anomalies += len(result['diagnosed_anomalies'])
                print(f'  🚨 发现 {len(result["diagnosed_anomalies"])} 个异常!')
            else:
                print('  ✅ 正常')
                
        except Exception as e:
            print(f'  ❌ 错误: {str(e)[:80]}')
        
        current += timedelta(days=1)
    
    print(f'\n批次 {batch_num} 完成! {day_count} 天, {total_anomalies} 个异常')
    return day_count, total_anomalies


def main():
    """主入口 - 运行单批次"""
    import argparse
    
    parser = argparse.ArgumentParser(description='V3.0分批回放')
    parser.add_argument('--start', required=True, help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--batch', type=int, default=1, help='批次号')
    
    args = parser.parse_args()
    
    days, anomalies = run_batch(args.start, args.end, args.batch)
    
    # 显式释放内存
    gc.collect()
    
    print(f'\n{"="*60}')
    print(f'批次 {args.batch} 最终统计:')
    print(f'  处理天数: {days}')
    print(f'  异常数量: {anomalies}')
    print(f'{"="*60}')


if __name__ == '__main__':
    main()
