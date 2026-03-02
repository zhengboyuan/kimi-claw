#!/usr/bin/env python3
"""
V3.1 分批回放脚本（工业级）
使用V3.1引擎，每30天一批
"""
import sys
import gc
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from skills.skill_1_data_collector import DataCollector
from core.composite_engine_v31 import CompositeIndicatorEngineV31, AnomalyConfig
import pandas as pd


def run_batch_v31(start_date: str, end_date: str, batch_num: int):
    """运行V3.1批次"""
    device_sn = 'XHDL_1NBQ'
    collector = DataCollector(device_sn)
    
    # V3.1配置（宽松阈值，确保能抓到异常）
    config = AnomalyConfig(
        diff_pct_info=0.05,
        diff_pct_warning=0.10,
        diff_pct_critical=0.15,
        efficiency_info=0.03,
        efficiency_warning=0.05,
        efficiency_critical=0.08,
        min_continuous_points=3,
        rolling_window=10
    )
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    day_count = 0
    total_anomalies = {'critical': 0, 'warning': 0, 'info': 0}
    daily_results = []
    
    print(f'\n{"="*60}')
    print(f'V3.1 批次 {batch_num}: {start_date} ~ {end_date}')
    print(f'{"="*60}')
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        day_count += 1
        
        print(f'\n[{day_count}] {date_str}')
        
        try:
            daily_data = collector.collect_daily_data(date_str)
            
            if not daily_data:
                print('  无数据')
                daily_results.append({'date': date_str, 'status': 'no_data'})
                current += timedelta(days=1)
                continue
            
            # 合并数据
            df_merged = pd.DataFrame()
            for code, data in daily_data.items():
                if not data.empty and 'value' in data.columns:
                    df_merged[code] = data['value']
            
            if df_merged.empty:
                print('  数据为空')
                daily_results.append({'date': date_str, 'status': 'empty'})
                current += timedelta(days=1)
                continue
            
            # 运行V3.1引擎
            engine = CompositeIndicatorEngineV31(df_merged, config)
            survivors = engine.generate_and_select()
            
            day_result = {'date': date_str, 'status': 'normal', 'anomalies': {}}
            
            if survivors:
                for name, info in survivors.items():
                    severity = info['severity']
                    total_anomalies[severity] += 1
                    if severity not in day_result['anomalies']:
                        day_result['anomalies'][severity] = []
                    day_result['anomalies'][severity].append({
                        'name': name,
                        'peak': info['anomaly_peak'],
                        'points': info['continuous_points']
                    })
                
                # 打印摘要
                crit = len([s for s in survivors.values() if s['severity'] == 'critical'])
                warn = len([s for s in survivors.values() if s['severity'] == 'warning'])
                info = len([s for s in survivors.values() if s['severity'] == 'info'])
                print(f'  🚨 异常: 🔴{crit} 🟡{warn} 🔵{info}')
            else:
                print('  ✅ 正常')
                
            daily_results.append(day_result)
            
        except Exception as e:
            print(f'  ❌ 错误: {str(e)[:60]}')
            daily_results.append({'date': date_str, 'status': 'error', 'msg': str(e)[:60]})
        
        current += timedelta(days=1)
    
    # 批次总结
    print(f'\n{"="*60}')
    print(f'批次 {batch_num} 完成!')
    print(f'  处理天数: {day_count}')
    print(f'  🔴 P0 Critical: {total_anomalies["critical"]}')
    print(f'  🟡 P1 Warning: {total_anomalies["warning"]}')
    print(f'  🔵 P2 Info: {total_anomalies["info"]}')
    print(f'{"="*60}')
    
    # 保存结果
    import json
    result_file = f'v31_batch{batch_num}_result.json'
    with open(result_file, 'w') as f:
        json.dump({
            'batch': batch_num,
            'start_date': start_date,
            'end_date': end_date,
            'total_days': day_count,
            'anomalies': total_anomalies,
            'daily_results': daily_results
        }, f, indent=2)
    print(f'  结果已保存: {result_file}')
    
    return day_count, total_anomalies


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--batch', type=int, required=True)
    args = parser.parse_args()
    
    run_batch_v31(args.start, args.end, args.batch)
    gc.collect()
