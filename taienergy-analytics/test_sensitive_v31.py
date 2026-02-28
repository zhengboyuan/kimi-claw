#!/usr/bin/env python3
"""
V3.1 敏感阈值测试（调优版）
降低阈值，提高雷达灵敏度
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from skills.skill_1_data_collector import DataCollector
from core.composite_engine_v31 import CompositeIndicatorEngineV31, AnomalyConfig
import pandas as pd


def run_sensitive_test(start_date: str, end_date: str):
    """使用敏感阈值运行测试"""
    device_sn = 'XHDL_1NBQ'
    collector = DataCollector(device_sn)
    
    # 敏感阈值配置（大幅降低门槛）
    config = AnomalyConfig(
        diff_pct_info=0.03,      # 3%即记录
        diff_pct_warning=0.05,   # 5%告警
        diff_pct_critical=0.10,  # 10%严重
        efficiency_info=0.02,    # 2%损耗即记录
        efficiency_warning=0.04, # 4%告警
        efficiency_critical=0.06,# 6%严重
        unbalance_info=0.02,     # 2%不平衡
        unbalance_warning=0.04,  # 4%告警
        unbalance_critical=0.06, # 6%严重
        min_continuous_points=2, # 连续2点即可
        rolling_window=5,        # 5点窗口更敏感
        min_load_ratio=0.30      # 30%负载即判效率
    )
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    day_count = 0
    total_anomalies = {'critical': 0, 'warning': 0, 'info': 0}
    
    print(f'\n{"="*60}')
    print(f'V3.1 敏感阈值测试: {start_date} ~ {end_date}')
    print(f'阈值: 离散>3%记录, >5%告警, >10%严重')
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
            
            # 合并数据
            df_merged = pd.DataFrame()
            for code, data in daily_data.items():
                if not data.empty and 'value' in data.columns:
                    df_merged[code] = data['value']
            
            if df_merged.empty:
                print('  数据为空')
                current += timedelta(days=1)
                continue
            
            # 运行V3.1敏感版
            engine = CompositeIndicatorEngineV31(df_merged, config)
            survivors = engine.generate_and_select()
            
            if survivors:
                crit = len([s for s in survivors.values() if s['severity'] == 'critical'])
                warn = len([s for s in survivors.values() if s['severity'] == 'warning'])
                info = len([s for s in survivors.values() if s['severity'] == 'info'])
                
                total_anomalies['critical'] += crit
                total_anomalies['warning'] += warn
                total_anomalies['info'] += info
                
                print(f'  🚨 异常: 🔴{crit} 🟡{warn} 🔵{info}')
                
                # 打印前2个异常详情
                for name, info in list(survivors.items())[:2]:
                    print(f'    - {name}: {info["severity"]}, peak={info["anomaly_peak"]:.3f}')
            else:
                print('  ✅ 正常')
            
        except Exception as e:
            print(f'  ❌ 错误: {str(e)[:50]}')
        
        current += timedelta(days=1)
    
    print(f'\n{"="*60}')
    print(f'敏感阈值测试完成!')
    print(f'  处理天数: {day_count}')
    print(f'  🔴 P0 Critical: {total_anomalies["critical"]}')
    print(f'  🟡 P1 Warning: {total_anomalies["warning"]}')
    print(f'  🔵 P2 Info: {total_anomalies["info"]}')
    print(f'{"="*60}')


if __name__ == '__main__':
    # 测试2025年7月15日~8月14日（一个月）
    run_sensitive_test('2025-07-15', '2025-08-14')
