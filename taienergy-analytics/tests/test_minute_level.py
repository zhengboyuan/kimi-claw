#!/usr/bin/env python3
"""
V3.1 实际值+分钟级数据测试
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from skills.skill_1_data_collector import DataCollector
from core.composite_engine_v31 import CompositeIndicatorEngineV31, AnomalyConfig
import pandas as pd


def run_minute_level_test(start_date: str, end_date: str):
    """使用实际值+分钟级数据测试"""
    device_sn = 'XHDL_1NBQ'
    collector = DataCollector(device_sn)
    
    # 1%阈值配置（极敏感）
    config = AnomalyConfig(
        diff_pct_info=0.01,      # 1%即记录
        diff_pct_warning=0.03,   # 3%告警
        diff_pct_critical=0.05,  # 5%严重
        efficiency_info=0.01,    # 1%损耗即记录
        efficiency_warning=0.03, # 3%告警
        efficiency_critical=0.05,# 5%严重
        unbalance_info=0.01,     # 1%不平衡
        unbalance_warning=0.03,  # 3%告警
        unbalance_critical=0.05, # 5%严重
        min_continuous_points=2, # 连续2点即可
        rolling_window=4,        # 4个点 = 1小时（15分钟间隔）
        min_load_ratio=0.20      # 20%负载即判效率
    )
    
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    day_count = 0
    total_anomalies = {'critical': 0, 'warning': 0, 'info': 0}
    
    print(f'\n{"="*60}')
    print(f'V3.1 修复版+1%阈值测试: {start_date} ~ {end_date}')
    print(f'阈值: 离散>1%记录, >3%告警, >5%严重')
    print(f'{"="*60}')
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        day_count += 1
        
        print(f'\n[{day_count}] {date_str}')
        
        try:
            # 获取分钟级数据
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
            
            # 降采样到15分钟（如果数据点太多）
            if len(df_merged) > 200:
                print(f'  原始点数: {len(df_merged)}, 降采样到15分钟...')
                df_merged = df_merged.iloc[::15]  # 每15个点取1个
            
            # 打印数据点数
            print(f'  数据点数: {len(df_merged)} (约{len(df_merged)//4}小时有效数据)')
            
            # 运行V3.1
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
                
                for name, info in list(survivors.items())[:3]:
                    print(f'    - {name}: {info["severity"]}, peak={info["anomaly_peak"]:.3f}')
            else:
                print('  ✅ 正常')
            
        except Exception as e:
            print(f'  ❌ 错误: {str(e)[:60]}')
        
        current += timedelta(days=1)
    
    print(f'\n{"="*60}')
    print(f'分钟级测试完成!')
    print(f'  处理天数: {day_count}')
    print(f'  🔴 P0 Critical: {total_anomalies["critical"]}')
    print(f'  🟡 P1 Warning: {total_anomalies["warning"]}')
    print(f'  🔵 P2 Info: {total_anomalies["info"]}')
    print(f'{"="*60}')


if __name__ == '__main__':
    # 测试2025年7月15日~8月14日（一个月）
    run_minute_level_test('2025-07-15', '2025-08-14')
