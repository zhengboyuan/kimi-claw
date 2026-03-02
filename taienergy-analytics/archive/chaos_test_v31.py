#!/usr/bin/env python3
"""
V3.1 混沌测试 (Fault Injection)
人工制造异常，验证雷达是否正常工作
"""
import sys
sys.path.insert(0, '.')

import pandas as pd
import numpy as np
from datetime import datetime
from core.composite_engine_v31 import CompositeIndicatorEngineV31, AnomalyConfig
from skills.skill_1_data_collector import DataCollector


def inject_faults(df_day: pd.DataFrame, fault_type: str) -> pd.DataFrame:
    """
    注入故障数据
    
    Args:
        df_day: 原始数据
        fault_type: 故障类型
    
    Returns:
        注入故障后的数据
    """
    df_fault = df_day.copy()
    
    if fault_type == "pv_shading":
        # 模拟PV3(ai10)被树叶遮挡，电流下降15%
        print("  🔧 注入故障: PV3组串遮挡 (电流下降15%)")
        if 'ai10' in df_fault.columns:
            # 只在白天（功率>1000W）时注入
            mask = df_fault.get('ai45', pd.Series([0]*len(df_fault))) > 1000
            df_fault.loc[mask, 'ai10'] = df_fault.loc[mask, 'ai10'] * 0.85
            
    elif fault_type == "high_temp_derating":
        # 模拟高温降载，转换效率掉到93%
        print("  🔧 注入故障: 高温降载 (效率93%)")
        if 'ai45' in df_fault.columns and 'ai56' in df_fault.columns:
            # 半载以上才注入
            mask = df_fault['ai45'] > 25000  # 50kW的50%
            df_fault.loc[mask, 'ai56'] = df_fault.loc[mask, 'ai45'] * 0.93
            
    elif fault_type == "grid_unbalance":
        # 模拟电网三相不平衡，U相电压偏高5%
        print("  🔧 注入故障: 电网三相不平衡 (U相+5%)")
        if 'ai49' in df_fault.columns:
            df_fault['ai49'] = df_fault['ai49'] * 1.05
            
    elif fault_type == "multi_fault":
        # 多重故障：遮挡 + 降载
        print("  🔧 注入故障: 多重故障 (遮挡+降载)")
        if 'ai10' in df_fault.columns:
            mask = df_fault.get('ai45', pd.Series([0]*len(df_fault))) > 1000
            df_fault.loc[mask, 'ai10'] = df_fault.loc[mask, 'ai10'] * 0.85
        if 'ai45' in df_fault.columns and 'ai56' in df_fault.columns:
            mask = df_fault['ai45'] > 25000
            df_fault.loc[mask, 'ai56'] = df_fault.loc[mask, 'ai45'] * 0.93
    
    return df_fault


def run_chaos_test(date_str: str, fault_type: str):
    """运行混沌测试"""
    print(f'\n{"="*60}')
    print(f'混沌测试: {date_str} | 故障类型: {fault_type}')
    print(f'{"="*60}')
    
    # 获取真实数据
    collector = DataCollector('XHDL_1NBQ')
    daily_data = collector.collect_daily_data(date_str)
    
    # 合并数据
    df_merged = pd.DataFrame()
    for code, data in daily_data.items():
        if not data.empty and 'value' in data.columns:
            df_merged[code] = data['value']
    
    print(f'原始数据: {df_merged.shape}')
    
    # 先测试正常数据
    print('\n--- 测试1: 正常数据 ---')
    config = AnomalyConfig()
    engine_normal = CompositeIndicatorEngineV31(df_merged, config)
    survivors_normal = engine_normal.generate_and_select()
    
    if not survivors_normal:
        print('  ✅ 正常数据无异常（符合预期）')
    else:
        print(f'  ⚠️ 正常数据发现 {len(survivors_normal)} 个异常')
    
    # 注入故障
    print('\n--- 测试2: 注入故障后 ---')
    df_fault = inject_faults(df_merged, fault_type)
    
    # 运行V3.1引擎
    engine_fault = CompositeIndicatorEngineV31(df_fault, config)
    survivors_fault = engine_fault.generate_and_select()
    
    # 验证结果
    print('\n--- 验证结果 ---')
    if survivors_fault:
        print(f'  ✅ 雷达正常工作! 抓到 {len(survivors_fault)} 个异常')
        for name, info in survivors_fault.items():
            severity_emoji = {'critical': '🔴', 'warning': '🟡', 'info': '🔵'}.get(info['severity'], '⚪')
            print(f'    {severity_emoji} {name}')
            print(f'       级别: {info["severity"]}')
            print(f'       峰值: {info["anomaly_peak"]}')
            print(f'       持续: {info["continuous_points"]}个点')
        return True
    else:
        print('  ❌ 雷达失效! 未抓到注入的故障')
        return False


def main():
    """主入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='V3.1混沌测试')
    parser.add_argument('--date', default='2025-08-01', help='测试日期')
    parser.add_argument('--fault', default='multi_fault', 
                       choices=['pv_shading', 'high_temp_derating', 'grid_unbalance', 'multi_fault'],
                       help='故障类型')
    
    args = parser.parse_args()
    
    print('='*60)
    print('V3.1 混沌测试 (Fault Injection)')
    print('='*60)
    
    success = run_chaos_test(args.date, args.fault)
    
    print('\n' + '='*60)
    if success:
        print('✅ 混沌测试通过! V3.1雷达工作正常')
    else:
        print('❌ 混沌测试失败! 需要调优阈值')
    print('='*60)


if __name__ == '__main__':
    main()
