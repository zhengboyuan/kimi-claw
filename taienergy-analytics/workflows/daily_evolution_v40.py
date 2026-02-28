"""
V4.0 每日进化工作流 (Evolution Workflow)

核心流程：
1. 数据获取（实际值+分钟级）
2. 穷举变异，发现候选公式
3. LLM分类学家评审，注册新指标
4. 计算已注册L3指标
5. 生命周期管理（评分、晋升、淘汰）
6. 生成进化日志
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from skills.skill_1_data_collector import DataCollector
from core.composite_engine_v42 import discover_daily_candidates_v42
from core.indicator_taxonomist import review_candidates_with_llm
from core.lifecycle_manager import IndicatorLifecycleManager
import pandas as pd


def run_daily_evolution_workflow(device_sn: str, date_str: str, catalog_path: str = None):
    """
    运行V4.0每日进化工作流
    
    Args:
        device_sn: 设备SN
        date_str: 日期
        catalog_path: 指标目录路径
    """
    print(f'\n{"="*70}')
    print(f'V4.0 每日进化工作流: {device_sn} @ {date_str}')
    print(f'{"="*70}')
    
    # 1. 数据获取
    print('\n[Stage 1] 数据获取...')
    collector = DataCollector(device_sn)
    daily_data = collector.collect_daily_data(date_str)
    
    if not daily_data:
        print('  ❌ 无数据，跳过今日进化')
        return
    
    # 合并数据
    df_merged = pd.DataFrame()
    for code, data in daily_data.items():
        if not data.empty and 'value' in data.columns:
            df_merged[code] = data['value']
    
    if df_merged.empty:
        print('  ❌ 数据为空，跳过今日进化')
        return
    
    # 降采样
    if len(df_merged) > 200:
        df_merged = df_merged.iloc[::15]
    
    print(f'  ✅ 数据点数: {len(df_merged)}')
    
    # 2. 初始化生命周期管理器
    manager = IndicatorLifecycleManager(device_sn, catalog_path)
    
    # 获取已注册L3公式
    l3_formulas = {
        ind['id']: ind['formula'] 
        for ind in manager.catalog['indicators']['L3_Synthesized'].values()
    }
    
    # 3. 穷举变异，发现候选（V4.2扩展版）
    print('\n[Stage 2] 穷举变异，发现候选...')
    result = discover_daily_candidates_v42(df_merged, l3_formulas)
    candidates = result['candidates']
    l3_values = result['l3_values']
    
    # 4. LLM分类学家评审（如果有候选）
    if candidates:
        print('\n[Stage 3] LLM分类学家评审...')
        
        # 获取已注册数量
        existing_count = len(manager.catalog['indicators']['L3_Synthesized'])
        
        context = {
            'date': date_str,
            'device_sn': device_sn,
            'temp_max': df_merged.get('ai61', pd.Series([0])).max(),
            'existing_count': existing_count
        }
        
        review_result = review_candidates_with_llm(candidates, context)
        approved = review_result.get('approved_indicators', [])
        rejected = review_result.get('rejected_candidates', [])
        
        print(f'  ✅ 批准: {len(approved)}个')
        print(f'  ❌ 拒绝: {len(rejected)}个')
        
        # 注册新指标
        for ind in approved:
            manager.register_l3_indicator(
                indicator_id=ind['id'],
                name=ind['name'],
                formula=ind['formula'],
                physical_meaning=ind['physical_meaning'],
                birth_date=date_str
            )
    else:
        print('  ℹ️ 无新候选发现')
    
    # 5. 生命周期管理（评分、晋升、淘汰）
    print('\n[Stage 4] 生命周期管理...')
    manager.run_evolution_cycle(date_str, df_merged)
    
    # 6. 生成进化日志
    print('\n[Stage 5] 生成进化日志...')
    summary = manager.get_evolution_summary()
    
    print(f'\n{"="*70}')
    print(f'V4.0 进化日志: {date_str}')
    print(f'{"="*70}')
    print(f'  📊 当前种群:')
    print(f'     L0候选: {summary["current_l3"]}个')
    print(f'     L2核心: {summary["current_l2"]}个')
    print(f'  📈 累计注册: {summary["registered"]}个')
    print(f'  📈 累计晋升: {summary["promoted"]}个')
    print(f'  💀 累计淘汰: {summary["retired"]}个')
    print(f'  ⭐ L3平均生命值: {summary["l3_avg_score"]:.2f}')
    print(f'{"="*70}')
    
    return summary


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='V4.0每日进化工作流')
    parser.add_argument('--device', default='XHDL_1NBQ', help='设备SN')
    parser.add_argument('--date', help='日期 (YYYY-MM-DD)，默认昨天')
    parser.add_argument('--catalog', default='memory/indicator_catalog_v4.json', help='指标目录')
    
    args = parser.parse_args()
    
    if args.date:
        date_str = args.date
    else:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    run_daily_evolution_workflow(args.device, date_str, args.catalog)
