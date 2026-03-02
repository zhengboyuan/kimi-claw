"""
V4.3 快速测试 - 验证每日新发现
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from core.composite_engine_v43 import discover_daily_candidates_v43
from skills.skill_1_data_collector import DataCollector
import pandas as pd

print('='*70)
print('V4.3 每日新发现测试')
print('='*70)

dates = ['2025-07-15', '2025-07-16', '2025-07-17']
sn = 'XHDL_1NBQ'

for day_idx, date_str in enumerate(dates):
    print(f'\n{"="*70}')
    print(f'Day {day_idx+1}: {date_str}')
    print(f'{"="*70}')
    
    # 获取数据
    collector = DataCollector(sn)
    daily_data = collector.collect_daily_data(date_str)
    
    df = {code: data['value'] for code, data in daily_data.items() 
          if not data.empty and 'value' in data.columns}
    df_day = pd.DataFrame(df)
    
    # V4.3发现（传入day_index）
    result = discover_daily_candidates_v43(df_day, day_idx)
    
    print(f'\n结果:')
    print(f'  阈值: {0.003 if day_idx==0 else (0.002 if day_idx==1 else 0.001)}')
    print(f'  发现候选: {len(result["candidates"])}个')
    
    # 显示前3个候选
    for i, (name, info) in enumerate(list(result['candidates'].items())[:3]):
        print(f'  - {name}: {info["feature"]}')
    if len(result['candidates']) > 3:
        print(f'  ... 还有{len(result["candidates"])-3}个')

print(f'\n{"="*70}')
print('测试完成!')
print('='*70)
