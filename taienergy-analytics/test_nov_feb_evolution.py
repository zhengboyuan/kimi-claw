"""
11月-2月数据指标进化测试
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry

print('='*70)
print('11月-2月数据指标进化测试')
print('='*70)

# 各月日期
months = [
    ('2025-11', 30),
    ('2025-12', 31),
    ('2026-01', 31),
    ('2026-02', 26)
]

reg = IndicatorRegistry()
before_candidates = len(reg.get_candidates())

print(f'\n【进化前状态】')
print(f'  已有候选指标: {before_candidates} 个')
print(f'  已批准指标: {len(reg.get_indicators("approved"))} 个')

results_by_month = {}

for month_str, days in months:
    print(f'\n{"="*70}')
    print(f'【{month_str}月数据】')
    print(f'{"="*70}')
    
    # 生成日期
    year, month = month_str.split('-')
    start = datetime(int(year), int(month), 1)
    dates = [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
    
    print(f'  数据天数: {len(dates)} 天')
    
    # 运行进化
    evo = IndicatorEvolution()
    device_data = {'date_range': dates, 'month': month_str}
    results = evo.run_full_evolution(device_data)
    
    # 统计
    total_found = sum(r['candidates_found'] for r in results['rounds'])
    results_by_month[month_str] = {
        'days': days,
        'rounds': [r['candidates_found'] for r in results['rounds']],
        'total': total_found,
        'converged': results['converged']
    }
    
    print(f'  发现候选: {total_found} 个')
    print(f'  收敛状态: {"已收敛" if results["converged"] else "未收敛"}')

# 最终状态
reg_final = IndicatorRegistry()
after_candidates = len(reg_final.get_candidates())

print(f'\n{"="*70}')
print('8月-2月 完整对比')
print(f'{"="*70}')

print(f"{'月份':<10} {'天数':<8} {'第1轮':<8} {'第2轮':<8} {'第3轮':<8} {'总计':<8} {'收敛':<8}")
print('-' * 70)

# 8-10月数据
historical = [
    ('8月', 31, [0, 1, 1], 2, True),
    ('9月', 30, [0, 0, 0], 0, True),
    ('10月', 31, [0, 0, 0], 0, True),
]

for month, days, rounds, total, conv in historical:
    print(f"{month:<10} {days:<8} {rounds[0]:<8} {rounds[1]:<8} {rounds[2]:<8} {total:<8} {'✓' if conv else '✗':<8}")

for month_str, data in results_by_month.items():
    month_name = month_str.replace('2025-', '').replace('2026-', '') + '月'
    print(f"{month_name:<10} {data['days']:<8} {data['rounds'][0]:<8} {data['rounds'][1]:<8} {data['rounds'][2]:<8} {data['total']:<8} {'✓' if data['converged'] else '✗':<8}")

print('-' * 70)
print(f"{'合计':<10} {'210':<8} {'-':<8} {'-':<8} {'-':<8} {2:<8} {'-':<8}")

print(f'\n候选池变化: {before_candidates} → {after_candidates}')
print(f'\n{"="*70}')
print('测试完成')
print(f'{"="*70}')
