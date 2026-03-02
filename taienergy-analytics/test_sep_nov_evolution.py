"""
9-11月数据指标进化测试（优化后规则）
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry

print('='*70)
print('9-11月数据指标进化测试（优化后规则）')
print('='*70)

# 各月日期
months = [
    ('2025-09', 30),
    ('2025-10', 31),
    ('2025-11', 30)
]

reg = IndicatorRegistry()
before = len(reg.get_candidates())

print(f'\n【进化前】候选池: {before} 个')

results = {}

for month_str, days in months:
    print(f'\n{"="*70}')
    print(f'【{month_str}月】')
    print(f'{"="*70}')
    
    # 取每月第一天作为样本
    date_str = f'{month_str}-01'
    
    evo = IndicatorEvolution()
    candidates = evo.evolve(1, date_str)
    
    # 保存
    evo.registry.add_evolution_record(1, len(candidates), 0)
    evo.registry.save()
    
    results[month_str] = len(candidates)
    
    print(f'发现候选: {len(candidates)} 个')
    for c in candidates:
        print(f'  • {c["id"]}: {c["formula"]}')

# 最终统计
reg_final = IndicatorRegistry()
after = len(reg_final.get_candidates())

print(f'\n{"="*70}')
print('8-11月 对比总结（优化后）')
print(f'{"="*70}')

print(f"{'月份':<10} {'天数':<8} {'发现候选':<10} {'说明':<20}")
print('-' * 60)
print(f"{'8月':<10} {'31':<8} {'11':<10} {'建立核心指标体系':<20}")
for month, count in results.items():
    month_name = month.replace('2025-', '') + '月'
    note = '新增' if count > 0 else '无新发现'
    print(f"{month_name:<10} {'30/31':<8} {count:<10} {note:<20}")

print('-' * 60)
print(f'候选池变化: {before} → {after}')

print(f'\n{"="*70}')
print('结论')
print(f'{"="*70}')
print('优化后规则效果:')
print('- 8月: 11个核心指标（建立体系）')
print('- 9-11月: 验证稳定性')
print('- 如9-11月发现0个: 说明11个指标覆盖秋季特征')
print('- 如9-11月有新发现: 说明有季节性差异指标')
print(f'{"="*70}')
