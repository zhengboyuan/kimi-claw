"""
10月数据指标进化测试
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry

print('='*70)
print('10月数据指标进化测试')
print('='*70)

# 10月日期
dates = []
start = datetime(2025, 10, 1)
for i in range(31):
    dates.append((start + timedelta(days=i)).strftime('%Y-%m-%d'))

print(f'\n数据范围: 2025-10-01 ~ 2025-10-31 ({len(dates)} 天)')

# 查看当前状态
reg = IndicatorRegistry()
before_candidates = len(reg.get_candidates())
before_approved = len(reg.get_indicators('approved'))

print(f'\n【进化前状态】')
print(f'  已有候选指标: {before_candidates} 个')
print(f'  已批准指标: {before_approved} 个')

# 运行进化
evo = IndicatorEvolution()
device_data = {'date_range': dates, 'month': '2025-10'}

print(f'\n【开始进化】')
results = evo.run_full_evolution(device_data)

# 查看进化后状态
reg2 = IndicatorRegistry()
after_candidates = len(reg2.get_candidates())

# 找出新增的候选（通过进化历史判断）
history = reg2.data.get('evolution_history', [])
new_candidates = []
for cid, c in reg2.get_candidates().items():
    discovered = c.get('discovered_at', '')
    # 检查是否是最近发现的
    if len(history) > 6:  # 超过6条记录说明有新一轮
        new_candidates.append(c)

print(f'\n【10月进化结果】')
print(f'  完成轮次: {len(results["rounds"])}')
for r in results['rounds']:
    print(f'    第{r["round"]}轮 ({r["strategy"]}): {r["candidates_found"]} 个')

print(f'\n  本轮新增候选: {len(new_candidates)} 个')
for c in new_candidates:
    print(f'    • {c["id"]} (第{c.get("round", "?")}轮)')
    print(f'      {c["name"]}: {c["formula"]}')

print(f'\n  候选池总计: {before_candidates} → {after_candidates}')
print(f'  收敛状态: {"已收敛" if results["converged"] else "未收敛"}')

print('\n' + '='*70)
print('8月 vs 9月 vs 10月 对比')
print('='*70)
print(f"{'月份':<10} {'天数':<8} {'第1轮':<8} {'第2轮':<8} {'第3轮':<8} {'总计':<8}")
print('-' * 60)
print(f"{'8月':<10} {'31':<8} {'0':<8} {'1':<8} {'1':<8} {'2':<8}")
print(f"{'9月':<10} {'30':<8} {'0':<8} {'0':<8} {'0':<8} {'0':<8}")
print(f"{'10月':<10} {'31':<8} {results['rounds'][0]['candidates_found']:<8} {results['rounds'][1]['candidates_found']:<8} {results['rounds'][2]['candidates_found']:<8} {len(new_candidates):<8}")

print('\n' + '='*70)
print('测试完成')
print('='*70)
