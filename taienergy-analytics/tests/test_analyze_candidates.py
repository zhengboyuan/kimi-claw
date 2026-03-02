"""
分析125个候选指标的价值
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_evolution import IndicatorRegistry

reg = IndicatorRegistry()
candidates = reg.get_candidates()

print('='*70)
print('125个候选指标分析')
print('='*70)

# 按类型分组
power_ratios = []  # 功率类比值
voltage_ratios = []  # 电压类比值
current_ratios = []  # 电流类比值
mixed_ratios = []  # 混合类

for cid, c in candidates.items():
    formula = c.get('formula', '')
    if 'ai45' in formula or 'ai56' in formula:  # 功率相关
        power_ratios.append(c)
    elif 'ai4' in formula and 'ai5' not in formula:  # 纯电压
        voltage_ratios.append(c)
    elif 'ai5' in formula and 'ai4' not in formula:  # 纯电流
        current_ratios.append(c)
    else:
        mixed_ratios.append(c)

print(f'\n1. 功率类比值 ({len(power_ratios)} 个) - 最有价值')
print('-'*70)
for c in power_ratios[:10]:
    print(f"  {c['id']}: {c['formula']}")

print(f'\n2. 电压类比值 ({len(voltage_ratios)} 个)')
print('-'*70)
for c in voltage_ratios[:5]:
    print(f"  {c['id']}: {c['formula']}")

print(f'\n3. 电流类比值 ({len(current_ratios)} 个)')
print('-'*70)
for c in current_ratios[:5]:
    print(f"  {c['id']}: {c['formula']}")

print('\n' + '='*70)
print('价值分析')
print('='*70)

print('''
高价值指标（建议保留）:
1. ai45_over_ai56 - 输入/输出功率比 = 转换效率
2. ai56_over_ai45 - 输出功率/输入功率（效率的倒数）
3. ai10+ai12+ai16+ai20 相关组合 - 组串电流关系

中等价值（可选）:
- 同类型比值（电压/电压、电流/电流）- 反映比例关系
- 电压/电流 - 阻抗类指标

低价值（建议过滤）:
- 物理意义不明确的组合
- 量纲不匹配的组合（如温度/功率）
''')
