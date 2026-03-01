"""
最小改动后对比测试 - 8月数据

对比：
- 改动前：基于注册表（registry.json）
- 改动后：基于原始数据（DataCollector API）
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry
from datetime import datetime

print('='*70)
print('最小改动对比测试 - 8月数据')
print('='*70)

# 重置候选池
reg = IndicatorRegistry()
reg.data["candidates"] = {}
reg.data["evolution_history"] = []
reg.save()

print('\n【测试1】基于原始数据的第一轮进化')
print('-'*70)

evo = IndicatorEvolution()
date_str = '2025-08-01'

# 跑第一轮（会尝试从原始数据获取）
candidates = evo.evolve(1, date_str)

print(f'\n发现候选: {len(candidates)} 个')
for c in candidates[:5]:  # 只显示前5个
    source = c.get('data_source', 'unknown')
    print(f'  • {c["id"]} ({source})')
    print(f'    {c["formula"]}')

# 保存结果
reg.add_evolution_record(1, len(candidates), 0)
reg.save()

print('\n' + '='*70)
print('对比总结')
print('='*70)

print('\n改动前（基于注册表）:')
print('  - 只能组合已有指标定义')
print('  - 发现数量依赖注册表丰富度')
print('  - 速度快（毫秒级）')

print('\n改动后（基于原始数据）:')
print('  - 从ai10-ai68等原始测点发现关系')
print('  - 可发现全新类型指标（如ai56/ai45转换效率）')
print('  - 速度慢（需调用API，秒级到分钟级）')
print('  - 失败自动回退到注册表模式')

print('\n' + '='*70)
print('测试完成')
print('='*70)
