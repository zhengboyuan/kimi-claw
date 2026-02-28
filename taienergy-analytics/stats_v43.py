import json
import os

print('='*70)
print('V4.3 3天测试 - 完整统计')
print('='*70)

total_stats = {'L1_Active': 0, 'L2_Core': 0, 'L3_Synthesized': 0, 'L4_Retired': 0}

for i in range(1, 17):
    sn = f'XHDL_{i}NBQ'
    catalog_path = f'memory/indicator_catalog_{sn}.json'
    
    if os.path.exists(catalog_path):
        with open(catalog_path, 'r') as f:
            catalog = json.load(f)
        
        l1 = len(catalog['indicators']['L1_Active'])
        l2 = len(catalog['indicators']['L2_Core'])
        l3 = len(catalog['indicators']['L3_Synthesized'])
        l4 = len(catalog['indicators']['L4_Retired'])
        
        total_stats['L1_Active'] += l1
        total_stats['L2_Core'] += l2
        total_stats['L3_Synthesized'] += l3
        total_stats['L4_Retired'] += l4
        
        if l1 > 0 or l4 > 0:
            print(f'{sn}: L1={l1}, L2={l2}, L4={l4}')

print()
print('='*70)
print('总计统计:')
print(f'  L1_Active (观察期): {total_stats["L1_Active"]}个')
print(f'  L2_Core (核心): {total_stats["L2_Core"]}个')
print(f'  L3_Synthesized (复合): {total_stats["L3_Synthesized"]}个')
print(f'  L4_Retired (淘汰): {total_stats["L4_Retired"]}个')
print(f'  总计: {sum(total_stats.values())}个')
print('='*70)

total_discovered = total_stats['L1_Active'] + total_stats['L2_Core'] + total_stats['L4_Retired']
if total_discovered > 0:
    survival_rate = (total_stats['L1_Active'] + total_stats['L2_Core']) / total_discovered * 100
    print(f'存活率: {survival_rate:.1f}%')
    print(f'淘汰率: {total_stats["L4_Retired"]/total_discovered*100:.1f}%')
