"""
跑2025年10月数据测试 - 使用完整版taienergy-analytics
测试记忆文件更新
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_v5 import DailyAssetManagementV5
from datetime import datetime, timedelta
import os

workflow = DailyAssetManagementV5()

print("="*70)
print("2025年10月数据测试 - 完整版16台设备")
print("="*70)

# 清理10月旧数据
memory_base = '/root/.openclaw/workspace/taienergy-analytics/memory'
for f in os.listdir(f'{memory_base}/daily'):
    if f.startswith('2025-10'):
        os.remove(f'{memory_base}/daily/{f}')
print("已清理10月旧数据")

# 跑10月前5天数据
start_date = datetime(2025, 10, 1)
for i in range(5):
    date = start_date + timedelta(days=i)
    date_str = date.strftime('%Y-%m-%d')
    
    print(f"\n{'='*70}")
    print(f"【{date_str}】")
    print('='*70)
    
    try:
        result = workflow.run(date_str)
        print(f"\n结果: {result['online']}/16 在线, 健康分 {result['avg_health_score']:.1f}")
        print(f"横向异常: {result['comparison_anomaly']}, 趋势异常: {result['trend_analysis_anomaly']}, 洞察: {result['insights_count']}")
    except Exception as e:
        print(f"\n错误: {e}")

# 检查记忆文件
print("\n" + "="*70)
print("记忆文件检查")
print("="*70)

# 检查日报
daily_files = [f for f in os.listdir(f'{memory_base}/daily') if f.startswith('2025-10')]
print(f"\n日报文件: {len(daily_files)} 个")
for f in sorted(daily_files):
    print(f"  - {f}")

# 检查设备记忆
device_dirs = [d for d in os.listdir(f'{memory_base}/devices') if os.path.isdir(f'{memory_base}/devices/{d}')]
print(f"\n设备记忆: {len(device_dirs)} 个")
print(f"  设备: {', '.join(sorted(device_dirs))}")

# 检查认知层
cognitive_files = os.listdir(f'{memory_base}/cognitive')
print(f"\n认知层文件: {len(cognitive_files)} 个")
for f in cognitive_files:
    print(f"  - {f}")

# 检查是否有对比洞察
if 'pattern_library.json' in cognitive_files:
    import json
    with open(f'{memory_base}/cognitive/pattern_library.json') as f:
        data = json.load(f)
    patterns = data.get('patterns', [])
    comparison_patterns = [p for p in patterns if p.get('type') == 'comparison_pattern']
    print(f"\n对比洞察: {len(comparison_patterns)} 个")
    for p in comparison_patterns:
        print(f"  - {p['name']}: {p['summary']}")

print("\n" + "="*70)
print("测试完成")
print("="*70)
