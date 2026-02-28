"""
跑2025年8月数据测试 - 使用完整版taienergy-analytics
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_v5 import DailyAssetManagementV5
from datetime import datetime, timedelta
import os

workflow = DailyAssetManagementV5()

print("="*70)
print("2025年8月数据测试 - 完整版16台设备")
print("="*70)

# 跑8月前5天数据
start_date = datetime(2025, 8, 1)
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
        import traceback
        traceback.print_exc()

# 检查记忆文件
print("\n" + "="*70)
print("记忆文件检查")
print("="*70)

memory_base = '/root/.openclaw/workspace/taienergy-analytics/memory'

# 检查日报
daily_files = [f for f in os.listdir(f'{memory_base}/daily') if f.startswith('2025-08')]
print(f"\n8月日报文件: {len(daily_files)} 个")
for f in sorted(daily_files):
    print(f"  - {f}")

# 检查认知层是否有洞察
cognitive_path = f'{memory_base}/cognitive/pattern_library.json'
if os.path.exists(cognitive_path):
    import json
    with open(cognitive_path) as f:
        data = json.load(f)
    patterns = data.get('patterns', [])
    comparison_patterns = [p for p in patterns if p.get('type') == 'comparison_pattern']
    print(f"\n对比洞察: {len(comparison_patterns)} 个")
    for p in comparison_patterns:
        print(f"  - {p['name']}: {p.get('summary', '')}")
else:
    print("\n认知层: 无洞察文件")

print("\n" + "="*70)
print("测试完成")
print("="*70)
