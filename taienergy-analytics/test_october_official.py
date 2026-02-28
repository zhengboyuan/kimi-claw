"""
正式版 skill 跑2025年10月数据 - 输出完整日志
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_v5 import DailyAssetManagementV5
from datetime import datetime, timedelta
import os

workflow = DailyAssetManagementV5()

print("="*70)
print("正式版 skill - 2025年10月数据测试")
print("="*70)

# 跑10月前3天数据
start_date = datetime(2025, 10, 1)
for i in range(3):
    date = start_date + timedelta(days=i)
    date_str = date.strftime('%Y-%m-%d')
    
    print(f"\n{'='*70}")
    print(f"【{date_str}】")
    print('='*70)
    
    try:
        result = workflow.run(date_str)
        print(f"\n【结果】")
        print(f"  在线设备: {result['online']}/16")
        print(f"  平均健康分: {result['avg_health_score']:.1f}")
        print(f"  横向对比异常: {result['comparison_anomaly']}")
        print(f"  趋势分析异常: {result['trend_analysis_anomaly']}")
        print(f"  生成洞察: {result['insights_count']}")
    except Exception as e:
        print(f"\n【错误】")
        print(f"  {e}")
        import traceback
        traceback.print_exc()

# 检查记忆文件
print("\n" + "="*70)
print("【记忆文件检查】")
print("="*70)

memory_base = '/root/.openclaw/workspace/taienergy-analytics/memory'

# 检查日报
daily_files = [f for f in os.listdir(f'{memory_base}/daily') if f.startswith('2025-10')]
print(f"\n日报文件: {len(daily_files)} 个")
for f in sorted(daily_files):
    print(f"  - {f}")

# 检查设备记忆
device_dirs = [d for d in os.listdir(f'{memory_base}/devices') if os.path.isdir(f'{memory_base}/devices/{d}')]
print(f"\n设备记忆: {len(device_dirs)} 个")

# 检查认知层洞察
cognitive_path = f'{memory_base}/cognitive/pattern_library.json'
if os.path.exists(cognitive_path):
    import json
    with open(cognitive_path) as f:
        data = json.load(f)
    patterns = data.get('patterns', [])
    comparison_patterns = [p for p in patterns if p.get('type') == 'comparison_pattern']
    print(f"\n对比洞察: {len(comparison_patterns)} 个")
    for p in comparison_patterns:
        print(f"  - {p['name']}")
        print(f"    摘要: {p.get('summary', '')}")
        print(f"    设备: {p.get('devices_involved', [])}")
        print(f"    验证次数: {p.get('verified_count', 0)}")
else:
    print("\n认知层: 无洞察文件")

print("\n" + "="*70)
print("【测试完成】")
print("="*70)
