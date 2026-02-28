"""
生成昨日功率运行曲线图
"""
import sys
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from skills.skill_1_data_collector import DataCollector
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def get_device_power_curve(sn, date_str):
    """获取设备功率曲线数据"""
    try:
        collector = DataCollector(sn)
        data = collector.collect_daily_data(date_str)
        if 'ai56' in data and not data['ai56'].empty:
            df = data['ai56'].copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return (sn, df)
    except Exception as e:
        print(f"{sn} 错误: {e}")
    return (sn, None)

# 昨天日期
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"正在查询 {yesterday} 的功率数据...")

devices = [f'XHDL_{i}NBQ' for i in range(1, 17)]
device_data = {}

# 并发获取数据
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(get_device_power_curve, sn, yesterday): sn for sn in devices}
    for future in as_completed(futures):
        sn, df = future.result()
        if df is not None:
            device_data[sn] = df
            print(f"  ✓ {sn}: {len(df)} 个数据点")

if not device_data:
    print("无数据")
    sys.exit(1)

# 创建图表
fig, axes = plt.subplots(4, 4, figsize=(20, 16))
fig.suptitle(f'昨日功率运行曲线 ({yesterday})\nStation Power Curves', fontsize=16, fontweight='bold')

# 为每台设备绘制子图
for idx, (sn, df) in enumerate(sorted(device_data.items())):
    row = idx // 4
    col = idx % 4
    ax = axes[row, col]
    
    # 绘制功率曲线
    ax.plot(df['timestamp'], df['value'], linewidth=0.8, color='#2E86AB')
    ax.fill_between(df['timestamp'], df['value'], alpha=0.3, color='#2E86AB')
    
    # 设置标题和标签
    avg_power = df['value'].mean()
    max_power = df['value'].max()
    ax.set_title(f'{sn}\nAvg: {avg_power:.1f}kW Max: {max_power:.1f}kW', fontsize=9)
    ax.set_ylabel('Power (kW)', fontsize=8)
    ax.grid(True, alpha=0.3)
    
    # 格式化x轴时间
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=7)

plt.tight_layout()
output_file = f'/root/.openclaw/workspace/power_curve_{yesterday}.png'
plt.savefig(output_file, dpi=150, bbox_inches='tight')
print(f"\n图表已保存: {output_file}")

# 同时生成一个总功率曲线
fig2, ax2 = plt.subplots(figsize=(14, 6))

# 计算总功率（按时间对齐）
all_timestamps = None
for sn, df in device_data.items():
    if all_timestamps is None:
        all_timestamps = df.set_index('timestamp')['value']
    else:
        all_timestamps = all_timestamps.add(df.set_index('timestamp')['value'], fill_value=0)

ax2.plot(all_timestamps.index, all_timestamps.values, linewidth=1.5, color='#E94F37', label='Total Power')
ax2.fill_between(all_timestamps.index, all_timestamps.values, alpha=0.3, color='#E94F37')

ax2.set_title(f'场站总功率曲线 ({yesterday})\nStation Total Power: {all_timestamps.mean():.1f}kW avg, {all_timestamps.sum()/1000:.2f}MWh/day', 
              fontsize=14, fontweight='bold')
ax2.set_xlabel('Time', fontsize=12)
ax2.set_ylabel('Total Power (kW)', fontsize=12)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
ax2.legend()

plt.tight_layout()
output_file2 = f'/root/.openclaw/workspace/power_curve_total_{yesterday}.png'
plt.savefig(output_file2, dpi=150, bbox_inches='tight')
print(f"总功率图表已保存: {output_file2}")

print("\n完成!")
