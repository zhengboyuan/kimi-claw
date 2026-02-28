#!/usr/bin/env python3
"""
滚动迭代分析入口
从2025年7月开始，逐日分析，每日发现新规律
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from skills.skill_1_data_collector import DataCollector
from core.rolling_iterator import RollingIterationAnalyzer
from datetime import datetime, timedelta


def run_rolling_iteration(device_sn: str, start_date: str, end_date: str):
    """运行滚动迭代分析"""
    
    print(f"\n{'='*70}")
    print(f"滚动迭代深度分析")
    print(f"设备: {device_sn}")
    print(f"时间范围: {start_date} ~ {end_date}")
    print(f"分析逻辑: Day 1建立认知 → Day 2对比发现 → Day 3验证规律...")
    print(f"{'='*70}\n")
    
    # 初始化
    collector = DataCollector(device_sn)
    iterator = RollingIterationAnalyzer()
    
    # 获取日期列表
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    
    print(f"共 {len(dates)} 天数据，开始逐日迭代分析...\n")
    
    # 逐日分析
    all_results = []
    for date_str in dates:
        print(f"获取 {date_str} 数据...")
        daily_data = collector.collect_daily_data(date_str)
        
        if not daily_data:
            print(f"  ✗ {date_str} 无数据，跳过")
            continue
        
        print(f"  ✓ 获取 {len(daily_data)} 个指标")
        
        # 执行迭代分析
        result = iterator.analyze_day(date_str, daily_data)
        all_results.append(result)
        
        # 显示当日发现
        discoveries = result["new_discoveries"]
        if discoveries:
            finding_count = sum(len(d.get("findings", [])) for d in discoveries)
            print(f"  📊 发现 {finding_count} 个新规律/异常")
        else:
            print(f"  📊 无新发现")
        
        print()
    
    # 生成迭代报告
    print(f"{'='*70}")
    print("生成迭代分析报告...")
    print(f"{'='*70}\n")
    
    report = iterator.generate_iteration_report()
    print(report)
    
    # 保存报告
    report_file = f"memory/rolling_iteration_report_{start_date}_to_{end_date}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n报告已保存: {report_file}")
    
    return all_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="滚动迭代分析")
    parser.add_argument("--device", default="XHDL_1NBQ", help="设备SN")
    parser.add_argument("--start-date", default="2025-07-01", help="开始日期")
    parser.add_argument("--end-date", default="2025-07-07", help="结束日期")
    
    args = parser.parse_args()
    
    run_rolling_iteration(args.device, args.start_date, args.end_date)