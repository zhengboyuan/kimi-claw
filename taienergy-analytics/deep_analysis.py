#!/usr/bin/env python3
"""
深度全面分析入口
分析136个指标的深度特性、相关性、异常
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from skills.skill_1_data_collector import DataCollector
from core.deep_analyzer_v2 import DeepIndicatorAnalyzer
from utils.memory_manager import MemoryManager
from datetime import datetime, timedelta


def run_deep_analysis(device_sn: str, start_date: str, end_date: str):
    """运行深度全面分析"""
    
    print(f"\n{'='*70}")
    print(f"光伏设备深度全面分析")
    print(f"设备: {device_sn}")
    print(f"时间范围: {start_date} ~ {end_date}")
    print(f"{'='*70}\n")
    
    # 初始化
    collector = DataCollector(device_sn)
    analyzer = DeepIndicatorAnalyzer()
    memory = MemoryManager(device_sn)
    
    # 获取所有日期
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    current_dt = start_dt
    dates = []
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    
    print(f"将分析 {len(dates)} 天的数据\n")
    
    # 收集所有数据
    for date_str in dates:
        print(f"获取 {date_str} 的数据...")
        daily_data = collector.collect_daily_data(date_str)
        if daily_data:
            analyzer.add_day_data(date_str, daily_data)
            print(f"  ✓ 获取 {len(daily_data)} 个指标")
        else:
            print(f"  ✗ 无数据")
    
    # 执行深度分析
    results = analyzer.analyze_all()
    
    # 输出分析报告
    print_deep_analysis_report(results)
    
    # 保存到记忆
    save_deep_analysis_to_memory(memory, results)
    
    return results


def print_deep_analysis_report(results: dict):
    """打印深度分析报告"""
    
    print(f"\n{'='*70}")
    print("深度分析报告")
    print(f"{'='*70}\n")
    
    # 汇总
    summary = results.get("summary", {})
    print("【分析汇总】")
    print(f"  分析天数: {summary.get('total_days', 0)}")
    print(f"  指标总数: {summary.get('total_indicators', 0)}")
    print(f"  生成画像: {summary.get('profiles_generated', 0)}")
    print(f"  强相关对: {summary.get('strong_correlations', 0)}")
    print(f"  异常事件: {summary.get('anomalies_detected', 0)}")
    
    # 综合洞察
    print(f"\n{'='*70}")
    print("【综合洞察】")
    print(f"{'='*70}")
    for i, insight in enumerate(results.get("insights", []), 1):
        print(f"{i}. {insight}")
    
    # 指标画像示例（前5个）
    profiles = results.get("indicator_profiles", {})
    if profiles:
        print(f"\n{'='*70}")
        print("【指标画像示例】（前5个）")
        print(f"{'='*70}")
        
        for code, profile in list(profiles.items())[:5]:
            print(f"\n{code}:")
            
            # 基础统计
            basic = profile.get("basic_stats", {})
            print(f"  均值: {basic.get('mean', 0):.3f}, 标准差: {basic.get('std', 0):.3f}")
            print(f"  范围: [{basic.get('min', 0):.3f}, {basic.get('max', 0):.3f}]")
            
            # 分布
            dist = profile.get("distribution", {})
            print(f"  分布: {dist.get('type', 'unknown')}, 零值比例: {dist.get('zero_ratio', 0):.1%}")
            
            # 稳定性
            stability = profile.get("stability", {})
            if stability:
                print(f"  稳定性: {stability.get('stability_level', 'unknown')}")
            
            # 周期性
            periodicity = profile.get("periodicity", {})
            if periodicity.get("has_periodicity"):
                print(f"  周期性: 是，主导周期: {periodicity.get('dominant_periods', [])}")
    
    # 强相关指标对
    correlations = results.get("correlations", {})
    strong_pairs = correlations.get("strong_pairs", [])
    if strong_pairs:
        print(f"\n{'='*70}")
        print("【强相关指标对】（|r| > 0.8）")
        print(f"{'='*70}")
        for pair in strong_pairs:
            print(f"  {pair['indicator1']} ↔ {pair['indicator2']}: r = {pair['correlation']:.3f}")
    
    # 异常事件
    anomalies = results.get("anomalies", [])
    if anomalies:
        print(f"\n{'='*70}")
        print("【异常事件】")
        print(f"{'='*70}")
        for anomaly in anomalies:
            print(f"  [{anomaly['severity']}] {anomaly['indicator']} @ {anomaly['date']}")
            print(f"    {anomaly['description']}")
    else:
        print(f"\n{'='*70}")
        print("【异常事件】")
        print(f"{'='*70}")
        print("  未检测到显著异常")
    
    print(f"\n{'='*70}")
    print("分析完成")
    print(f"{'='*70}\n")


def save_deep_analysis_to_memory(memory: MemoryManager, results: dict):
    """保存深度分析到记忆"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # 保存综合洞察
    insights_content = "\n".join([f"- {i}" for i in results.get("insights", [])])
    memory.update_analysis_memory(
        section="deep_analysis_insights",
        content=f"**分析时间**: {timestamp}\n\n{insights_content}"
    )
    
    # 保存指标画像摘要
    profiles = results.get("indicator_profiles", {})
    pv_indicators = [code for code, p in profiles.items() 
                     if p.get("zero_pattern", {}).get("is_typical_pv", False)]
    
    if pv_indicators:
        memory.update_analysis_memory(
            section="pv_pattern_indicators",
            content=f"识别出 {len(pv_indicators)} 个典型光伏模式指标:\n" + 
                    "\n".join([f"- {code}" for code in pv_indicators[:20]])
        )
    
    # 保存相关性
    correlations = results.get("correlations", {})
    strong_pairs = correlations.get("strong_pairs", [])
    if strong_pairs:
        pairs_content = "\n".join([
            f"- {p['indicator1']} ↔ {p['indicator2']}: r={p['correlation']:.3f}"
            for p in strong_pairs
        ])
        memory.update_analysis_memory(
            section="strong_correlations",
            content=f"**强相关指标对** ({len(strong_pairs)}对):\n{pairs_content}"
        )
    
    print(f"深度分析结果已保存至: {memory.analysis_file}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="深度全面分析")
    parser.add_argument("--device", default="XHDL_1NBQ", help="设备SN")
    parser.add_argument("--start-date", default="2025-02-22", help="开始日期")
    parser.add_argument("--end-date", default="2025-02-24", help="结束日期")
    
    args = parser.parse_args()
    
    run_deep_analysis(args.device, args.start_date, args.end_date)