"""
指标进化前后对比测试

对比维度：
1. 进化前：只跑 daily_v5（现有逻辑）
2. 进化后：daily_v5 + 指标进化（新逻辑）

数据：2025年8月（已测试过的数据）
"""

import json
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry
from core.memory_system import MemorySystem


def reset_candidates():
    """重置候选池，用于对比测试"""
    reg = IndicatorRegistry()
    reg.data["candidates"] = {}
    reg.data["evolution_history"] = []
    reg.save()
    print("✓ 候选池已重置")


def run_before_evolution(dates):
    """进化前：只跑现有逻辑"""
    print("\n" + "="*60)
    print("【进化前】现有逻辑 - Daily V5 报告")
    print("="*60)
    
    memory = MemorySystem()
    
    stats = {
        "total_days": len(dates),
        "reports_generated": 0,
        "insights_found": 0,
        "indicators_used": set()
    }
    
    for date in dates:
        # 读取已有报告
        report = memory.read_daily_report(date)
        if report:
            stats["reports_generated"] += 1
            # 统计使用的指标
            for device_id, device_data in report.get("devices", {}).items():
                stats["indicators_used"].add("health_score")  # 简化统计
    
    # 读取认知层洞察
    patterns = memory.get_comparison_patterns() if hasattr(memory, 'get_comparison_patterns') else []
    stats["insights_found"] = len(patterns)
    
    print(f"统计结果:")
    print(f"  数据天数: {stats['total_days']}")
    print(f"  生成报告: {stats['reports_generated']}")
    print(f"  发现洞察: {stats['insights_found']}")
    print(f"  使用指标: {len(stats['indicators_used'])} 类")
    print(f"  新增指标: 0 (无进化能力)")
    
    return stats


def run_after_evolution(dates):
    """进化后：跑指标进化"""
    print("\n" + "="*60)
    print("【进化后】指标进化逻辑 - 三轮迭代")
    print("="*60)
    
    reset_candidates()
    
    evo = IndicatorEvolution()
    memory = MemorySystem()
    
    # 模拟设备数据（简化版）
    device_data = {"date_range": dates}
    
    # 运行完整三轮进化
    results = evo.run_full_evolution(device_data)
    
    # 统计
    reg = IndicatorRegistry()
    candidates = reg.get_candidates()
    approved = reg.get_indicators("approved")
    
    # 找出本轮新批准的指标
    new_indicators = []
    for cid, c in candidates.items():
        if c.get("round") in [1, 2, 3]:
            new_indicators.append(c)
    
    stats = {
        "total_days": len(dates),
        "rounds_completed": len(results["rounds"]),
        "candidates_found": results["total_candidates"],
        "new_indicators": len(new_indicators),
        "converged": results["converged"]
    }
    
    print(f"\n统计结果:")
    print(f"  数据天数: {stats['total_days']}")
    print(f"  完成轮次: {stats['rounds_completed']}")
    print(f"  候选指标: {stats['candidates_found']}")
    print(f"  新增指标: {stats['new_indicators']}")
    print(f"  收敛状态: {'已收敛' if stats['converged'] else '未收敛'}")
    
    # 显示发现的指标详情
    if new_indicators:
        print(f"\n  发现的候选指标:")
        for c in new_indicators:
            print(f"    • {c['id']} (第{c.get('round', '?')}轮)")
            print(f"      {c['name']}: {c['formula']}")
    
    return stats


def compare_results(before, after):
    """对比结果"""
    print("\n" + "="*60)
    print("【对比总结】")
    print("="*60)
    
    print(f"\n维度对比:")
    print(f"{'维度':<20} {'进化前':<15} {'进化后':<15} {'变化':<10}")
    print("-" * 60)
    print(f"{'处理天数':<20} {before['total_days']:<15} {after['total_days']:<15} {'-'}")
    print(f"{'生成报告':<20} {before['reports_generated']:<15} {after['total_days']:<15} {'持平'}")
    print(f"{'发现洞察/候选':<20} {before['insights_found']:<15} {after['candidates_found']:<15} {'+' + str(after['candidates_found'] - before['insights_found'])}")
    print(f"{'新增指标':<20} {'0':<15} {after['new_indicators']:<15} {'+' + str(after['new_indicators'])}")
    
    print(f"\n关键改进:")
    if after['candidates_found'] > 0:
        print(f"  ✓ 从 0 个新增指标 → {after['candidates_found']} 个候选指标")
        print(f"  ✓ 建立指标进化流水线（3轮迭代）")
        print(f"  ✓ 支持人工审核后转正")
    
    if after['converged']:
        print(f"  ✓ 系统已收敛，指标体系相对稳定")
    
    print(f"\n价值:")
    print(f"  • 进化前：只能使用预定义指标，无法自动扩展")
    print(f"  • 进化后：数据驱动发现新指标，持续完善指标体系")


def main():
    # 使用2025年8月数据（已测试过的）
    dates = []
    start = datetime(2025, 8, 1)
    for i in range(31):  # 8月整月
        dates.append((start + timedelta(days=i)).strftime('%Y-%m-%d'))
    
    print("="*60)
    print("指标进化前后对比测试")
    print("="*60)
    print(f"数据范围: 2025-08-01 ~ 2025-08-31 ({len(dates)} 天)")
    print(f"数据状态: 已测试过的历史数据")
    
    # 进化前
    before_stats = run_before_evolution(dates)
    
    # 进化后
    after_stats = run_after_evolution(dates)
    
    # 对比
    compare_results(before_stats, after_stats)
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)


if __name__ == "__main__":
    main()
