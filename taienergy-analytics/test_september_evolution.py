"""
9月数据指标进化对比测试

对比：
1. 8月进化结果（已有2个候选指标）
2. 9月新跑进化（看是否有新发现）
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry


def run_september_evolution():
    """跑9月数据进化"""
    print("="*70)
    print("9月数据指标进化测试")
    print("="*70)
    
    # 9月日期
    dates = []
    start = datetime(2025, 9, 1)
    for i in range(30):
        dates.append((start + timedelta(days=i)).strftime('%Y-%m-%d'))
    
    print(f"\n数据范围: 2025-09-01 ~ 2025-09-30 ({len(dates)} 天)")
    
    # 查看当前候选池状态（保留8月结果）
    reg = IndicatorRegistry()
    before_candidates = len(reg.get_candidates())
    before_approved = len(reg.get_indicators('approved'))
    
    print(f"\n【进化前状态】")
    print(f"  已有候选指标: {before_candidates} 个")
    print(f"  已批准指标: {before_approved} 个")
    
    # 运行进化（不重置，看是否会新增）
    evo = IndicatorEvolution()
    device_data = {"date_range": dates, "month": "2025-09"}
    
    print(f"\n【开始进化】")
    results = evo.run_full_evolution(device_data)
    
    # 查看进化后状态
    reg2 = IndicatorRegistry()
    after_candidates = len(reg2.get_candidates())
    after_approved = len(reg2.get_indicators('approved'))
    
    # 找出新增的候选
    new_candidates = []
    for cid, c in reg2.get_candidates().items():
        if c.get("discovered_at", "").startswith("2026-03-01"):
            new_candidates.append(c)
    
    print(f"\n【9月进化结果】")
    print(f"  完成轮次: {len(results['rounds'])}")
    for r in results['rounds']:
        print(f"    第{r['round']}轮 ({r['strategy']}): {r['candidates_found']} 个")
    
    print(f"\n  本轮新增候选: {len(new_candidates)} 个")
    for c in new_candidates:
        print(f"    • {c['id']} (第{c.get('round', '?')}轮)")
        print(f"      {c['name']}: {c['formula']}")
    
    print(f"\n  候选池总计: {before_candidates} → {after_candidates}")
    print(f"  收敛状态: {'已收敛' if results['converged'] else '未收敛'}")
    
    return {
        "before_candidates": before_candidates,
        "after_candidates": after_candidates,
        "new_candidates": len(new_candidates),
        "converged": results['converged'],
        "new_list": new_candidates
    }


def compare_august_september():
    """对比8月和9月进化结果"""
    print("\n" + "="*70)
    print("8月 vs 9月 进化对比")
    print("="*70)
    
    reg = IndicatorRegistry()
    history = reg.data.get("evolution_history", [])
    
    # 统计各月发现
    august_found = 2  # 已知
    september_found = 0  # 待统计
    
    print(f"\n{'月份':<10} {'数据天数':<10} {'发现候选':<10} {'收敛状态':<10}")
    print("-" * 50)
    print(f"{'8月':<10} {'31':<10} {august_found:<10} {'已收敛':<10}")
    print(f"{'9月':<10} {'30':<10} {september_found:<10} {'待测试':<10}")
    
    print(f"\n结论预测:")
    print(f"  • 如果9月发现0个：说明指标体系已相对稳定")
    print(f"  • 如果9月发现1-2个：说明月度数据有差异特征")
    print(f"  • 如果9月发现>3个：说明8月未充分收敛，或数据质量差异大")


def main():
    # 跑9月进化
    results = run_september_evolution()
    
    # 对比
    compare_august_september()
    
    print("\n" + "="*70)
    print("测试完成")
    print("="*70)


if __name__ == "__main__":
    main()
