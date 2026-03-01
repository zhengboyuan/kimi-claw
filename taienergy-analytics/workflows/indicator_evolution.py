"""
指标进化 Workflow

符合V5.1规范：
- 已注册到 skills_registry.yaml
- 输入输出明确
- 调用原子能力
"""

import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from core.indicator_evolution import IndicatorEvolution, IndicatorRegistry
from core.memory_system import MemorySystem


def run_indicator_evolution(round_num: Optional[int] = None, 
                            date_str: Optional[str] = None) -> Dict:
    """
    指标进化 Workflow
    
    Args:
        round_num: 指定轮次(1/2/3)，None则自动判断
        date_range: 数据日期范围，None则使用最近30天
    
    Returns:
        {
            "success": bool,
            "round": int,
            "candidates_found": int,
            "candidates": [...],
            "converged": bool,
            "report_path": str
        }
    """
    print(f"\n{'='*60}")
    print("指标进化 Workflow")
    print(f"{'='*60}")
    
    # 初始化
    evo = IndicatorEvolution()
    memory = MemorySystem()
    
    # 确定轮次
    if round_num is None:
        round_num = evo.suggest_next_round()
        if round_num == 0:
            print("✓ 指标体系已收敛，无需继续进化")
            return {
                "success": True,
                "round": 0,
                "candidates_found": 0,
                "candidates": [],
                "converged": True
            }
    
    print(f"\n执行第 {round_num} 轮进化: {evo.ROUNDS[round_num]}")
    
    # 确定日期
    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"数据日期: {date_str}")
    
    # 执行进化（传入日期字符串）
    candidates = evo.evolve(round_num, date_str)
    
    # 判断数据来源
    data_source = "registry"
    if candidates and candidates[0].get("data_source") == "raw_api":
        data_source = "raw_api"
    
    # 生成报告
    report = {
        "success": True,
        "round": round_num,
        "strategy": evo.ROUNDS[round_num],
        "candidates_found": len(candidates),
        "candidates": [
            {
                "id": c["id"],
                "name": c["name"],
                "formula": c["formula"],
                "description": c["description"]
            }
            for c in candidates
        ],
        "converged": len(candidates) < 3,
        "data_source": data_source
    }
    
    # 保存报告
    report_path = memory.write_evolution_report(report)
    
    # 打印摘要
    print(f"\n发现候选指标: {len(candidates)} 个")
    for c in candidates:
        print(f"  • {c['id']}: {c['name']}")
        print(f"    公式: {c['formula']}")
    
    if report["converged"]:
        print(f"\n✓ 本轮收敛（发现<3个），建议停止或人工审核")
    else:
        print(f"\n→ 建议继续第 {round_num + 1} 轮进化")
    
    print(f"\n报告已保存: {report_path}")
    print(f"{'='*60}\n")
    
    return report


def run_full_evolution() -> Dict:
    """运行完整的三轮进化"""
    print(f"\n{'='*60}")
    print("完整指标进化（三轮）")
    print(f"{'='*60}")
    
    evo = IndicatorEvolution()
    device_data = {"placeholder": "actual_data"}
    
    results = evo.run_full_evolution(device_data)
    
    # 打印汇总
    print("\n三轮进化汇总:")
    for r in results["rounds"]:
        print(f"  第{r['round']}轮 ({r['strategy']}): {r['candidates_found']} 个候选")
    
    print(f"\n总计候选指标: {results['total_candidates']} 个")
    print(f"收敛状态: {'已收敛' if results['converged'] else '未收敛'}")
    print(f"{'='*60}\n")
    
    return results


def approve_candidate(candidate_id: str) -> bool:
    """
    审核通过候选指标
    
    Args:
        candidate_id: 候选指标ID
    
    Returns:
        是否成功
    """
    reg = IndicatorRegistry()
    success = reg.approve_candidate(candidate_id)
    
    if success:
        reg.save()
        print(f"✓ 候选指标 '{candidate_id}' 已批准，进入注册表")
    else:
        print(f"✗ 候选指标 '{candidate_id}' 不存在或已批准")
    
    return success


def list_candidates(status: str = "pending") -> List[Dict]:
    """列出候选指标"""
    reg = IndicatorRegistry()
    candidates = reg.get_candidates()
    
    print(f"\n候选指标列表 ({len(candidates)} 个):")
    print("-" * 60)
    
    for cid, c in candidates.items():
        print(f"ID: {cid}")
        print(f"  名称: {c['name']}")
        print(f"  公式: {c['formula']}")
        print(f"  轮次: 第{c.get('round', '?')}轮")
        print(f"  发现时间: {c.get('discovered_at', 'unknown')}")
        print()
    
    return list(candidates.values())


def main():
    parser = argparse.ArgumentParser(description="指标进化 Workflow")
    parser.add_argument("--round", type=int, choices=[1, 2, 3], 
                       help="指定进化轮次")
    parser.add_argument("--full", action="store_true",
                       help="运行完整三轮进化")
    parser.add_argument("--list", action="store_true",
                       help="列出候选指标")
    parser.add_argument("--approve", type=str,
                       help="批准指定候选指标")
    
    args = parser.parse_args()
    
    if args.full:
        run_full_evolution()
    elif args.list:
        list_candidates()
    elif args.approve:
        approve_candidate(args.approve)
    else:
        run_indicator_evolution(args.round)


if __name__ == "__main__":
    main()
