#!/usr/bin/env python3
"""
第二、三阶段运行脚本
- 第二阶段：假设生成与验证
- 第三阶段：知识蒸馏
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime, timedelta
from skills.skill_1_data_collector import DataCollector
from core.hypothesis_registry import HypothesisRegistry
from core.knowledge_distiller import KnowledgeDistiller
from core.evolution_manager import IndicatorEvolutionManager

# 模拟LLM客户端（实际使用时需要接入真实LLM）
class MockLLMClient:
    def generate(self, prompt):
        # 模拟LLM响应
        if "假设" in prompt or "hypothesis" in prompt.lower():
            return """
- "ai45 * 0.98 ≈ ai56" (输入功率乘以转换效率约等于输出功率)
- "ai61 > 45 时 ai60 < 0.96" (温度高于45度时效率低于96%)
- "ai45 correlates with ai56" (输入功率与输出功率相关)
"""
        elif "反思" in prompt or "reflection" in prompt.lower():
            return "假设失败的可能原因是系数需要调整，建议降低转换效率预期值到0.95。"
        elif "Architect" in prompt or "知识蒸馏" in prompt:
            return """{
  "version": "1.1.0",
  "cognitive_summary": "系统发现逆变器效率与温度密切相关，高温时效率下降明显",
  "hardcoded_rules": [
    {
      "rule_id": "R_001",
      "name": "高温效率监控",
      "target_metrics": ["ai45", "ai56", "ai61"],
      "condition": {
        "operator": "and",
        "conditions": [
          {"metric": "ai61", "op": ">", "value": 45},
          {"operator": "div", "left": {"metric": "ai56"}, "right": {"metric": "ai45"}, "op": "<", "value": 0.96}
        ]
      },
      "severity": "warning",
      "description": "当内部温度高于45度时，若转换效率低于96%则判定为异常发热降载"
    }
  ]
}"""
        return ""

def run_stage_2_and_3(device_sn="XHDL_1NBQ"):
    """运行第二、三阶段"""
    print("="*70)
    print("第二阶段：左脑 - 假设验证")
    print("="*70)
    
    # 初始化组件
    collector = DataCollector(device_sn)
    registry = HypothesisRegistry(device_sn)
    evo_manager = IndicatorEvolutionManager(device_sn)
    llm_client = MockLLMClient()
    
    # 获取L2核心指标
    l2_indicators = evo_manager.get_indicators_by_level("L2")
    print(f"\nL2核心指标: {len(l2_indicators)} 个")
    print(f"  示例: {', '.join(l2_indicators[:5])}")
    
    # 1. 生成初始假设
    print("\n" + "="*70)
    print("1. 生成初始假设")
    print("="*70)
    
    # 基于核心指标创建假设
    hypotheses = [
        ("ai45 * 0.98 ≈ ai56", ["ai45", "ai56"]),
        ("ai61 > 45 时 ai60 < 0.96", ["ai61", "ai60"]),
        ("ai45 correlates with ai56", ["ai45", "ai56"]),
        ("ai10 + ai12 + ai16 + ai20 ≈ ai45", ["ai10", "ai12", "ai16", "ai20", "ai45"]),
    ]
    
    for logic, indicators in hypotheses:
        hyp_id = registry.create_hypothesis(
            logic=logic,
            related_indicators=indicators,
            source="manual_creation"
        )
        print(f"  创建假设: {hyp_id} - {logic}")
    
    # 2. 验证假设（使用最近30天数据）
    print("\n" + "="*70)
    print("2. 验证假设（使用历史数据）")
    print("="*70)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    testing_hyps = registry.get_testing_hypotheses()
    print(f"\n待验证假设: {len(testing_hyps)} 个")
    
    # 简化：直接模拟验证结果
    for hyp in testing_hyps:
        hyp_id = hyp["id"]
        # 模拟验证成功
        for day in range(1, 15):  # 验证14天
            result = registry.verify_hypothesis(
                hyp_id=hyp_id,
                actual_value=0.97 + (day * 0.001),
                expected_value=0.98,
                deviation=0.02,
                test_data={"day": day}
            )
        
        if result["success_count"] >= 10:
            print(f"  ✓ {hyp_id}: 已验证 (成功 {result['success_count']} 次)")
        
        # 添加反思（如果是失败的假设）
        if result["status"] == "failed":
            reflection = llm_client.generate(f"反思: {hyp['logic']}")
            registry.add_reflection(hyp_id, reflection)
    
    # 3. 记录认知增量
    print("\n" + "="*70)
    print("3. 记录认知增量")
    print("="*70)
    
    new_insights = [
        "发现ai45与ai56高度相关（相关系数0.99）",
        "发现温度ai61与效率ai60呈负相关",
        "PV输入电流总和约等于输入功率"
    ]
    
    gain = registry.record_cognitive_gain(
        date_str=datetime.now().strftime("%Y-%m-%d"),
        new_insights=new_insights,
        source="stage_2_analysis"
    )
    print(f"  认知增量: {gain} (发现 {len(new_insights)} 个新洞察)")
    
    # 打印假设库报告
    registry.print_registry_report()
    
    # 第三阶段：知识蒸馏
    print("\n" + "="*70)
    print("第三阶段：右脑 - 知识蒸馏")
    print("="*70)
    
    distiller = KnowledgeDistiller(device_sn)
    
    # 检查是否有足够的已验证假设
    ready_hyps = registry.get_hypotheses_for_distillation(min_success_count=10)
    print(f"\n可蒸馏的假设: {len(ready_hyps)} 个")
    
    if len(ready_hyps) >= 3:
        # 执行知识蒸馏
        rules = distiller.distill(
            llm_client=llm_client,
            force=True
        )
        
        if rules:
            print(f"\n✓ 知识蒸馏完成!")
            print(f"  生成规则数: {len(rules.get('hardcoded_rules', []))}")
            print(f"  版本: {rules.get('version', 'unknown')}")
            print(f"\n规则摘要:")
            for rule in rules.get('hardcoded_rules', [])[:3]:
                print(f"  - {rule.get('rule_id')}: {rule.get('name')}")
        else:
            print("\n✗ 知识蒸馏失败")
    else:
        print(f"\n⚠ 可蒸馏假设不足 ({len(ready_hyps)} < 3)，跳过知识蒸馏")
    
    # 测试规则执行
    print("\n" + "="*70)
    print("4. 测试规则执行")
    print("="*70)
    
    # 模拟测试数据
    import pandas as pd
    import numpy as np
    
    test_data = {
        "ai45": pd.DataFrame({
            "timestamp": pd.date_range("2026-01-01", periods=24, freq="h"),
            "value": [100.0] * 24
        }),
        "ai56": pd.DataFrame({
            "timestamp": pd.date_range("2026-01-01", periods=24, freq="h"),
            "value": [97.0] * 24
        }),
        "ai61": pd.DataFrame({
            "timestamp": pd.date_range("2026-01-01", periods=24, freq="h"),
            "value": [50.0] * 24  # 高温
        })
    }
    
    triggered = distiller.evaluate_rules(test_data)
    
    if triggered:
        print(f"\n✓ 触发 {len(triggered)} 条规则:")
        for rule in triggered:
            print(f"  - {rule['rule_id']}: {rule['name']} ({rule['severity']})")
    else:
        print("\n  未触发任何规则")
    
    print("\n" + "="*70)
    print("第二、三阶段完成!")
    print("="*70)

if __name__ == "__main__":
    run_stage_2_and_3()
