"""
V4.5.2 指标评估引擎
四维评分体系：业务价值、数据质量、预测能力、实现成本
"""

import json
import os
import numpy as np
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class EvaluationScore:
    """评估分数"""
    total: float
    business: float
    quality: float
    predictive: float
    cost: float
    details: Dict


class IndicatorEvaluator:
    """指标评估器"""
    
    def __init__(self):
        # 标准权重（成熟期指标）
        self.standard_weights = {
            "business": 0.4,
            "quality": 0.25,
            "predictive": 0.2,
            "cost": 0.15
        }
    
    def get_dynamic_weights(self, indicator_age_days: int) -> Dict[str, float]:
        """
        根据指标年龄返回动态权重
        
        解决冷启动问题：新指标降低predictive权重
        """
        if indicator_age_days < 7:
            # 试用初期：侧重业务价值 + 数据质量
            return {
                "business": 0.5,
                "quality": 0.35,
                "predictive": 0.05,
                "cost": 0.10
            }
        elif indicator_age_days < 30:
            # 试用中期：逐步增加predictive权重
            return {
                "business": 0.45,
                "quality": 0.30,
                "predictive": 0.15,
                "cost": 0.10
            }
        else:
            # 成熟期：标准权重
            return self.standard_weights
    
    def evaluate(self, indicator: Dict, history_data: List[Dict] = None) -> EvaluationScore:
        """
        全面评估指标
        
        Args:
            indicator: 指标定义
            history_data: 历史计算数据（用于评估predictive）
        
        Returns:
            EvaluationScore
        """
        # 计算各项指标年龄
        created_at = indicator.get("created_at", datetime.now().isoformat())
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        age_days = (datetime.now() - created_at).days
        
        # 获取动态权重
        weights = self.get_dynamic_weights(age_days)
        
        # 计算各维度分数
        business_score = self._evaluate_business(indicator)
        quality_score = self._evaluate_quality(indicator, history_data)
        predictive_score = self._evaluate_predictive(indicator, history_data)
        cost_score = self._evaluate_cost(indicator)
        
        # 加权总分
        total = (
            business_score * weights["business"] +
            quality_score * weights["quality"] +
            predictive_score * weights["predictive"] +
            cost_score * weights["cost"]
        )
        
        return EvaluationScore(
            total=round(total, 2),
            business=round(business_score, 2),
            quality=round(quality_score, 2),
            predictive=round(predictive_score, 2),
            cost=round(cost_score, 2),
            details={
                "age_days": age_days,
                "weights": weights
            }
        )
    
    def _evaluate_business(self, indicator: Dict) -> float:
        """
        评估业务价值（0-1）
        
        考虑因素：
        - 可解释性（名称清晰度）
        - 与业务目标相关性
        - 独特性（与现有指标差异）
        """
        score = 0.5  # 基础分
        
        # 1. 名称规范性（可解释性）
        name = indicator.get("name", "")
        good_patterns = ["_rate", "_ratio", "_index", "_efficiency", "_score"]
        if any(p in name for p in good_patterns):
            score += 0.2
        
        # 2. 依赖合理性（与业务相关）
        deps = indicator.get("dependencies", [])
        key_metrics = ["ai56", "ai62", "ai68"]  # 关键业务指标
        if any(d in key_metrics for d in deps):
            score += 0.2
        
        # 3. 信息增益（独特性）
        info_gain = indicator.get("info_gain", 0)
        if info_gain > 0.05:
            score += 0.1
        
        return min(score, 1.0)
    
    def _evaluate_quality(self, indicator: Dict, history_data: List[Dict] = None) -> float:
        """
        评估数据质量（0-1）
        
        考虑因素：
        - 完整性（missing_rate）
        - 稳定性（CV）
        - 一致性（日波动）
        """
        score = 0.5
        
        # 1. 完整性
        missing_rate = indicator.get("missing_rate", 0)
        if missing_rate < 0.01:  # <1%
            score += 0.2
        elif missing_rate < 0.05:  # <5%
            score += 0.1
        
        # 2. 稳定性
        cv = indicator.get("cv", 1)
        if cv < 0.1:  # 非常稳定
            score += 0.2
        elif cv < 0.3:  # 稳定
            score += 0.1
        
        # 3. 历史数据质量（如果有）
        if history_data and len(history_data) > 0:
            values = [h.get("value") for h in history_data if h.get("value") is not None]
            if len(values) > 0:
                # 检查异常值比例
                q1, q3 = np.percentile(values, [25, 75])
                iqr = q3 - q1
                outliers = [v for v in values if v < q1 - 1.5*iqr or v > q3 + 1.5*iqr]
                outlier_rate = len(outliers) / len(values)
                if outlier_rate < 0.05:  # <5%异常值
                    score += 0.1
        
        return min(score, 1.0)
    
    def _evaluate_predictive(self, indicator: Dict, history_data: List[Dict] = None) -> float:
        """
        评估预测能力（0-1）
        
        考虑因素：
        - 与故障/异常的相关性
        - 趋势稳定性
        - 预警提前量
        """
        # 如果没有历史数据，返回中性分数
        if not history_data or len(history_data) < 7:
            return 0.5  # 中性，不加分不减分
        
        score = 0.5
        
        # 简化版：检查值的分布是否有区分度
        values = [h.get("value") for h in history_data if h.get("value") is not None]
        if len(values) > 10:
            # 检查动态范围
            value_range = max(values) - min(values)
            mean_val = np.mean(values)
            if mean_val > 0 and value_range / mean_val > 0.2:  # 有20%以上的变化范围
                score += 0.2
            
            # 检查趋势（是否有方向性）
            if len(values) >= 7:
                recent = np.mean(values[-3:])
                older = np.mean(values[:3])
                if abs(recent - older) / (abs(older) + 1e-6) > 0.1:  # 有明显趋势
                    score += 0.1
        
        return min(score, 1.0)
    
    def _evaluate_cost(self, indicator: Dict) -> float:
        """
        评估实现成本（0-1，越高表示成本越低/越好）
        
        考虑因素：
        - 计算复杂度（依赖数量）
        - 存储成本（数据量）
        - 维护难度
        """
        score = 0.5
        
        # 1. 依赖数量（越少越好）
        deps = indicator.get("dependencies", [])
        if len(deps) <= 2:
            score += 0.3
        elif len(deps) <= 4:
            score += 0.2
        elif len(deps) <= 6:
            score += 0.1
        
        # 2. 计算复杂度（通过公式长度粗略估计）
        formula = indicator.get("formula", "")
        if len(formula) < 50:  # 简单公式
            score += 0.1
        
        # 3. 是否有现有实现（复用性）
        # 简化处理，实际应该检查代码库
        
        return min(score, 1.0)
    
    def make_decision(self, score: EvaluationScore) -> str:
        """
        根据评分做决策
        
        Returns:
            "promote": 晋升到core
            "extend": 延长试用期
            "keep": 保持候选
            "deprecate": 废弃
        """
        if score.total >= 0.85:
            return "promote"
        elif score.total >= 0.70:
            return "extend"
        elif score.total >= 0.60:
            return "keep"
        else:
            return "deprecate"


# 便捷函数
def evaluate_indicator(indicator: Dict, history_data: List[Dict] = None) -> Dict:
    """评估指标的便捷函数"""
    evaluator = IndicatorEvaluator()
    score = evaluator.evaluate(indicator, history_data)
    decision = evaluator.make_decision(score)
    
    return {
        "score": {
            "total": score.total,
            "business": score.business,
            "quality": score.quality,
            "predictive": score.predictive,
            "cost": score.cost,
        },
        "decision": decision,
        "details": score.details
    }


if __name__ == "__main__":
    # 测试
    print("Indicator Evaluator 测试")
    print("=" * 50)
    
    test_indicator = {
        "name": "ai56_rolling_std",
        "dependencies": ["ai56"],
        "formula": "rolling_std(ai56, 24h)",
        "info_gain": 0.08,
        "missing_rate": 0.0,
        "cv": 0.15,
        "created_at": (datetime.now() - timedelta(days=15)).isoformat()
    }
    
    result = evaluate_indicator(test_indicator)
    print(f"\n指标: {test_indicator['name']}")
    print(f"总分: {result['score']['total']}")
    print(f"各维度: {result['score']}")
    print(f"决策: {result['decision']}")
