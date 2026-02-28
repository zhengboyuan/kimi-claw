"""
指标评估子智能体
负责深度评估候选指标质量
"""

import json
from typing import List, Dict
from dataclasses import dataclass


@dataclass
class AssessmentResult:
    """评估结果"""
    name: str
    overall_score: float  # 0-1
    dimensions: Dict[str, float]  # predictive, stability, explainability, cost
    verdict: str  # PROMOTE, EXTEND, REJECT
    reason: str


class IndicatorAssessor:
    """指标质量评估器"""
    
    def __init__(self, llm_client=None):
        self.llm = llm_client
    
    def assess_batch(self, candidates: List[Dict], context: Dict = None) -> List[AssessmentResult]:
        """
        批量评估候选指标
        
        Args:
            candidates: 候选指标列表
            context: 上下文信息（如现有指标、历史表现等）
            
        Returns:
            评估结果列表
        """
        if not self.llm:
            # LLM不可用时，使用规则降级
            return self._rule_assess(candidates)
        
        # 构建评估prompt
        prompt = self._build_assessment_prompt(candidates, context)
        
        try:
            response = self.llm.complete(prompt)
            return self._parse_assessment_response(response, candidates)
        except Exception as e:
            print(f"LLM评估失败: {e}，使用规则降级")
            return self._rule_assess(candidates)
    
    def _build_assessment_prompt(self, candidates: List[Dict], context: Dict) -> str:
        """构建评估prompt"""
        
        candidates_str = json.dumps(candidates, ensure_ascii=False, indent=2)
        
        existing_indicators = context.get('existing_indicators', []) if context else []
        existing_str = json.dumps(existing_indicators, ensure_ascii=False, indent=2)
        
        return f"""你是指标质量评估专家。请评估以下候选指标的质量。

## 评估维度
1. **predictive (预测力)**: 能否预测设备故障？0-1分
2. **stability (稳定性)**: 跨时间/跨设备是否稳定？0-1分  
3. **explainability (可解释性)**: 业务含义是否清晰？0-1分
4. **cost (计算成本)**: 实现复杂度如何？0-1分（越高越简单）

## 现有指标（避免重复）
{existing_str}

## 候选指标
{candidates_str}

## 输出格式
对每个候选指标，返回：
{{
    "name": "指标名称",
    "overall_score": 0.85,
    "dimensions": {{
        "predictive": 0.9,
        "stability": 0.8,
        "explainability": 0.85,
        "cost": 0.9
    }},
    "verdict": "PROMOTE|EXTEND|REJECT",
    "reason": "简要说明评估理由"
}}

返回JSON数组格式。
"""
    
    def _parse_assessment_response(self, response: str, candidates: List[Dict]) -> List[AssessmentResult]:
        """解析LLM评估响应"""
        try:
            # 提取JSON部分
            json_str = response
            if '```json' in response:
                json_str = response.split('```json')[1].split('```')[0]
            elif '```' in response:
                json_str = response.split('```')[1].split('```')[0]
            
            results = json.loads(json_str.strip())
            
            # 转换为AssessmentResult
            assessment_results = []
            for r in results:
                assessment_results.append(AssessmentResult(
                    name=r['name'],
                    overall_score=r['overall_score'],
                    dimensions=r['dimensions'],
                    verdict=r['verdict'],
                    reason=r['reason']
                ))
            
            return assessment_results
            
        except Exception as e:
            print(f"解析评估响应失败: {e}")
            return self._rule_assess(candidates)
    
    def _rule_assess(self, candidates: List[Dict]) -> List[AssessmentResult]:
        """规则降级评估（LLM不可用时）"""
        results = []
        
        for c in candidates:
            # 基于info_gain和cv简单评分
            info_gain = c.get('info_gain', 0)
            cv = c.get('cv', 1.0)
            
            # 计算综合分数
            predictive = min(info_gain * 10, 1.0)  # info_gain越高预测力越强
            stability = 1.0 - min(cv, 1.0)  # cv越低越稳定
            explainability = 0.7  # 默认中等
            cost = 0.8  # 默认较简单
            
            overall = (predictive + stability + explainability + cost) / 4
            
            # 判决
            if overall > 0.7:
                verdict = "PROMOTE"
            elif overall > 0.5:
                verdict = "EXTEND"
            else:
                verdict = "REJECT"
            
            results.append(AssessmentResult(
                name=c.get('name', 'unknown'),
                overall_score=round(overall, 2),
                dimensions={
                    "predictive": round(predictive, 2),
                    "stability": round(stability, 2),
                    "explainability": explainability,
                    "cost": cost
                },
                verdict=verdict,
                reason=f"规则评估: info_gain={info_gain:.4f}, cv={cv:.4f}"
            ))
        
        return results
    
    def smart_decide(self, candidate: Dict, registry: Dict, relationships: List[Dict]) -> str:
        """
        LLM智能决策：是否值得写入？
        
        Returns:
            SKIP: 跳过
            WRITE: 写入
            WRITE_VARIANT: 变体版本写入
            MERGE: 合并到现有
        """
        # 构造现有指标列表
        existing_indicators = []
        for name, info in registry.get('indicators', {}).items():
            existing_indicators.append({
                'name': name,
                'status': info.get('status', 'unknown'),
                'dependencies': info.get('dependencies', [])
            })
        
        # 如果没有现有指标，直接写入
        if not existing_indicators:
            return "WRITE"
        
        # 构造prompt
        prompt = f"""你是指标管理专家。请判断新候选指标是否应该写入候选池。

## 现有指标（已存在，避免重复）
{json.dumps(existing_indicators, indent=2, ensure_ascii=False)}

## 新候选指标
{json.dumps(candidate, indent=2, ensure_ascii=False)}

## 判断标准
1. WRITE - 全新指标，与现有指标完全不同（依赖不同或业务含义不同）
2. WRITE_VARIANT - 类似现有指标但有独特价值（如不同时间窗口/不同计算方式）
3. MERGE - 与现有指标高度相似，建议合并
4. SKIP - 完全重复，无需写入

## 输出格式
返回JSON：
{{
    "decision": "WRITE|WRITE_VARIANT|MERGE|SKIP",
    "reason": "简要说明理由"
}}
"""
        
        try:
            # 调用LLM判断
            if self.llm:
                response = self.llm.complete(prompt)
                result = self._parse_llm_decision(response)
                return result.get('decision', 'SKIP')
            else:
                # LLM不可用时，使用简单规则
                return self._rule_decide(candidate, existing_indicators)
        except Exception as e:
            logger.warning(f"LLM决策失败: {e}，使用规则降级")
            return self._rule_decide(candidate, existing_indicators)
    
    def _parse_llm_decision(self, response: str) -> Dict:
        """解析LLM决策响应"""
        try:
            # 提取JSON
            if '```json' in response:
                json_str = response.split('```json')[1].split('```')[0]
            elif '```' in response:
                json_str = response.split('```')[1].split('```')[0]
            else:
                json_str = response
            
            return json.loads(json_str.strip())
        except:
            return {'decision': 'SKIP', 'reason': '解析失败'}
    
    def _rule_decide(self, candidate: Dict, existing: List[Dict]) -> str:
        """规则降级（LLM不可用时）"""
        candidate_name = candidate.get('name', '')
        candidate_deps = set(candidate.get('dependencies', []))
        
        # 检查是否同名
        for ind in existing:
            if ind.get('name') == candidate_name:
                return "SKIP"
        
        # 检查依赖是否完全相同
        for ind in existing:
            if set(ind.get('dependencies', [])) == candidate_deps:
                return "WRITE_VARIANT"
        
        return "WRITE"
