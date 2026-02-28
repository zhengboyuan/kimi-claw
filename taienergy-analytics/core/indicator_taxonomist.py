"""
LLM指标分类学家 V4.0

角色：工业光伏指标分类学家 (Indicator Taxonomy Expert)
任务：评审候选公式，赐名，定物理意义
"""
import json
from typing import Dict, List


class IndicatorTaxonomist:
    """指标分类学家"""
    
    def __init__(self, llm_client=None):
        self.llm_client = llm_client
    
    def review_candidates(self, candidates: Dict, context: Dict) -> Dict:
        """
        评审候选公式
        
        Args:
            candidates: {公式名: {formula, fluctuation_feature}}
            context: {date, temp_max, etc.}
        
        Returns:
            approved_indicators: 批准的指标列表
        """
        prompt = self._build_taxonomy_prompt(candidates, context)
        
        if self.llm_client:
            response = self.llm_client.generate(prompt)
            return self._parse_response(response)
        else:
            # 模拟响应（用于测试）- 传递已注册数量
            existing_count = context.get('existing_count', 0)
            return self._mock_review(candidates, existing_count)
    
    def _build_taxonomy_prompt(self, candidates: Dict, context: Dict) -> str:
        """构建分类学家Prompt"""
        
        candidates_str = "\n".join([
            f"{i+1}. formula: `{info['formula']}`, 波动特征: {info.get('feature', 'N/A')}"
            for i, (name, info) in enumerate(candidates.items())
        ])
        
        prompt = f"""# Role: 工业光伏指标分类学家 (Indicator Taxonomy Expert)

## 背景
底层无监督算法在海量数据中，通过随机组合 (A/B, A-B) 发现了以下几个【具有强烈时序波动的数学公式】。

## 环境信息
- 日期: {context.get('date', 'N/A')}
- 最高温度: {context.get('temp_max', 'N/A')}℃
- 设备: {context.get('device_sn', 'XHDL_1NBQ')}

## 候选公式列表
{candidates_str}

## 你的任务：物种评审
请你以物理学视角审查这些纯数学公式：

1. **拒绝无意义噪音**：如果该公式纯属数学巧合或毫无物理意义，请直接丢弃。
2. **赐名与注册**：如果它有明确的物理意义（如温漂损耗、三相不平衡），请赐予它一个专业的【指标名称】，并简述其【物理含义】。

评审标准：
- ✅ 有明确物理意义（如效率、损耗、不平衡、离散等）
- ✅ 与光伏设备运行状态相关
- ❌ 纯数学巧合，无物理意义
- ❌ 与设备运行无关的随机组合

## 输出格式 (JSON)
```json
{{
  "approved_indicators": [
    {{
      "id": "comp_001",
      "name": "逆变器高温动态损耗率",
      "formula": "1.0 - (ai56/ai45)",
      "physical_meaning": "当内部温度升高时，逆变器由于散热瓶颈导致的效率衰减，反映高温降载程度",
      "confidence": 0.9
    }}
  ],
  "rejected_candidates": [
    {{
      "formula": "ai10*ai20/ai61",
      "reason": "纯数学组合，无明确物理意义"
    }}
  ]
}}
```

请严格按JSON格式输出，不要添加其他内容。
"""
        return prompt
    
    def _parse_response(self, response: str) -> Dict:
        """解析LLM响应"""
        try:
            # 提取JSON
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            return json.loads(response)
        except Exception as e:
            print(f"解析LLM响应失败: {e}")
            return {"approved_indicators": [], "rejected_candidates": []}
    
    def _mock_review(self, candidates: Dict, existing_count: int = 0) -> Dict:
        """模拟评审（用于测试）"""
        approved = []
        rejected = []
        
        for i, (name, info) in enumerate(candidates.items()):
            formula = info.get('formula', '')
            
            # 简单规则判断
            if 'efficiency_loss' in name or '1.0 - (ai56/ai45)' in formula:
                approved.append({
                    "id": f"comp_{existing_count + i + 1:03d}",
                    "name": "逆变器高温动态损耗率",
                    "formula": formula,
                    "physical_meaning": "当内部温度升高时，逆变器由于散热瓶颈导致的效率衰减，反映高温降载程度",
                    "confidence": 0.85
                })
            elif 'diff_pct' in name:
                approved.append({
                    "id": f"comp_{existing_count + i + 1:03d}",
                    "name": "PV组串电流离散率",
                    "formula": formula,
                    "physical_meaning": "反映组串间电流不平衡程度，可检测局部遮挡或组件老化差异",
                    "confidence": 0.9
                })
            elif 'vol_unbalance' in name:
                approved.append({
                    "id": f"comp_{existing_count + i + 1:03d}",
                    "name": "电网三相电压不平衡度",
                    "formula": formula,
                    "physical_meaning": "反映电网三相电压不平衡程度，超过国标5%需关注",
                    "confidence": 0.8
                })
            else:
                rejected.append({
                    "formula": formula,
                    "reason": "波动特征不明显或物理意义不明确"
                })
        
        return {
            "approved_indicators": approved,
            "rejected_candidates": rejected
        }


# 便捷函数
def review_candidates_with_llm(candidates: Dict, context: Dict, llm_client=None) -> Dict:
    """
    使用LLM评审候选公式（便捷函数）
    
    Args:
        candidates: 候选公式字典
        context: 上下文信息
        llm_client: LLM客户端（可选）
    
    Returns:
        评审结果
    """
    taxonomist = IndicatorTaxonomist(llm_client)
    return taxonomist.review_candidates(candidates, context)
