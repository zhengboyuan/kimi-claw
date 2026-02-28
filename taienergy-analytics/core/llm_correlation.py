"""
LLM关联分析模块
用于L1层指标的多指标关联分析和复合指标生成
"""
import json
from typing import Dict, List, Optional
from datetime import datetime

from core.evolution_manager import IndicatorEvolutionManager
from utils.llm_client import LLMClient


class LLMCorrelationAnalyzer:
    """
    LLM驱动的指标关联分析器
    
    职责：
    1. 分析L1/L2指标间的物理关联
    2. 生成复合指标建议
    3. 沉淀到L3复合建议层
    """
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.evolution_manager = IndicatorEvolutionManager(device_sn, memory_dir)
        self.llm_client = LLMClient()
    
    def generate_correlation_prompt(self, max_indicators: int = 15) -> str:
        """
        动态生成LLM关联分析prompt
        
        Args:
            max_indicators: 最多分析多少个指标（避免token过长）
        """
        # 获取L2和L1指标
        l2_indicators = self.evolution_manager.get_indicators_by_level('L2')
        l1_indicators = self.evolution_manager.get_indicators_by_level('L1')
        
        # 优先分析L2，再取L1前N个
        all_indicators = l2_indicators + l1_indicators
        selected_indicators = all_indicators[:max_indicators]
        
        # 生成指标描述
        indicator_descriptions = []
        
        for code in selected_indicators:
            info = self.evolution_manager.catalog['indicators'].get(code, {})
            # 使用get_indicator_metadata获取名称
            meta = self.evolution_manager.get_indicator_metadata(code)
            history = info.get('evaluation_history', [])
            
            if len(history) >= 3:
                # 最近3天数据
                recent_scores = [h.get('score', 0) for h in history[-3:]]
                avg_score = sum(recent_scores) / len(recent_scores)
                
                # 动态生成描述
                desc = f"- {code}: {meta.get('name', code)}"
                if meta.get('unit'):
                    desc += f", 单位{meta['unit']}"
                desc += f", 最近评分{avg_score:.2f}"
                
                # 添加趋势判断
                if len(recent_scores) >= 2:
                    if recent_scores[-1] > recent_scores[0] * 1.1:
                        desc += ", 上升趋势"
                    elif recent_scores[-1] < recent_scores[0] * 0.9:
                        desc += ", 下降趋势"
                    else:
                        desc += ", 相对稳定"
                
                indicator_descriptions.append(desc)
        
        # 组装prompt
        prompt = f"""你是一位资深的光伏设备分析专家，请分析以下设备指标的关联性，并设计复合指标。

【设备信息】
设备SN: {self.device_sn}
分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}

【指标列表】（共{len(indicator_descriptions)}个）
{chr(10).join(indicator_descriptions)}

【分析任务】

1. 【物理关联分析】
   - 哪些指标在物理上存在关联？（如同一相的电压电流、输入输出功率等）
   - 关联强度估计（强/中/弱）？
   - 是否存在领先-滞后关系？

2. 【复合指标设计】
   基于关联分析，设计2-5个复合指标：
   - 指标名称（中文，业务易懂）
   - 计算公式（使用指标代码）
   - 涉及的原子指标
   - 正常范围/告警阈值
   - 业务含义

3. 【异常模式识别】
   - 哪些指标组合异常时代表特定故障？
   - 异常传播路径？

【输出要求】
必须输出有效的JSON格式：
{{
  "composite_indicators": [
    {{
      "name": "三相不平衡度",
      "formula": "max(abs(ai52-ai53), abs(ai53-ai54), abs(ai54-ai52)) / avg(ai52, ai53, ai54)",
      "components": ["ai52", "ai53", "ai54"],
      "threshold": {{"warning": 0.10, "critical": 0.15}},
      "unit": "%",
      "description": "三相电流不平衡度，超过15%表示电网严重不平衡"
    }},
    {{
      "name": "转换效率",
      "formula": "ai56 / ai45",
      "components": ["ai56", "ai45"],
      "threshold": {{"warning": 0.95, "critical": 0.90}},
      "unit": "%",
      "description": "逆变器转换效率，低于90%表示设备效率严重下降"
    }}
  ],
  "correlation_groups": [
    {{
      "name": "三相电流组",
      "indicators": ["ai52", "ai53", "ai54"],
      "correlation": "强",
      "pattern": "同步波动，应保持一致"
    }}
  ],
  "anomaly_patterns": [
    {{
      "name": "电网异常",
      "indicators": ["ai50", "ai51", "ai52"],
      "sequence": "电压先异常→电流跟随",
      "suggestion": "优先监控电压指标"
    }}
  ]
}}

请确保JSON格式正确，可以被Python json.loads解析。"""
        
        return prompt
    
    def analyze_correlations(self) -> Optional[Dict]:
        """
        执行LLM关联分析
        
        Returns:
            解析后的JSON结果，或None如果失败
        """
        prompt = self.generate_correlation_prompt()
        
        print(f"[LLM关联分析] 生成prompt，长度: {len(prompt)} 字符")
        
        # 调用LLM
        try:
            response = self.llm_client.complete(prompt, temperature=0.3)
            
            # 提取JSON
            result = self._extract_json(response)
            
            if result:
                print(f"[LLM关联分析] 成功生成 {len(result.get('composite_indicators', []))} 个复合指标")
                return result
            else:
                print("[LLM关联分析] 无法从响应中提取有效JSON")
                return None
                
        except Exception as e:
            print(f"[LLM关联分析] 错误: {e}")
            return None
    
    def _extract_json(self, text: str) -> Optional[Dict]:
        """从LLM响应中提取JSON"""
        import re
        
        # 尝试直接解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        
        # 尝试提取```json ```块
        json_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
        matches = re.findall(json_pattern, text, re.DOTALL)
        
        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
        
        # 尝试提取{}块
        brace_pattern = r'(\{[\s\S]*\})'
        matches = re.findall(brace_pattern, text)
        
        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
        
        return None
    
    def save_composite_suggestions(self, analysis_result: Dict):
        """
        保存复合指标建议到L3层
        
        Args:
            analysis_result: LLM分析结果
        """
        composite_indicators = analysis_result.get('composite_indicators', [])
        
        for comp in composite_indicators:
            suggestion_text = f"{comp['name']}: {comp['description']} (公式: {comp['formula']})"
            components = comp.get('components', [])
            
            # 保存到evolution_manager的L3建议
            self.evolution_manager.add_composite_suggestion(
                suggestion=suggestion_text,
                related_indicators=components
            )
            
            # 同时保存详细定义
            self._save_composite_definition(comp)
    
    def _save_composite_definition(self, comp: Dict):
        """保存复合指标详细定义到文件"""
        import os
        
        composite_dir = f"memory/composite_indicators"
        os.makedirs(composite_dir, exist_ok=True)
        
        filename = f"{composite_dir}/{comp['name']}_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(comp, f, ensure_ascii=False, indent=2)
        
        print(f"  [保存] 复合指标定义: {filename}")
    
    def run_analysis(self) -> bool:
        """
        运行完整的关联分析流程
        
        Returns:
            是否成功
        """
        print("\n" + "="*60)
        print("开始LLM关联分析")
        print("="*60)
        
        # 1. 执行分析
        result = self.analyze_correlations()
        
        if not result:
            print("[失败] LLM关联分析未返回有效结果")
            return False
        
        # 2. 保存结果
        self.save_composite_suggestions(result)
        
        # 3. 输出摘要
        print("\n[分析结果摘要]")
        print(f"- 复合指标: {len(result.get('composite_indicators', []))} 个")
        print(f"- 关联分组: {len(result.get('correlation_groups', []))} 个")
        print(f"- 异常模式: {len(result.get('anomaly_patterns', []))} 个")
        
        for comp in result.get('composite_indicators', [])[:3]:
            print(f"  • {comp['name']}: {comp['description'][:50]}...")
        
        print("="*60)
        return True
