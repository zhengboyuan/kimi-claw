"""
Claw Agent 关联分析器
利用Kimi Claw自身的大模型能力进行指标关联分析
"""
import json
from typing import Dict, List, Optional
from datetime import datetime

from core.evolution_manager import IndicatorEvolutionManager


class ClawAgentCorrelationAnalyzer:
    """
    使用Claw Agent进行指标关联分析
    
    不依赖外部LLM API，而是spawn一个sub-agent来完成分析任务
    """
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.evolution_manager = IndicatorEvolutionManager(device_sn, memory_dir)
    
    def generate_analysis_task(self, max_indicators: int = 10) -> str:
        """
        生成关联分析任务描述（增强版）
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
            meta = self.evolution_manager.get_indicator_metadata(code)
            history = info.get('evaluation_history', [])
            
            if len(history) >= 3:
                recent_scores = [h.get('score', 0) for h in history[-3:]]
                avg_score = sum(recent_scores) / len(recent_scores)
                
                desc = f"- {code}: {meta.get('name', code)}"
                desc += f", 最近评分{avg_score:.2f}"
                
                # 添加趋势
                if len(recent_scores) >= 2:
                    if recent_scores[-1] > recent_scores[0] * 1.05:
                        desc += ", 上升"
                    elif recent_scores[-1] < recent_scores[0] * 0.95:
                        desc += ", 下降"
                    else:
                        desc += ", 稳定"
                
                indicator_descriptions.append(desc)
        
        # 生成任务描述
        task = f"""你是一个光伏设备数据分析专家。请基于以下指标信息，分析它们的关联性并设计复合指标。

【设备信息】
设备SN: {self.device_sn}
分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}

【指标列表】（共{len(indicator_descriptions)}个）
{chr(10).join(indicator_descriptions)}

【分析要求】

1. 识别物理关联：
   - 哪些指标在物理上相关？（如同一相电压电流、输入输出功率）
   - 关联强度（强/中/弱）

2. 设计复合指标（2-4个）：
   - 指标名称（中文，业务易懂）
   - 计算公式（使用指标代码如ai52, ai53）
   - 涉及的原子指标列表
   - 告警阈值（warning/critical）
   - 单位
   - 业务含义说明

3. 识别异常模式：
   - 哪些指标组合异常代表特定故障？

【输出格式】
必须返回有效的JSON，格式如下：
{{
  "composite_indicators": [
    {{
      "name": "三相不平衡度",
      "formula": "max(abs(ai52-ai53), abs(ai53-ai54), abs(ai54-ai52)) / avg(ai52, ai53, ai54)",
      "components": ["ai52", "ai53", "ai54"],
      "threshold": {{"warning": 0.10, "critical": 0.15}},
      "unit": "%",
      "description": "三相电流不平衡度，超过15%表示电网严重不平衡"
    }}
  ],
  "correlation_groups": [
    {{
      "indicators": ["ai52", "ai53", "ai54"],
      "relation_type": "物理关联",
      "strength": "强"
    }}
  ],
  "anomaly_patterns": [
    {{
      "pattern": "电压降+电流升",
      "indicators": ["ai49", "ai12"],
      "meaning": "可能存在短路风险"
    }}
  ]
}}
"""
        return task
    
    def analyze_with_claw_agent(self) -> Optional[Dict]:
        """
        使用Claw Agent进行关联分析
        """
        try:
            # 生成任务
            task = self.generate_analysis_task(max_indicators=10)
            
            print(f"[Claw Agent关联分析] 生成任务，长度: {len(task)} 字符")
            print(f"[Claw Agent关联分析] 准备spawn sub-agent...")
            
            # 执行分析
            result = self._call_claw_agent(task)
            
            if result:
                print(f"[Claw Agent关联分析] 成功生成 {len(result.get('composite_indicators', []))} 个复合指标")
                return result
            else:
                print("[Claw Agent关联分析] 未返回有效结果")
                return None
                
        except Exception as e:
            print(f"[Claw Agent关联分析] 错误: {e}")
            return None
    
    def _call_claw_agent(self, task: str) -> Optional[Dict]:
        """
        调用Claw Agent
        """
        # 检查是否在Claw环境中
        try:
            # 尝试使用sessions_spawn
            from tools import sessions_spawn
            
            # Spawn一个sub-agent来执行任务
            spawn_result = sessions_spawn(
                task=task,
                agent_id="main",
                label="correlation_analysis",
                runTimeoutSeconds=120
            )
            
            # 解析结果
            if spawn_result:
                return self._extract_json(spawn_result)
            return None
            
        except ImportError:
            print("[Claw Agent] 不在Claw环境中，使用备用方案")
            # 备用：返回一个基于规则的默认结果
            return self._generate_default_result()
    
    def _extract_json(self, text: str) -> Optional[Dict]:
        """从响应中提取JSON"""
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
    
    def _generate_default_result(self) -> Dict:
        """
        生成默认的关联分析结果（增强版）
        """
        # 获取当前L2指标
        l2_indicators = self.evolution_manager.get_indicators_by_level('L2')
        l2_set = set(l2_indicators)
        
        composite_indicators = []
        
        # 1. 三相不平衡度
        if {'ai52', 'ai53', 'ai54'}.issubset(l2_set):
            composite_indicators.append({
                "name": "三相不平衡度",
                "formula": "max(abs(ai52-ai53), abs(ai53-ai54), abs(ai54-ai52)) / avg(ai52, ai53, ai54) * 100",
                "components": ["ai52", "ai53", "ai54"],
                "threshold": {"warning": 10, "critical": 15},
                "unit": "%",
                "description": "三相电流不平衡度，超过15%表示电网严重不平衡",
                "category": "质量类"
            })
        
        # 2. 转换效率
        if {'ai56', 'ai45'}.issubset(l2_set):
            composite_indicators.append({
                "name": "转换效率",
                "formula": "ai56 / ai45 * 100",
                "components": ["ai56", "ai45"],
                "threshold": {"warning": 95, "critical": 90},
                "unit": "%",
                "description": "逆变器转换效率，低于90%表示设备效率严重下降",
                "category": "效率类"
            })
        
        # 3. 总有功功率
        if {'ai12', 'ai41', 'ai59'}.issubset(l2_set):
            composite_indicators.append({
                "name": "总有功功率",
                "formula": "(ai12 + ai41 + ai59) * 220 / 1000",
                "components": ["ai12", "ai41", "ai59"],
                "threshold": {"warning": 50, "critical": 80},
                "unit": "kW",
                "description": "三相总有功功率，反映实际发电能力",
                "category": "效率类"
            })
        
        # 4. 热效率指数
        if {'ai37', 'ai42'}.issubset(l2_set):
            composite_indicators.append({
                "name": "热效率指数",
                "formula": "ai42 / (ai37 + 273) * 100",
                "components": ["ai37", "ai42"],
                "threshold": {"warning": 0.5, "critical": 0.3},
                "unit": "指数",
                "description": "单位温度下的功率输出，反映散热效率",
                "category": "效率类"
            })
        
        # 5. 电压质量指数
        if {'ai49', 'ai9', 'ai54'}.issubset(l2_set):
            composite_indicators.append({
                "name": "电压质量指数",
                "formula": "std([ai49, ai9, ai54]) / avg([ai49, ai9, ai54]) * 100",
                "components": ["ai49", "ai9", "ai54"],
                "threshold": {"warning": 5, "critical": 10},
                "unit": "%",
                "description": "三相电压不平衡度，反映电网电压质量",
                "category": "质量类"
            })
        
        # 6. 功率因数健康度
        if 'ai11' in l2_set:
            composite_indicators.append({
                "name": "功率因数健康度",
                "formula": "ai11 * 100",
                "components": ["ai11"],
                "threshold": {"warning": 85, "critical": 80},
                "unit": "%",
                "description": "功率因数，低于0.8表示无功功率过大",
                "category": "质量类"
            })
        
        # 7. 设备负载率
        if {'ai12', 'ai45'}.issubset(l2_set):
            composite_indicators.append({
                "name": "设备负载率",
                "formula": "ai12 / ai45 * 100",
                "components": ["ai12", "ai45"],
                "threshold": {"warning": 80, "critical": 95},
                "unit": "%",
                "description": "A相电流与额定电流比值，超过95%为过载",
                "category": "安全类"
            })
        
        # 8. 发电稳定性指数
        if 'ai56' in l2_set:
            composite_indicators.append({
                "name": "日发电稳定性",
                "formula": "1 - (std(ai56_history) / avg(ai56_history))",
                "components": ["ai56"],
                "threshold": {"warning": 0.8, "critical": 0.6},
                "unit": "指数",
                "description": "日发电量的稳定性，反映设备运行平稳程度",
                "category": "经济类"
            })
        
        return {
            "composite_indicators": composite_indicators,
            "correlation_groups": [
                {
                    "name": "三相电流组",
                    "indicators": ["ai52", "ai53", "ai54"],
                    "correlation": "强"
                },
                {
                    "name": "三相电压组", 
                    "indicators": ["ai49", "ai9", "ai54"],
                    "correlation": "强"
                },
                {
                    "name": "功率组",
                    "indicators": ["ai45", "ai56"],
                    "correlation": "强"
                },
                {
                    "name": "温度功率组",
                    "indicators": ["ai37", "ai42"],
                    "correlation": "中"
                }
            ],
            "anomaly_patterns": [
                {
                    "name": "电网电压异常",
                    "indicators": ["ai50", "ai51", "ai52"],
                    "pattern": "电压先异常→电流跟随"
                },
                {
                    "name": "过热降额",
                    "indicators": ["ai37", "ai42"],
                    "pattern": "温度升高→功率下降"
                },
                {
                    "name": "三相失衡",
                    "indicators": ["ai52", "ai53", "ai54"],
                    "pattern": "三相电流差异超过15%"
                }
            ],
            "note": f"基于{len(l2_indicators)}个L2指标动态生成{len(composite_indicators)}个复合指标"
        }
    
    def save_composite_suggestions(self, analysis_result: Dict):
        """保存复合指标建议到L3层"""
        composite_indicators = analysis_result.get('composite_indicators', [])
        
        for comp in composite_indicators:
            suggestion_text = f"{comp['name']}: {comp['description']} (公式: {comp['formula']})"
            components = comp.get('components', [])
            
            self.evolution_manager.add_composite_suggestion(
                suggestion=suggestion_text,
                related_indicators=components
            )
            
            self._save_composite_definition(comp)
    
    def _save_composite_definition(self, comp: Dict):
        """保存复合指标详细定义"""
        import os
        
        composite_dir = f"memory/composite_indicators"
        os.makedirs(composite_dir, exist_ok=True)
        
        filename = f"{composite_dir}/{comp['name']}_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(comp, f, ensure_ascii=False, indent=2)
        
        print(f"  [保存] 复合指标定义: {filename}")
    
    def run_analysis(self) -> bool:
        """运行完整的关联分析流程"""
        print("\n" + "="*60)
        print("开始Claw Agent关联分析")
        print("="*60)
        
        # 检查L2指标数量
        l2_count = len(self.evolution_manager.get_indicators_by_level('L2'))
        if l2_count < 3:
            print(f"[跳过] L2指标不足3个（当前{l2_count}个），无需关联分析")
            return False
        
        # 执行分析
        result = self.analyze_with_claw_agent()
        
        if not result:
            print("[失败] 关联分析未返回有效结果")
            return False
        
        # 保存结果
        self.save_composite_suggestions(result)
        
        # 输出摘要
        print("\n[分析结果摘要]")
        print(f"- 复合指标: {len(result.get('composite_indicators', []))} 个")
        print(f"- 关联分组: {len(result.get('correlation_groups', []))} 个")
        print(f"- 异常模式: {len(result.get('anomaly_patterns', []))} 个")
        
        for comp in result.get('composite_indicators', [])[:3]:
            print(f"  • {comp['name']}: {comp['description'][:50]}...")
        
        print("="*60)
        
        return True