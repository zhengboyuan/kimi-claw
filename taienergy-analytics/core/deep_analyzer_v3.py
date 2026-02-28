"""
深度分析器 V3.0

核心升级：
1. 不再凭空猜测，而是基于 Python 引擎抓取的异常事实进行解释
2. 强制 LLM 对复合指标异常进行物理解释
3. 生成硬核 DSL 告警规则

解决痛点：根除 AI "废话假设" 和 "幻觉"
"""
import json
from typing import Dict, List, Optional


class DeepAnalyzerV3:
    """
    深度分析器 V3.0
    
    工作流程：
    1. 接收 Python 引擎抓取的异常复合指标
    2. LLM 基于异常事实进行物理解释
    3. 生成可固化的 DSL 告警规则
    """
    
    def __init__(self, llm_client=None):
        self.llm_client = llm_client
    
    def analyze_anomalies(
        self,
        surviving_composites: Dict,
        temp_max: float,
        context: Dict = None
    ) -> Dict:
        """
        分析异常复合指标
        
        Args:
            surviving_composites: Python 引擎抓取的异常复合指标
            temp_max: 当日最高温度
            context: 额外上下文信息
        
        Returns:
            LLM 生成的诊断结果和规则
        """
        if not surviving_composites:
            return {"status": "no_anomaly", "message": "今日无异常突变"}
        
        # 构建 Prompt
        prompt = self._build_interpretation_prompt(
            surviving_composites,
            temp_max,
            context or {}
        )
        
        # 调用 LLM
        if self.llm_client:
            response = self.llm_client.generate(prompt)
            return self._parse_llm_response(response)
        else:
            # 模拟 LLM 响应（用于测试）
            return self._mock_llm_response(surviving_composites, temp_max)
    
    def _build_interpretation_prompt(
        self,
        survivors: Dict,
        temp_max: float,
        context: Dict
    ) -> str:
        """
        构建物理解释 Prompt
        
        核心约束：
        1. LLM 不能提出新的基础公式
        2. 必须解释已抓取的异常
        3. 必须生成可执行的 DSL 规则
        """
        
        prompt = f"""# Role: 首席光伏诊断工程师 (Chief PV Diagnostic Engineer)

## 核心约束
你是一位资深的光伏系统诊断专家。你的任务是基于**已被数学引擎验证的异常事实**进行深度分析。
**重要**：你不需要发明新的基础公式，系统底层的 Python 引擎已经完成了特征组合和异常筛选。

## 背景信息
系统通过自动特征工程(AutoFE)发现了今日数据中存在几组【极度异常的复合指标尖刺】。

### 设备运行环境
- 今日最高设备温度: {temp_max}℃
- 数据时间范围: {context.get('date', '今日')} 全天运行数据

### 异常突变档案 (Surviving Composites)
以下复合指标经过达尔文筛选，被判定为"平时如死水，今天有惊雷"：

```json
{json.dumps(survivors, indent=2, ensure_ascii=False)}
```

### 指标含义对照表
- ai10-ai20: PV1-PV8 输入电流（组串电流）
- ai45: 逆变器输入功率
- ai56: 逆变器输出功率（有功功率）
- ai49-ai51: 电网三相电压（Ua, Ub, Uc）
- ai61: 设备内部温度

## 你的任务

### 任务1: 物理解因 (Physical Diagnosis)
对于每一个异常复合指标，解释：
1. **为什么** `formula` 今天会出现这么高的 `anomaly_peak_today`？
2. **根本原因**是什么？（结合环境温度、局部阴影、设备老化、电网波动等工业知识）
3. **如果不处理**，会导致什么后果？

### 任务2: 规则固化 (Rule Definition)
基于 `baseline_mean_today` 和 `anomaly_peak_today`，推导出一个实用的阈值条件：
1. 阈值应该设置为多少？（建议: mean + 2σ 或 anomaly_peak 的 80%）
2. 告警级别是什么？（warning/critical）
3. 告警描述应该怎么写？（简洁、 actionable）

### 任务3: 修复建议 (Remediation)
给出具体的修复操作建议：
1. 现场运维人员应该检查什么？
2. 优先处理哪个异常？
3. 预防措施是什么？

## 强制输出格式 (仅输出 JSON)

```json
{{
    "analysis_summary": "一句话总结今日异常的核心原因",
    "diagnosed_anomalies": [
        {{
            "composite_name": "异常指标名称",
            "formula": "公式",
            "physical_diagnosis": "深度原理解释（2-3句话）",
            "root_cause": "根本原因（遮挡/老化/高温/电网）",
            "consequence_if_ignored": "不处理的后果",
            "recommended_threshold": "建议阈值",
            "severity": "warning/critical",
            "remediation": {{
                "immediate_action": "立即检查项",
                "priority": "P0/P1/P2",
                "preventive_measure": "预防措施"
            }},
            "dsl_rule": {{
                "rule_name": "规则名称",
                "condition": {{
                    "left": "formula或指标",
                    "op": ">/</==",
                    "right": "阈值"
                }},
                "description": "告警描述"
            }}
        }}
    ],
    "overall_risk_level": "low/medium/high/critical",
    "recommended_actions_priority": ["按优先级排序的行动列表"]
}}
```

## 诊断示例

**示例输入**：
- composite_name: "diff_pct_ai10_ai12"
- formula: "Abs(ai10-ai12)/ai10"
- anomaly_peak_today: 0.35
- baseline_mean_today: 0.05

**示例输出**：
```json
{{
    "physical_diagnosis": "PV3(ai10)与PV4(ai12)组串电流偏差率高达35%，远超正常5%基线。这通常表明PV4组串存在局部遮挡（如树叶、鸟粪）或组件老化。",
    "root_cause": "局部遮挡",
    "consequence_if_ignored": "长期遮挡会导致热斑效应，损坏组件，降低整体发电量15-20%",
    "recommended_threshold": 0.20,
    "severity": "warning",
    "remediation": {{
        "immediate_action": "现场检查PV4组串表面是否有遮挡物",
        "priority": "P1",
        "preventive_measure": "定期清洗组件，修剪周围植被"
    }}
}}
```

请基于上述异常档案，给出专业的诊断分析。
"""
        return prompt
    
    def _parse_llm_response(self, response: str) -> Dict:
        """解析 LLM 响应"""
        try:
            # 尝试提取 JSON
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            
            # 尝试直接解析
            return json.loads(response)
        except Exception as e:
            return {
                "status": "parse_error",
                "error": str(e),
                "raw_response": response[:500]
            }
    
    def _mock_llm_response(self, survivors: Dict, temp_max: float) -> Dict:
        """模拟 LLM 响应（用于测试）"""
        diagnosed = []
        
        for name, info in survivors.items():
            if "diff_pct" in name:
                diagnosed.append({
                    "composite_name": name,
                    "formula": info["formula"],
                    "physical_diagnosis": f"{name}偏差率高达{info['anomaly_peak_today']*100:.1f}%，远超正常基线{info['baseline_mean_today']*100:.1f}%。这表明组串间存在严重不平衡，可能原因：局部遮挡、组件老化或接线松动。",
                    "root_cause": "局部遮挡或组件老化",
                    "consequence_if_ignored": "长期不平衡会导致热斑效应，降低整体发电量15-20%，甚至损坏组件",
                    "recommended_threshold": round(info['baseline_mean_today'] * 3, 3),
                    "severity": "warning" if info['anomaly_peak_today'] < 0.5 else "critical",
                    "remediation": {
                        "immediate_action": "现场检查组串表面遮挡物和接线状态",
                        "priority": "P1",
                        "preventive_measure": "定期清洗组件，检查MC4接头"
                    },
                    "dsl_rule": {
                        "rule_name": f"{name}_异常",
                        "condition": {
                            "left": name,
                            "op": ">",
                            "right": round(info['baseline_mean_today'] * 3, 3)
                        },
                        "description": f"组串电流偏差异常，建议检查遮挡或老化"
                    }
                })
            elif "efficiency_loss" in name:
                diagnosed.append({
                    "composite_name": name,
                    "formula": info["formula"],
                    "physical_diagnosis": f"转换效率损失率达{info['anomaly_peak_today']*100:.1f}%，结合今日最高温度{temp_max}℃，判断为高温降载或逆变器老化。正常损耗应<5%，当前异常。",
                    "root_cause": "高温降载或逆变器老化",
                    "consequence_if_ignored": "持续高温运行会加速逆变器老化，降低设备寿命30%以上",
                    "recommended_threshold": 0.10,
                    "severity": "critical" if info['anomaly_peak_today'] > 0.15 else "warning",
                    "remediation": {
                        "immediate_action": "检查逆变器散热风扇，清理散热片灰尘",
                        "priority": "P0",
                        "preventive_measure": "改善通风条件，考虑增加遮阳棚"
                    },
                    "dsl_rule": {
                        "rule_name": "转换效率损失异常",
                        "condition": {
                            "left": "efficiency_loss",
                            "op": ">",
                            "right": 0.10
                        },
                        "description": f"转换效率损失{info['anomaly_peak_today']*100:.1f}%，建议检查散热"
                    }
                })
            elif "vol_unbalance" in name:
                diagnosed.append({
                    "composite_name": name,
                    "formula": info["formula"],
                    "physical_diagnosis": f"三相电压不平衡度达{info['anomaly_peak_today']*100:.1f}%，超过正常范围（<5%）。可能原因：电网侧故障、负载不平衡或接线松动。",
                    "root_cause": "电网波动或接线问题",
                    "consequence_if_ignored": "长期不平衡会导致逆变器过流保护，影响发电稳定性",
                    "recommended_threshold": 0.05,
                    "severity": "warning",
                    "remediation": {
                        "immediate_action": "检查电网侧电压和逆变器接线端子",
                        "priority": "P1",
                        "preventive_measure": "定期紧固接线，监控电网质量"
                    },
                    "dsl_rule": {
                        "rule_name": "三相电压不平衡异常",
                        "condition": {
                            "left": "vol_unbalance",
                            "op": ">",
                            "right": 0.05
                        },
                        "description": "三相电压不平衡，建议检查电网和接线"
                    }
                })
        
        return {
            "analysis_summary": f"今日发现{len(diagnosed)}个复合指标异常，主要涉及{'高温降载' if temp_max > 40 else '组串不平衡'}问题，建议优先处理。",
            "diagnosed_anomalies": diagnosed,
            "overall_risk_level": "high" if any(d["severity"] == "critical" for d in diagnosed) else "medium",
            "recommended_actions_priority": [d["remediation"]["immediate_action"] for d in diagnosed]
        }


def save_diagnosis_to_rules(diagnosis: Dict, distiller) -> bool:
    """
    将诊断结果保存为 DSL 规则
    
    Args:
        diagnosis: LLM 诊断结果
        distiller: KnowledgeDistiller 实例
    
    Returns:
        是否成功保存
    """
    if "diagnosed_anomalies" not in diagnosis:
        return False
    
    rules_added = 0
    
    for anomaly in diagnosis["diagnosed_anomalies"]:
        if "dsl_rule" not in anomaly:
            continue
        
        rule = anomaly["dsl_rule"]
        # 这里可以调用 distiller 保存规则
        # distiller.add_rule(rule)
        rules_added += 1
    
    return rules_added > 0
