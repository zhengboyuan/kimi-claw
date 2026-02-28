"""
知识蒸馏器（第三阶段核心组件）

职责：
1. 每月/触发式读取已验证假设
2. 调用 Architect Prompt 生成 dynamic_rules.json
3. 安全地执行 DSL 规则

触发条件：
- 某假设 success_count >= 10
- 每运行满 30 天
"""
import json
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd

from core.hypothesis_registry import HypothesisRegistry


class KnowledgeDistiller:
    """
    知识蒸馏器
    
    工作流程：
    1. 读取 hypothesis_registry.json 中 verified 状态的假设
    2. 调用 Architect Prompt 生成规则 DSL
    3. 保存为 dynamic_rules.json
    4. 提供安全执行引擎
    """
    
    # 触发阈值
    MIN_SUCCESS_COUNT = 10
    DEFAULT_INTERVAL_DAYS = 30
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.memory_dir = Path(memory_dir)
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        
        self.registry = HypothesisRegistry(device_sn, memory_dir)
        
        # 动态规则文件路径
        self.rules_file = self.memory_dir / "dynamic_rules.json"
    
    def should_distill(self, force: bool = False) -> bool:
        """
        检查是否应该执行知识蒸馏
        
        Args:
            force: 强制触发
        
        Returns:
            是否应该蒸馏
        """
        if force:
            return True
        
        # 检查是否有足够的已验证假设
        ready_hyps = self.registry.get_hypotheses_for_distillation(self.MIN_SUCCESS_COUNT)
        
        if len(ready_hyps) >= 3:  # 至少 3 个规则才值得蒸馏
            return True
        
        # 检查是否到了时间间隔
        if self.rules_file.exists():
            import os
            mtime = datetime.fromtimestamp(os.path.getmtime(self.rules_file))
            days_since = (datetime.now() - mtime).days
            
            if days_since >= self.DEFAULT_INTERVAL_DAYS:
                return True
        
        return False
    
    def distill(
        self,
        llm_client,
        force: bool = False
    ) -> Optional[Dict]:
        """
        执行知识蒸馏
        
        Args:
            llm_client: LLM 客户端
            force: 强制触发
        
        Returns:
            生成的规则集，或 None（如果不需要蒸馏）
        """
        if not self.should_distill(force):
            print("[知识蒸馏] 暂不需要执行蒸馏")
            return None
        
        print("\n[知识蒸馏] 开始执行知识蒸馏...")
        
        # 1. 获取已验证假设
        verified_hyps = self.registry.get_hypotheses_for_distillation(self.MIN_SUCCESS_COUNT)
        
        if not verified_hyps:
            print("  没有足够的已验证假设")
            return None
        
        print(f"  已验证假设: {len(verified_hyps)} 个")
        
        # 2. 加载当前规则（用于对比）
        current_rules = self._load_current_rules()
        
        # 3. 调用 Architect Prompt
        rules_json = self._call_architect_prompt(
            verified_hyps=verified_hyps,
            current_rules=current_rules,
            llm_client=llm_client
        )
        
        if not rules_json:
            print("  生成规则失败")
            return None
        
        # 4. 保存规则
        self._save_rules(rules_json)
        
        # 5. 更新假设状态（标记为已固化）
        for hyp in verified_hyps:
            hyp["status"] = "distilled"
        
        print(f"  知识蒸馏完成: 生成 {len(rules_json.get('hardcoded_rules', []))} 条规则")
        
        return rules_json
    
    def _load_current_rules(self) -> Dict:
        """加载当前动态规则"""
        if self.rules_file.exists():
            with open(self.rules_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            "version": "1.0.0",
            "hardcoded_rules": []
        }
    
    def _save_rules(self, rules: Dict):
        """保存动态规则"""
        rules["updated_at"] = datetime.now().isoformat()
        with open(self.rules_file, 'w', encoding='utf-8') as f:
            json.dump(rules, f, ensure_ascii=False, indent=2)
    
    def _call_architect_prompt(
        self,
        verified_hyps: List[Dict],
        current_rules: Dict,
        llm_client
    ) -> Optional[Dict]:
        """
        调用 Architect Prompt 生成规则 DSL
        
        安全设计：
        - 让 LLM 生成 JSON DSL，而不是 Python 代码
        - DSL 使用简单的操作符（>, <, ==, between 等）
        """
        # 构建输入
        hyps_text = "\n".join([
            f"- {h['id']}: {h['logic']} (成功 {h['success_count']} 次，权重 {h['weight']:.2f})"
            for h in verified_hyps[:20]  # 最多取 20 个
        ])
        
        current_rules_text = json.dumps(
            current_rules.get("hardcoded_rules", []),
            ensure_ascii=False,
            indent=2
        )
        
        prompt = f"""# Role: 工业光伏系统首席架构师 (Chief Architect)

## Context
你正在主导一个自动进化光伏监控系统的"月度认知大版本更新"。
过去 30 天，系统的"假设实验库"通过对海量真实数据的追踪，验证了大量关于设备物理特性的假设。

## Input Data

### 1. 已验证的假设库 (Success >= {self.MIN_SUCCESS_COUNT})
{hyps_text}

### 2. 当前固化的规则库
```json
{current_rules_text}
```

## Your Task
你需要进行"知识蒸馏"，输出一份结构化的 `dynamic_rules.json`，供下一代规则引擎直接调用执行。

### Execution Steps
1. **清理认知债务**：分析已验证假设，指出旧规则错在哪里（如有）。
2. **提取不变量 (Invariants)**：从已验证假设中，提取出可以通过简单数学逻辑表达的核心物理公式。
3. **降维打击**：将依赖昂贵 LLM 算力的长文本推理，压缩为精确的 DSL 规则。

### DSL 规范
规则必须使用以下安全的 DSL 格式（绝不使用 Python 代码）：

```json
{{
  "rule_id": "R_001",
  "name": "动态效率监控",
  "target_metrics": ["ai45", "ai56", "ai61"],
  "condition": {{
    "operator": "and",
    "conditions": [
      {{"metric": "ai61", "op": ">", "value": 45}},
      {{
        "operator": "div",
        "left": {{"metric": "ai56"}},
        "right": {{"metric": "ai45"}},
        "op": "<",
        "value": 0.96
      }}
    ]
  }},
  "severity": "warning",
  "description": "当内部温度高于45度时，若转换效率低于96%则判定为异常发热降载"
}}
```

支持的运算符：
- 比较: `>`, `<`, `>=`, `<=`, `==`, `!=`
- 逻辑: `and`, `or`, `not`
- 算术: `add`, `sub`, `mul`, `div`
- 聚合: `avg`, `min`, `max` (对时间序列)

## Output Format (严格输出 JSON)
```json
{{
  "version": "1.1.0",
  "cognitive_summary": "本月系统认识到...",
  "hardcoded_rules": [
    // DSL 规则列表
  ]
}}
```

注意：
1. 只输出 JSON，不要输出任何其他文本
2. 规则数量控制在 5-10 条，优先保留高权重、高成功次数的假设
3. 每条规则必须有清晰的 description 说明物理意义
"""
        
        try:
            response = llm_client.generate(prompt)
            
            # 提取 JSON
            # 尝试直接解析
            try:
                rules = json.loads(response)
                return rules
            except json.JSONDecodeError:
                # 尝试从 markdown 代码块中提取
                import re
                json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
                if json_match:
                    rules = json.loads(json_match.group(1))
                    return rules
                
                # 尝试找第一个 { 和最后一个 }
                start = response.find('{')
                end = response.rfind('}')
                if start != -1 and end != -1:
                    rules = json.loads(response[start:end+1])
                    return rules
                
                raise ValueError("无法从响应中提取 JSON")
        
        except Exception as e:
            print(f"  [Architect Prompt] 失败: {e}")
            return None
    
    def load_rules(self) -> List[Dict]:
        """加载动态规则"""
        rules_data = self._load_current_rules()
        return rules_data.get("hardcoded_rules", [])
    
    def evaluate_rules(
        self,
        data_dict: Dict[str, pd.DataFrame]
    ) -> List[Dict]:
        """
        安全地执行 DSL 规则
        
        绝不使用 eval()，使用安全的 DSL 解析器
        """
        rules = self.load_rules()
        triggered = []
        
        for rule in rules:
            try:
                is_triggered = self._evaluate_rule_dsl(rule, data_dict)
                if is_triggered:
                    triggered.append({
                        "rule_id": rule.get("rule_id"),
                        "name": rule.get("name"),
                        "severity": rule.get("severity", "info"),
                        "description": rule.get("description", "")
                    })
            except Exception as e:
                print(f"  [规则执行] {rule.get('rule_id', 'unknown')}: 执行失败 - {e}")
        
        return triggered
    
    def _evaluate_rule_dsl(
        self,
        rule: Dict,
        data_dict: Dict[str, pd.DataFrame]
    ) -> bool:
        """
        安全地评估单条 DSL 规则
        
        使用递归下降解析，绝不使用 eval()
        """
        condition = rule.get("condition", {})
        
        return self._evaluate_condition(condition, data_dict)
    
    def _evaluate_condition(
        self,
        condition: Dict,
        data_dict: Dict[str, pd.DataFrame]
    ) -> bool:
        """递归评估条件"""
        operator = condition.get("operator")
        
        # 逻辑运算符
        if operator == "and":
            sub_conditions = condition.get("conditions", [])
            return all(
                self._evaluate_condition(c, data_dict) 
                for c in sub_conditions
            )
        
        elif operator == "or":
            sub_conditions = condition.get("conditions", [])
            return any(
                self._evaluate_condition(c, data_dict) 
                for c in sub_conditions
            )
        
        elif operator == "not":
            sub_condition = condition.get("condition", {})
            return not self._evaluate_condition(sub_condition, data_dict)
        
        # 算术运算符（返回数值，不是布尔）
        elif operator in ["add", "sub", "mul", "div"]:
            return self._evaluate_arithmetic(condition, data_dict)
        
        # 比较运算符
        else:
            return self._evaluate_comparison(condition, data_dict)
    
    def _evaluate_arithmetic(
        self,
        condition: Dict,
        data_dict: Dict[str, pd.DataFrame]
    ) -> float:
        """评估算术表达式"""
        operator = condition.get("operator")
        
        left = self._get_value(condition.get("left"), data_dict)
        right = self._get_value(condition.get("right"), data_dict)
        
        if operator == "add":
            return left + right
        elif operator == "sub":
            return left - right
        elif operator == "mul":
            return left * right
        elif operator == "div":
            return left / right if right != 0 else float('inf')
        
        return 0.0
    
    def _evaluate_comparison(
        self,
        condition: Dict,
        data_dict: Dict[str, pd.DataFrame]
    ) -> bool:
        """评估比较表达式"""
        # 可能是嵌套的算术表达式
        if "operator" in condition and condition["operator"] in ["add", "sub", "mul", "div"]:
            left = self._evaluate_arithmetic(condition, data_dict)
        else:
            left = self._get_value(condition.get("left") or condition.get("metric"), data_dict)
        
        op = condition.get("op") or condition.get("operator")
        right = condition.get("value", 0)
        
        if op == ">":
            return left > right
        elif op == "<":
            return left < right
        elif op == ">=":
            return left >= right
        elif op == "<=":
            return left <= right
        elif op == "==":
            return left == right
        elif op == "!=":
            return left != right
        
        return False
    
    def _get_value(
        self,
        operand: any,
        data_dict: Dict[str, pd.DataFrame]
    ) -> float:
        """获取操作数的值"""
        if isinstance(operand, (int, float)):
            return float(operand)
        
        if isinstance(operand, dict):
            # 可能是嵌套表达式
            if "operator" in operand:
                return self._evaluate_arithmetic(operand, data_dict)
            # 或者是指标引用
            metric = operand.get("metric")
            if metric and metric in data_dict:
                return data_dict[metric]['value'].mean()
            return 0.0
        
        if isinstance(operand, str):
            # 指标代码
            if operand in data_dict:
                return data_dict[operand]['value'].mean()
            return 0.0
        
        return 0.0
