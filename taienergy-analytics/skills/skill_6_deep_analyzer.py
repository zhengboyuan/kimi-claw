"""
Skill 6: 根因诊断助手
基于记忆上下文进行深度诊断
"""
from typing import Dict, List, Any
import pandas as pd
from utils.memory_manager import MemoryManager


class RootCauseDiagnostician:
    """
    根因诊断助手
    
    职责：
    1. 读取记忆上下文（L1/L2/L3）
    2. 结合当前异常数据进行诊断
    3. 输出诊断结论和建议
    
    注意：此为 Prompt 模板设计，实际 LLM 调用由外部实现
    """
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.memory = MemoryManager(device_sn)
    
    def prepare_context(
        self,
        indicator_code: str,
        current_data: pd.DataFrame,
        anomaly_info: Dict
    ) -> Dict[str, Any]:
        """
        准备诊断上下文
        
        收集所有相关信息供 LLM 分析
        """
        context = {
            "device_sn": self.device_sn,
            "indicator_code": indicator_code,
            "current_data_summary": self._summarize_current_data(current_data),
            "anomaly_info": anomaly_info,
            "memory_context": self._load_memory_context(indicator_code),
            "diagnostic_prompt": self._build_diagnostic_prompt()
        }
        
        return context
    
    def _summarize_current_data(self, df: pd.DataFrame) -> Dict:
        """汇总当前数据"""
        if df.empty:
            return {"status": "no_data"}
        
        values = df['value'].dropna()
        
        return {
            "time_range": f"{df['timestamp'].min()} ~ {df['timestamp'].max()}",
            "data_points": len(df),
            "mean": float(values.mean()) if len(values) > 0 else None,
            "std": float(values.std()) if len(values) > 0 else None,
            "min": float(values.min()) if len(values) > 0 else None,
            "max": float(values.max()) if len(values) > 0 else None,
            "zero_ratio": float((values == 0).sum() / len(values)) if len(values) > 0 else None
        }
    
    def _load_memory_context(self, indicator_code: str) -> Dict:
        """加载记忆上下文"""
        # L1: 静态档案
        l1_static = {
            "device_sn": self.device_sn,
            "indicator": indicator_code,
            "note": "测试设备，详细档案待补充"
        }
        
        # L2: 动态履历（近期记录）
        dates = self.memory.get_all_dates()
        recent_logs = []
        for date in dates[-7:]:  # 最近7天
            log = self.memory.load_daily_log(date)
            if log:
                recent_logs.append({
                    "date": date,
                    "has_anomaly": log.get("data", {}).get("has_anomaly", False)
                })
        
        # L3: 认知知识
        l3_cognitive = self.memory.get_indicator_memory(indicator_code)
        
        return {
            "L1_static": l1_static,
            "L2_recent_logs": recent_logs,
            "L3_cognitive": l3_cognitive
        }
    
    def _build_diagnostic_prompt(self) -> str:
        """构建诊断 Prompt 模板"""
        prompt = """# Role: 光伏设备根因诊断专家

## 任务
基于提供的设备历史记忆和当前异常数据，进行深度根因分析。

## 输入信息
1. **L1 静态档案**: 设备基础信息
2. **L2 动态履历**: 近期运行记录和维修历史
3. **L3 认知知识**: 已发现的规律和顽疾
4. **当前异常数据**: 今日检测到的异常特征

## 诊断流程
1. **读取记忆**: 必须仔细阅读 L1/L2/L3 所有上下文
2. **对比分析**: 当前异常 vs 历史正常模式
3. **关联排查**: 是否与其他指标异常同时发生
4. **根因推断**: 给出最可能的 2-3 个根因及置信度
5. **建议措施**: 具体的检查/维修建议

## 输出格式
```json
{
  "diagnosis_status": "confirmed/suspected/unknown",
  "root_causes": [
    {
      "cause": "根因描述",
      "confidence": 0.85,
      "evidence": "基于...证据",
      "recommended_action": "建议措施"
    }
  ],
  "similar_historical_cases": ["日期1", "日期2"],
  "requires_physical_inspection": true/false,
  "priority": "high/medium/low"
}
```

## 重要约束
- 如果记忆中没有该设备历史记录，明确说明"缺乏历史上下文，诊断置信度低"
- 不要编造历史案例，只能引用提供的 L2 履历
- 区分"设备故障"和"环境因素"（如天气）
- 给出可执行的建议，而非泛泛而谈

## 记忆内容
{memory_context}

## 当前异常
{anomaly_info}

请进行根因诊断：
"""
        return prompt
    
    def generate_diagnostic_report(
        self,
        indicator_code: str,
        analysis_results: Dict
    ) -> str:
        """
        生成诊断报告（非 LLM 版本，用于测试）
        
        实际 LLM 版本会调用外部 LLM API
        """
        insights = analysis_results.get("insights", [])
        anomalies = analysis_results.get("anomalies", [])
        change_points = analysis_results.get("change_points", [])
        
        report = f"""# 根因诊断报告

## 设备信息
- **设备 SN**: {self.device_sn}
- **分析指标**: {indicator_code}
- **诊断时间**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}

## 异常摘要
- **异常点数量**: {len(anomalies)}
- **变点检测**: {len(change_points)} 个
- **严重程度**: {"高" if len(anomalies) > 3 else "中" if len(anomalies) > 0 else "低"}

## 深度洞察
"""
        
        if insights:
            for i, insight in enumerate(insights, 1):
                report += f"{i}. {insight}\n"
        else:
            report += "暂无深度洞察，需要更多数据积累。\n"
        
        report += f"""
## 建议措施
{"- 建议进行现场检查，重点关注异常时段的设备状态" if len(anomalies) > 0 else "- 当前无明显异常，继续监测"}
- 参考历史记忆对比分析
- 如异常持续，触发详细诊断流程

## 记忆更新
此诊断结果将写入 L3 认知知识库，用于后续分析参考。
"""
        
        return report