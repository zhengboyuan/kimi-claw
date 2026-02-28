"""
Skill 10: 资产简报生成器
生成每日分析报告（支持中文属性名称）
"""
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
from utils.memory_manager import MemoryManager


class DailyReporter:
    """
    资产简报生成器
    
    职责：
    1. 汇总当日所有指标分析结果
    2. 生成简洁/详细两种报告
    3. 正常时发"无异常"，异常时发详细诊断
    4. 使用中文属性名称展示
    """
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.memory = MemoryManager(device_sn)
    
    def generate_report(
        self,
        date_str: str,
        analysis_results: Dict[str, Dict],
        data_quality_report: Dict
    ) -> Dict[str, str]:
        """
        生成日报
        
        Returns:
            {
                "summary": 简洁版（正常情况）,
                "detailed": 详细版（异常情况）,
                "status": "normal/abnormal/error"
            }
        """
        # 判断整体状态
        has_anomaly = self._check_anomalies(analysis_results)
        has_error = data_quality_report.get("quality_score", 100) < 60
        
        if has_error:
            status = "error"
        elif has_anomaly:
            status = "abnormal"
        else:
            status = "normal"
        
        # 生成报告
        summary = self._generate_summary(date_str, analysis_results, status)
        detailed = self._generate_detailed(date_str, analysis_results, data_quality_report, status)
        
        return {
            "summary": summary,
            "detailed": detailed,
            "status": status
        }
    
    def _check_anomalies(self, analysis_results: Dict[str, Dict]) -> bool:
        """检查是否有异常"""
        for indicator, result in analysis_results.items():
            anomalies = result.get("anomalies", [])
            if anomalies and len(anomalies) > 0:
                return True
            
            insights = result.get("insights", [])
            for insight in insights:
                if "异常" in insight or "高风险" in insight:
                    return True
        
        return False
    
    def _generate_summary(
        self,
        date_str: str,
        analysis_results: Dict[str, Dict],
        status: str
    ) -> str:
        """生成简洁报告"""
        if status == "normal":
            return f"""# 资产日报 ({date_str})

## 状态: ✅ 正常

设备 {self.device_sn} 今日运行正常，无异常指标。

## 关键指标速览
{self._format_key_indicators(analysis_results)}

---
*自动生成于 {datetime.now().strftime('%H:%M')}*
"""
        
        elif status == "abnormal":
            anomaly_count = sum(
                len(r.get("anomalies", [])) 
                for r in analysis_results.values()
            )
            return f"""# 资产日报 ({date_str})

## 状态: ⚠️ 异常

设备 {self.device_sn} 检测到 **{anomaly_count}** 个异常点，建议关注。

## 异常摘要
{self._format_anomaly_summary(analysis_results)}

## 详细诊断
请查看详细报告。

---
*自动生成于 {datetime.now().strftime('%H:%M')}*
"""
        
        else:  # error
            return f"""# 资产日报 ({date_str})

## 状态: ❌ 数据异常

设备 {self.device_sn} 数据质量异常，无法完成分析。

## 问题
- 数据质量评分低于 60 分
- 可能原因：传感器故障、通信中断

## 建议
- 检查设备通信状态
- 验证传感器数据

---
*自动生成于 {datetime.now().strftime('%H:%M')}*
"""
    
    def _generate_detailed(
        self,
        date_str: str,
        analysis_results: Dict[str, Dict],
        data_quality_report: Dict,
        status: str
    ) -> str:
        """生成详细报告"""
        report = f"""# 资产详细分析报告 ({date_str})

## 设备信息
- **设备 SN**: {self.device_sn}
- **报告时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}
- **数据质量**: {data_quality_report.get('quality_score', 0):.1f}/100

## 整体状态: {"✅ 正常" if status == "normal" else "⚠️ 异常" if status == "abnormal" else "❌ 数据错误"}

---

## 指标详细分析

"""
        
        # 每个指标的详细分析
        for indicator, result in analysis_results.items():
            report += self._format_indicator_detail(indicator, result)
            report += "\n---\n\n"
        
        # 记忆更新部分
        report += f"""## 记忆更新

本次分析结果已写入临时记忆文件：
- 文件路径: `{self.memory.analysis_file}`
- 更新内容: 各指标行为模型、异常记录

## 下一步行动

{"继续监测，明日对比分析" if status == "normal" else "建议进行根因诊断，查看详细异常时段数据"}

---
*此报告由光伏资产智能运维系统自动生成*
"""
        
        return report
    
    def _format_key_indicators(self, analysis_results: Dict[str, Dict]) -> str:
        """格式化关键指标（使用中文名称）"""
        lines = []
        
        # 显示前5个指标
        count = 0
        for indicator, result in analysis_results.items():
            if count >= 5:
                break
            basic = result.get("basic_stats", {})
            mean_val = basic.get("mean", 0)
            unit = result.get("unit", "")
            unit_str = f" {unit}" if unit else ""
            lines.append(f"- **{indicator}**: {mean_val:.2f}{unit_str}")
            count += 1
        
        return "\n".join(lines) if lines else "- 暂无关键指标数据"
    
    def _format_anomaly_summary(self, analysis_results: Dict[str, Dict]) -> str:
        """格式化异常摘要"""
        lines = []
        
        for indicator, result in analysis_results.items():
            anomalies = result.get("anomalies", [])
            if anomalies:
                high_risk = sum(1 for a in anomalies if a.get("severity") == "high")
                lines.append(f"- **{indicator}**: {len(anomalies)} 个异常点（{high_risk} 个高风险）")
            
            insights = result.get("insights", [])
            for insight in insights:
                if "异常" in insight or "趋势" in insight:
                    lines.append(f"  - {insight}")
        
        return "\n".join(lines) if lines else "- 暂无异常详情"
    
    def _format_indicator_detail(self, indicator: str, result: Dict) -> str:
        """格式化单个指标详情"""
        basic = result.get("basic_stats", {})
        trend = result.get("trend", {})
        seasonality = result.get("seasonality", {})
        anomalies = result.get("anomalies", [])
        insights = result.get("insights", [])
        unit = result.get("unit", "")
        code = result.get("code", "")  # 原始代码
        
        # 构建指标标题（中文名称 + 原始代码）
        title = indicator
        if code and code != indicator:
            title = f"{indicator} ({code})"
        
        unit_str = f" {unit}" if unit else ""
        
        detail = f"""### {title}

**基础统计**:
- 数据点数: {basic.get('count', 0)}
- 均值: {basic.get('mean', 0):.3f}{unit_str}
- 标准差: {basic.get('std', 0):.3f}{unit_str}
- 范围: [{basic.get('min', 0):.3f}{unit_str}, {basic.get('max', 0):.3f}{unit_str}]

**趋势分析**:
- 方向: {trend.get('direction', 'unknown')}
- 显著性: {'是' if trend.get('significant') else '否'}

**周期性**:
- 有周期性: {'是' if seasonality.get('has_seasonality') else '否'}
- 主导周期: {seasonality.get('dominant_period', 'N/A')}

**异常检测**:
- 异常点数量: {len(anomalies)}
"""
        
        if anomalies:
            detail += "- 异常详情:\n"
            for a in anomalies[:3]:  # 只显示前3个
                detail += f"  - {a.get('timestamp')}: {a.get('value'):.3f}{unit_str} (Z={a.get('z_score'):.2f})\n"
        
        if insights:
            detail += "\n**深度洞察**:\n"
            for i, insight in enumerate(insights, 1):
                detail += f"{i}. {insight}\n"
        
        return detail
