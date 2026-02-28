"""
滚动迭代分析器
逐日累积，每日对比历史发现新规律
"""
import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict


class RollingIterationAnalyzer:
    """
    滚动迭代分析器
    
    核心逻辑：
    Day 1: 建立初始认知
    Day 2: 对比Day 1，发现差异和新规律
    Day 3: 对比Day 1-2，验证/修正规律，发现新异常
    ...
    """
    
    def __init__(self):
        self.cumulative_data = {}  # 累积数据 {indicator: [day1_data, day2_data, ...]}
        self.daily_discoveries = {}  # 每日发现 {date: discoveries}
        self.cumulative_knowledge = {}  # 累积认知 {indicator: knowledge}
    
    def analyze_day(self, date_str: str, daily_data: Dict[str, pd.DataFrame]) -> Dict:
        """
        分析某一天（基于历史累积）
        
        Returns:
            {
                "day": 日期,
                "day_number": 第几天,
                "new_discoveries": 新发现,
                "confirmed_patterns": 确认的规律,
                "anomalies": 异常,
                "knowledge_update": 认知更新
            }
        """
        day_number = len(self.daily_discoveries) + 1
        
        print(f"\n{'='*60}")
        print(f"Day {day_number} 分析: {date_str}")
        print(f"{'='*60}")
        
        result = {
            "day": date_str,
            "day_number": day_number,
            "new_discoveries": [],
            "confirmed_patterns": [],
            "revised_patterns": [],
            "anomalies": [],
            "knowledge_update": {}
        }
        
        # 累积数据
        for indicator, df in daily_data.items():
            if indicator not in self.cumulative_data:
                self.cumulative_data[indicator] = []
            self.cumulative_data[indicator].append({
                "date": date_str,
                "data": df
            })
        
        # 根据天数执行不同分析
        if day_number == 1:
            # Day 1: 建立初始认知
            result["new_discoveries"] = self._day1_initial_discovery(daily_data)
        else:
            # Day 2+: 对比历史，发现新规律
            result["new_discoveries"] = self._day_n_comparative_analysis(
                date_str, daily_data, day_number
            )
        
        self.daily_discoveries[date_str] = result
        return result
    
    def _day1_initial_discovery(self, daily_data: Dict[str, pd.DataFrame]) -> List[Dict]:
        """Day 1: 初始发现"""
        discoveries = []
        
        print("  [Day 1] 建立初始认知...")
        
        for indicator, df in daily_data.items():
            values = df['value'].dropna()
            if len(values) == 0:
                continue
            
            # 基础特征
            zero_ratio = (values == 0).sum() / len(values)
            
            discovery = {
                "indicator": indicator,
                "type": "initial_baseline",
                "findings": []
            }
            
            # 发现1: 零值模式
            if zero_ratio > 0.3:
                discovery["findings"].append({
                    "aspect": "zero_pattern",
                    "description": f"零值比例 {zero_ratio:.1%}，可能存在启停周期",
                    "confidence": "medium"
                })
            
            # 发现2: 数值范围
            discovery["findings"].append({
                "aspect": "value_range",
                "description": f"观测范围 [{values.min():.2f}, {values.max():.2f}]，均值 {values.mean():.2f}",
                "confidence": "high"
            })
            
            # 发现3: 波动性
            cv = values.std() / values.mean() if values.mean() != 0 else 0
            stability = "high" if cv < 0.3 else "medium" if cv < 0.6 else "low"
            discovery["findings"].append({
                "aspect": "volatility",
                "description": f"变异系数 {cv:.2f}，稳定性评估为 {stability}",
                "confidence": "medium"
            })
            
            discoveries.append(discovery)
        
        print(f"  生成 {len(discoveries)} 个指标的初始画像")
        return discoveries
    
    def _day_n_comparative_analysis(
        self, 
        date_str: str, 
        daily_data: Dict[str, pd.DataFrame],
        day_number: int
    ) -> List[Dict]:
        """Day N: 对比历史发现新规律"""
        discoveries = []
        
        print(f"  [Day {day_number}] 对比历史，发现新规律...")
        
        for indicator, df in daily_data.items():
            values = df['value'].dropna()
            if len(values) == 0:
                continue
            
            # 获取历史数据
            history = self.cumulative_data.get(indicator, [])
            if len(history) < 2:
                continue
            
            # 历史统计（不含今天）
            historical_values = []
            for h in history[:-1]:  # 排除今天
                historical_values.extend(h["data"]['value'].dropna().tolist())
            
            if len(historical_values) < 5:
                continue
            
            historical_values = np.array(historical_values)
            today_mean = values.mean()
            hist_mean = historical_values.mean()
            hist_std = historical_values.std()
            
            discovery = {
                "indicator": indicator,
                "type": "comparative",
                "findings": []
            }
            
            # 发现1: 均值偏移（与历史对比）
            if hist_std > 0:
                z_score = (today_mean - hist_mean) / hist_std
                if abs(z_score) > 2:
                    discovery["findings"].append({
                        "aspect": "mean_shift",
                        "description": f"今日均值 {today_mean:.2f} 偏离历史均值 {hist_mean:.2f} ({z_score:+.1f}σ)",
                        "severity": "high" if abs(z_score) > 3 else "medium",
                        "confidence": "high"
                    })
                elif abs(z_score) > 1:
                    discovery["findings"].append({
                        "aspect": "mean_shift",
                        "description": f"今日均值略有偏移 ({z_score:+.1f}σ)，在合理范围内",
                        "severity": "low",
                        "confidence": "medium"
                    })
            
            # 发现2: 范围扩展（发现新的极值）
            today_min, today_max = values.min(), values.max()
            hist_min, hist_max = historical_values.min(), historical_values.max()
            
            if today_min < hist_min * 0.9 or today_max > hist_max * 1.1:
                discovery["findings"].append({
                    "aspect": "range_expansion",
                    "description": f"观测到新的极值范围 [{today_min:.2f}, {today_max:.2f}]，历史 [{hist_min:.2f}, {hist_max:.2f}]",
                    "confidence": "high"
                })
            
            # 发现3: 模式验证（第3天+）
            if day_number >= 3:
                # 检查前两天的模式是否延续
                prev_patterns = self._extract_patterns(history[:-1])
                today_pattern = self._extract_single_pattern(values)
                
                if prev_patterns and today_pattern:
                    pattern_consistency = self._compare_patterns(prev_patterns, today_pattern)
                    if pattern_consistency < 0.5:
                        discovery["findings"].append({
                            "aspect": "pattern_break",
                            "description": f"今日模式与历史不一致（一致性 {pattern_consistency:.0%}），可能存在异常",
                            "severity": "medium",
                            "confidence": "medium"
                        })
                    elif pattern_consistency > 0.8:
                        discovery["findings"].append({
                            "aspect": "pattern_confirmed",
                            "description": f"模式得到验证（一致性 {pattern_consistency:.0%}），规律稳定",
                            "confidence": "high"
                        })
            
            if discovery["findings"]:
                discoveries.append(discovery)
        
        print(f"  发现 {len(discoveries)} 个指标的新规律/异常")
        return discoveries
    
    def _extract_patterns(self, history: List[Dict]) -> Dict:
        """从历史数据提取模式"""
        all_values = []
        for h in history:
            all_values.extend(h["data"]['value'].dropna().tolist())
        
        if len(all_values) < 10:
            return {}
        
        values = np.array(all_values)
        non_zero = values[values != 0]
        
        return {
            "mean": np.mean(non_zero) if len(non_zero) > 0 else 0,
            "std": np.std(non_zero) if len(non_zero) > 0 else 0,
            "zero_ratio": (values == 0).sum() / len(values),
            "has_peak": len(non_zero) > 0 and np.max(non_zero) > np.mean(non_zero) * 2
        }
    
    def _extract_single_pattern(self, values: pd.Series) -> Dict:
        """从单日数据提取模式"""
        values = values.dropna()
        if len(values) == 0:
            return {}
        
        non_zero = values[values != 0]
        
        return {
            "mean": non_zero.mean() if len(non_zero) > 0 else 0,
            "std": non_zero.std() if len(non_zero) > 0 else 0,
            "zero_ratio": (values == 0).sum() / len(values),
            "has_peak": len(non_zero) > 0 and non_zero.max() > non_zero.mean() * 2
        }
    
    def _compare_patterns(self, historical: Dict, today: Dict) -> float:
        """对比模式相似度，返回0-1"""
        scores = []
        
        if historical.get("mean") and today.get("mean"):
            mean_diff = abs(historical["mean"] - today["mean"]) / max(historical["mean"], 0.001)
            scores.append(max(0, 1 - mean_diff))
        
        if historical.get("zero_ratio") is not None and today.get("zero_ratio") is not None:
            zero_diff = abs(historical["zero_ratio"] - today["zero_ratio"])
            scores.append(max(0, 1 - zero_diff))
        
        return np.mean(scores) if scores else 0.5
    
    def generate_iteration_report(self) -> str:
        """生成迭代分析报告"""
        report = f"""# 滚动迭代分析报告

## 分析概况
- **总天数**: {len(self.daily_discoveries)}
- **分析指标**: {len(self.cumulative_data)}

## 逐日发现

"""
        
        for date_str, result in sorted(self.daily_discoveries.items()):
            day_num = result["day_number"]
            discoveries = result["new_discoveries"]
            
            report += f"### Day {day_num} ({date_str})\n\n"
            
            if not discoveries:
                report += "- 无新发现\n\n"
                continue
            
            # 按发现类型分组
            by_type = defaultdict(list)
            for d in discoveries:
                for finding in d.get("findings", []):
                    by_type[finding.get("aspect", "unknown")].append({
                        "indicator": d["indicator"],
                        "description": finding.get("description", ""),
                        "severity": finding.get("severity", "info"),
                        "confidence": finding.get("confidence", "low")
                    })
            
            for aspect, items in by_type.items():
                report += f"**{self._translate_aspect(aspect)}**:\n"
                for item in items[:5]:  # 只显示前5个
                    severity_emoji = "🔴" if item["severity"] == "high" else "🟡" if item["severity"] == "medium" else "🟢"
                    report += f"  {severity_emoji} `{item['indicator']}`: {item['description']}\n"
                if len(items) > 5:
                    report += f"  ... 还有 {len(items) - 5} 个类似发现\n"
                report += "\n"
        
        # 累积认知
        report += "## 累积认知\n\n"
        
        # 统计各类发现
        all_findings = []
        for result in self.daily_discoveries.values():
            for d in result["new_discoveries"]:
                all_findings.extend(d.get("findings", []))
        
        aspect_counts = defaultdict(int)
        for f in all_findings:
            aspect_counts[f.get("aspect", "unknown")] += 1
        
        report += "**发现统计**:\n"
        for aspect, count in sorted(aspect_counts.items(), key=lambda x: x[1], reverse=True):
            report += f"- {self._translate_aspect(aspect)}: {count} 次\n"
        
        return report
    
    def _translate_aspect(self, aspect: str) -> str:
        """翻译发现类型"""
        translations = {
            "zero_pattern": "零值模式",
            "value_range": "数值范围",
            "volatility": "波动性",
            "mean_shift": "均值偏移",
            "range_expansion": "范围扩展",
            "pattern_break": "模式中断",
            "pattern_confirmed": "模式验证",
            "initial_baseline": "初始基线"
        }
        return translations.get(aspect, aspect)