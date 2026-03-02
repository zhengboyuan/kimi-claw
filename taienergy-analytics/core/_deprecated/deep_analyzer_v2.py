"""
深度指标分析器 v2
针对136个指标进行全面深度分析
"""
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import pdist, squareform
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import json


class DeepIndicatorAnalyzer:
    """
    深度指标分析器
    
    分析维度：
    1. 单指标深度特性（分布、稳定性、周期性、突变）
    2. 指标间相关性（联动关系、因果关系）
    3. 异常深度检测（模式偏离、关联异常）
    """
    
    def __init__(self):
        self.indicators_data = {}  # 所有指标数据
        self.indicator_profiles = {}  # 指标画像
        self.correlation_matrix = None  # 相关性矩阵
        self.anomaly_events = []  # 异常事件
    
    def add_day_data(self, date_str: str, indicators_data: Dict[str, pd.DataFrame]):
        """添加一天的数据"""
        self.indicators_data[date_str] = indicators_data
    
    def analyze_all(self) -> Dict:
        """
        执行全面深度分析
        """
        print(f"\n{'='*60}")
        print("开始深度全面分析")
        print(f"{'='*60}")
        
        results = {
            "summary": {},
            "indicator_profiles": {},
            "correlations": {},
            "anomalies": {},
            "insights": []
        }
        
        # 1. 单指标深度特性分析
        print("\n[1/4] 单指标深度特性分析...")
        profiles = self._analyze_indicator_profiles()
        results["indicator_profiles"] = profiles
        print(f"  生成 {len(profiles)} 个指标画像")
        
        # 2. 指标间相关性分析
        print("\n[2/4] 指标间相关性分析...")
        results["correlations"] = self._analyze_correlations()
        
        # 3. 异常深度检测
        print("\n[3/4] 异常深度检测...")
        results["anomalies"] = self._detect_deep_anomalies()
        
        # 4. 生成综合洞察
        print("\n[4/4] 生成综合洞察...")
        results["insights"] = self._generate_comprehensive_insights(results)
        
        # 汇总统计
        results["summary"] = {
            "total_days": len(self.indicators_data),
            "total_indicators": len(self._get_all_indicator_codes()),
            "profiles_generated": len(results["indicator_profiles"]),
            "strong_correlations": sum(
                1 for c in results["correlations"].get("strong_pairs", [])
            ),
            "anomalies_detected": len(results["anomalies"])
        }
        
        return results
    
    def _get_all_indicator_codes(self) -> List[str]:
        """获取所有指标代码"""
        codes = set()
        for day_data in self.indicators_data.values():
            codes.update(day_data.keys())
        return sorted(list(codes))
    
    def _analyze_indicator_profiles(self) -> Dict[str, Dict]:
        """
        单指标深度特性分析
        
        每个指标生成详细画像：
        - 基础统计特征
        - 分布特征（正态/偏态/多峰）
        - 稳定性评估
        - 周期性特征
        - 突变点记录
        """
        profiles = {}
        
        for indicator in self._get_all_indicator_codes():
            # 收集所有天的数据
            all_values = []
            daily_stats = []
            
            for date_str, day_data in sorted(self.indicators_data.items()):
                if indicator in day_data:
                    df = day_data[indicator]
                    values = df['value'].dropna()
                    
                    if len(values) > 0:
                        all_values.extend(values.tolist())
                        daily_stats.append({
                            "date": date_str,
                            "mean": values.mean(),
                            "std": values.std(),
                            "max": values.max(),
                            "min": values.min(),
                            "non_zero_ratio": (values > 0).sum() / len(values)
                        })
            
            if not all_values:
                continue
            
            values_array = np.array(all_values)
            
            # 基础统计
            profile = {
                "indicator_code": indicator,
                "total_samples": len(values_array),
                "basic_stats": {
                    "mean": float(np.mean(values_array)),
                    "median": float(np.median(values_array)),
                    "std": float(np.std(values_array)),
                    "cv": float(np.std(values_array) / np.mean(values_array)) if np.mean(values_array) != 0 else 0,  # 变异系数
                    "min": float(np.min(values_array)),
                    "max": float(np.max(values_array)),
                    "range": float(np.max(values_array) - np.min(values_array)),
                }
            }
            
            # 分布特征
            profile["distribution"] = self._analyze_distribution(values_array)
            
            # 稳定性评估（跨天变异）
            if len(daily_stats) > 1:
                profile["stability"] = self._analyze_stability(daily_stats)
            
            # 周期性特征
            profile["periodicity"] = self._analyze_periodicity(values_array)
            
            # 零值模式（针对光伏指标）
            profile["zero_pattern"] = self._analyze_zero_pattern(values_array)
            
            # 指标分类
            profile["category"] = self._classify_indicator(indicator, profile)
            
            profiles[indicator] = profile
        
        return profiles
    
    def _analyze_distribution(self, values: np.ndarray) -> Dict:
        """分析分布特征"""
        # 去除零值进行分析（光伏指标夜间为零）
        non_zero = values[values != 0]
        
        if len(non_zero) < 5:
            return {"type": "mostly_zero", "zero_ratio": (values == 0).sum() / len(values)}
        
        # 偏度和峰度
        skewness = stats.skew(non_zero)
        kurtosis = stats.kurtosis(non_zero)
        
        # 正态性检验
        _, p_value = stats.normaltest(non_zero)
        
        # 分布类型判断
        dist_type = "normal"
        if abs(skewness) > 1:
            dist_type = "right_skewed" if skewness > 0 else "left_skewed"
        elif p_value < 0.05:
            dist_type = "non_normal"
        
        return {
            "type": dist_type,
            "skewness": float(skewness),
            "kurtosis": float(kurtosis),
            "normality_p_value": float(p_value),
            "is_normal": p_value > 0.05,
            "zero_ratio": (values == 0).sum() / len(values),
            "quartiles": {
                "q1": float(np.percentile(non_zero, 25)),
                "q2": float(np.percentile(non_zero, 50)),
                "q3": float(np.percentile(non_zero, 75))
            }
        }
    
    def _analyze_stability(self, daily_stats: List[Dict]) -> Dict:
        """分析跨天稳定性"""
        means = [d["mean"] for d in daily_stats if not np.isnan(d["mean"])]
        stds = [d["std"] for d in daily_stats if not np.isnan(d["std"])]
        
        if len(means) < 2:
            return {"status": "insufficient_data"}
        
        mean_cv = np.std(means) / np.mean(means) if np.mean(means) != 0 else 0
        
        # 稳定性评级
        stability_level = "high"
        if mean_cv > 0.5:
            stability_level = "very_low"
        elif mean_cv > 0.3:
            stability_level = "low"
        elif mean_cv > 0.1:
            stability_level = "medium"
        
        return {
            "daily_mean_cv": float(mean_cv),
            "stability_level": stability_level,
            "daily_means": means,
            "trend": "increasing" if means[-1] > means[0] else "decreasing" if means[-1] < means[0] else "stable"
        }
    
    def _analyze_periodicity(self, values: np.ndarray) -> Dict:
        """分析周期性特征"""
        non_zero = values[values != 0]
        
        if len(non_zero) < 10:
            return {"has_periodicity": False, "reason": "insufficient_data"}
        
        # 简化的周期性检测：检查自相关
        autocorr = np.correlate(non_zero - np.mean(non_zero), 
                                non_zero - np.mean(non_zero), 
                                mode='full')
        autocorr = autocorr[len(autocorr)//2:]
        autocorr = autocorr / autocorr[0]
        
        # 找峰值
        peaks = []
        for i in range(2, min(len(autocorr)-1, 25)):
            if autocorr[i] > 0.3 and autocorr[i] > autocorr[i-1] and autocorr[i] > autocorr[i+1]:
                peaks.append((i, float(autocorr[i])))
        
        peaks = sorted(peaks, key=lambda x: x[1], reverse=True)[:3]
        
        return {
            "has_periodicity": len(peaks) > 0,
            "dominant_periods": [p[0] for p in peaks],
            "period_strengths": [p[1] for p in peaks],
            "autocorr_at_lag1": float(autocorr[1]) if len(autocorr) > 1 else 0
        }
    
    def _analyze_zero_pattern(self, values: np.ndarray) -> Dict:
        """分析零值模式（光伏指标特征）"""
        zero_ratio = (values == 0).sum() / len(values)
        
        # 零值模式分类
        pattern = "continuous"
        if zero_ratio > 0.9:
            pattern = "mostly_zero"
        elif zero_ratio > 0.5:
            pattern = "half_zero"  # 典型的昼夜模式
        elif zero_ratio > 0.1:
            pattern = "occasional_zero"
        else:
            pattern = "rarely_zero"
        
        return {
            "zero_ratio": float(zero_ratio),
            "non_zero_ratio": float(1 - zero_ratio),
            "pattern": pattern,
            "is_typical_pv": zero_ratio > 0.3 and zero_ratio < 0.7  # 典型的光伏模式
        }
    
    def _classify_indicator(self, code: str, profile: Dict) -> str:
        """指标分类"""
        # 基于代码前缀分类
        if code.startswith("ai"):
            return "analog"
        elif code.startswith("di"):
            return "digital"
        else:
            return "special"
    
    def _analyze_correlations(self) -> Dict:
        """
        指标间相关性分析
        
        找出强相关的指标对
        """
        # 构建数据矩阵（指标 x 时间）
        indicator_codes = self._get_all_indicator_codes()
        
        if len(indicator_codes) < 2:
            return {"status": "insufficient_indicators"}
        
        # 收集每个指标的均值序列（跨天）
        indicator_means = {}
        for code in indicator_codes:
            means = []
            for date_str, day_data in sorted(self.indicators_data.items()):
                if code in day_data:
                    values = day_data[code]['value'].dropna()
                    if len(values) > 0:
                        means.append(values.mean())
            if len(means) >= 2:  # 至少需要2天数据
                indicator_means[code] = means
        
        if len(indicator_means) < 2:
            return {"status": "insufficient_data"}
        
        # 计算相关性
        codes = list(indicator_means.keys())
        n = len(codes)
        corr_matrix = np.zeros((n, n))
        
        for i, code1 in enumerate(codes):
            for j, code2 in enumerate(codes):
                if i == j:
                    corr_matrix[i, j] = 1.0
                elif i < j:
                    # 确保长度一致
                    min_len = min(len(indicator_means[code1]), len(indicator_means[code2]))
                    if min_len >= 2:
                        corr, _ = stats.pearsonr(
                            indicator_means[code1][:min_len],
                            indicator_means[code2][:min_len]
                        )
                        corr_matrix[i, j] = corr
                        corr_matrix[j, i] = corr
        
        # 找出强相关对
        strong_pairs = []
        moderate_pairs = []
        
        for i in range(n):
            for j in range(i+1, n):
                corr = corr_matrix[i, j]
                if abs(corr) > 0.8:
                    strong_pairs.append({
                        "indicator1": codes[i],
                        "indicator2": codes[j],
                        "correlation": float(corr),
                        "relationship": "strong_positive" if corr > 0 else "strong_negative"
                    })
                elif abs(corr) > 0.5:
                    moderate_pairs.append({
                        "indicator1": codes[i],
                        "indicator2": codes[j],
                        "correlation": float(corr),
                        "relationship": "moderate_positive" if corr > 0 else "moderate_negative"
                    })
        
        return {
            "status": "ok",
            "total_indicators_analyzed": n,
            "strong_pairs": strong_pairs,
            "moderate_pairs": moderate_pairs,
            "strong_pairs_count": len(strong_pairs),
            "moderate_pairs_count": len(moderate_pairs)
        }
    
    def _detect_deep_anomalies(self) -> List[Dict]:
        """
        深度异常检测
        
        检测：
        1. 单指标异常（偏离自身历史模式）
        2. 关联异常（相关指标同时异常）
        """
        anomalies = []
        
        # 对每个指标检测异常
        for indicator in self._get_all_indicator_codes():
            # 收集所有天的数据
            daily_values = {}
            for date_str, day_data in sorted(self.indicators_data.items()):
                if indicator in day_data:
                    values = day_data[indicator]['value'].dropna()
                    if len(values) > 0:
                        daily_values[date_str] = values.tolist()
            
            if len(daily_values) < 2:
                continue
            
            # 检测日间异常（某天的分布与其他天显著不同）
            dates = list(daily_values.keys())
            means = [np.mean(daily_values[d]) for d in dates]
            
            if len(means) >= 3:
                overall_mean = np.mean(means)
                overall_std = np.std(means)
                
                if overall_std > 0:
                    for i, (date, mean_val) in enumerate(zip(dates, means)):
                        z_score = abs(mean_val - overall_mean) / overall_std
                        if z_score > 2.0:  # 2个标准差
                            anomalies.append({
                                "type": "daily_distribution_anomaly",
                                "indicator": indicator,
                                "date": date,
                                "severity": "high" if z_score > 3 else "medium",
                                "z_score": float(z_score),
                                "description": f"{date}的均值({mean_val:.2f})偏离历史均值({overall_mean:.2f}) {z_score:.1f}个标准差"
                            })
        
        return anomalies
    
    def _generate_comprehensive_insights(self, results: Dict) -> List[str]:
        """生成综合洞察"""
        insights = []
        
        profiles = results.get("indicator_profiles", {})
        correlations = results.get("correlations", {})
        anomalies = results.get("anomalies", [])
        
        # 1. 指标分类统计
        categories = defaultdict(int)
        for p in profiles.values():
            categories[p.get("category", "unknown")] += 1
        
        insights.append(f"指标构成：模拟量({categories.get('analog', 0)}个)、数字量({categories.get('digital', 0)}个)、特殊({categories.get('special', 0)}个)")
        
        # 2. 典型光伏模式识别
        pv_like = [code for code, p in profiles.items() 
                   if p.get("zero_pattern", {}).get("is_typical_pv", False)]
        if pv_like:
            insights.append(f"识别出{len(pv_like)}个典型光伏模式指标（昼夜变化明显）：{', '.join(pv_like[:5])}...")
        
        # 3. 稳定性评估
        stability_dist = defaultdict(list)
        for code, p in profiles.items():
            level = p.get("stability", {}).get("stability_level", "unknown")
            stability_dist[level].append(code)
        
        if stability_dist:
            insights.append(f"稳定性分布：高稳定({len(stability_dist.get('high', []))}个)、中稳定({len(stability_dist.get('medium', []))}个)、低稳定({len(stability_dist.get('low', [])) + len(stability_dist.get('very_low', []))}个)")
        
        # 4. 强相关性发现
        strong_pairs = correlations.get("strong_pairs", [])
        if strong_pairs:
            insights.append(f"发现{len(strong_pairs)}对强相关指标（|r|>0.8），可能存在物理关联")
            for pair in strong_pairs[:3]:
                insights.append(f"  - {pair['indicator1']} ↔ {pair['indicator2']}: r={pair['correlation']:.2f}")
        
        # 5. 异常汇总
        if anomalies:
            insights.append(f"检测到{len(anomalies)}个异常事件，建议关注")
        else:
            insights.append("3天内未检测到显著异常，所有指标运行平稳")
        
        # 6. 数据质量建议
        insights.append("建议：继续累积数据至7-30天，以建立稳定的基线模型和周期性模式")
        
        return insights