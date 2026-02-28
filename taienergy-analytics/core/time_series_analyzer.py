"""
时间序列深度分析器
实现趋势分解、周期发现、异常检测
"""
import numpy as np
import pandas as pd
from scipy import stats
from scipy.fft import fft
from typing import Dict, List, Tuple, Optional
from core.base_analyzer import RuleEngineAnalyzer


class TimeSeriesAnalyzer(RuleEngineAnalyzer):
    """
    时间序列深度分析器
    
    功能：
    1. 趋势分解（长期趋势 + 周期性 + 残差）
    2. 变点检测（行为突变点）
    3. 周期发现（日内周期、日间周期）
    4. 异常模式识别（偏离自身规律）
    """
    
    def __init__(self, indicator_code: str, indicator_name: str):
        super().__init__(indicator_code, indicator_name)
        self.baseline_model = None  # 基线模型
        self.seasonality_period = None  # 发现的周期
        self.change_points = []  # 变点记录
        self.anomaly_history = []  # 异常历史
    
    def analyze(self, new_data: pd.DataFrame) -> Dict:
        """
        执行深度分析
        
        Returns:
            {
                "basic_stats": 基础统计,
                "trend": 趋势分析,
                "seasonality": 周期性分析,
                "change_points": 变点检测,
                "anomalies": 异常检测,
                "insights": 深度洞察
            }
        """
        if new_data.empty or len(new_data) < 5:
            return {"status": "insufficient_data", "count": len(new_data)}
        
        results = {
            "status": "ok",
            "indicator": self.indicator_code,
            "timestamp": pd.Timestamp.now().isoformat()
        }
        
        # 1. 基础统计
        results["basic_stats"] = self._basic_statistics(new_data)
        
        # 2. 趋势分析
        results["trend"] = self._analyze_trend(new_data)
        
        # 3. 周期性分析
        results["seasonality"] = self._analyze_seasonality(new_data)
        
        # 4. 变点检测（需要累积足够数据）
        if len(self.history_data) >= 3:  # 至少3天数据
            results["change_points"] = self._detect_change_points()
        
        # 5. 异常检测（基于基线模型）
        if self.baseline_model:
            results["anomalies"] = self._detect_anomalies(new_data)
        
        # 6. 生成洞察
        results["insights"] = self._generate_insights(results)
        
        return results
    
    def _basic_statistics(self, df: pd.DataFrame) -> Dict:
        """基础统计分析"""
        values = df['value'].dropna()
        
        if len(values) == 0:
            return {"status": "no_valid_data"}
        
        return {
            "count": len(values),
            "mean": float(values.mean()),
            "std": float(values.std()),
            "min": float(values.min()),
            "max": float(values.max()),
            "median": float(values.median()),
            "skewness": float(stats.skew(values)),  # 偏度
            "kurtosis": float(stats.kurtosis(values)),  # 峰度
            "non_zero_ratio": float((values > 0).sum() / len(values)),
            "zero_ratio": float((values == 0).sum() / len(values))
        }
    
    def _analyze_trend(self, df: pd.DataFrame) -> Dict:
        """
        趋势分析
        使用简单线性回归判断上升/下降/平稳
        """
        values = df['value'].dropna()
        
        if len(values) < 3:
            return {"status": "insufficient_data"}
        
        # 时间索引作为 X
        x = np.arange(len(values))
        y = values.values
        
        # 线性回归
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        
        # 判断趋势
        trend_direction = "stable"
        if p_value < 0.05:  # 显著性检验
            if slope > 0.01:
                trend_direction = "increasing"
            elif slope < -0.01:
                trend_direction = "decreasing"
        
        return {
            "direction": trend_direction,
            "slope": float(slope),
            "r_squared": float(r_value ** 2),
            "p_value": float(p_value),
            "significant": p_value < 0.05
        }
    
    def _analyze_seasonality(self, df: pd.DataFrame) -> Dict:
        """
        周期性分析
        使用自相关和傅里叶变换发现周期
        """
        values = df['value'].dropna()
        
        if len(values) < 10:
            return {"status": "insufficient_data"}
        
        # 去除零值（夜间无发电）
        non_zero = values[values > 0]
        
        if len(non_zero) < 5:
            return {"status": "no_active_period"}
        
        # 计算自相关
        autocorr = self._calculate_autocorr(non_zero.values)
        
        # 寻找峰值（周期）
        peaks = self._find_peaks(autocorr)
        
        # 傅里叶变换找主导频率
        fft_result = fft(non_zero.values)
        frequencies = np.fft.fftfreq(len(non_zero))
        
        # 找主导周期
        dominant_period = None
        if len(peaks) > 0:
            dominant_period = int(peaks[0])
        
        return {
            "has_seasonality": len(peaks) > 0,
            "dominant_period": dominant_period,
            "peak_periods": peaks[:3],  # 前3个周期
            "autocorr_max": float(np.max(autocorr[1:])) if len(autocorr) > 1 else 0,
            "active_hours": len(non_zero)
        }
    
    def _calculate_autocorr(self, x: np.ndarray, max_lags: int = 24) -> np.ndarray:
        """计算自相关函数"""
        x = x - np.mean(x)
        autocorr = np.correlate(x, x, mode='full')
        autocorr = autocorr[len(autocorr)//2:]
        autocorr = autocorr / autocorr[0]  # 归一化
        return autocorr[:max_lags]
    
    def _find_peaks(self, autocorr: np.ndarray, threshold: float = 0.3) -> List[int]:
        """从自相关中找峰值（周期）"""
        peaks = []
        for i in range(2, len(autocorr) - 1):
            if autocorr[i] > threshold and autocorr[i] > autocorr[i-1] and autocorr[i] > autocorr[i+1]:
                peaks.append(i)
        return peaks
    
    def _detect_change_points(self) -> List[Dict]:
        """
        变点检测
        检测指标行为何时发生突变
        """
        if len(self.history_data) < 3:
            return []
        
        # 合并历史数据
        all_data = pd.concat(self.history_data, ignore_index=True)
        values = all_data['value'].dropna().values
        
        if len(values) < 20:
            return []
        
        # 使用 CUSUM 算法检测变点
        change_points = self._cusum_detection(values)
        
        return change_points
    
    def _cusum_detection(self, values: np.ndarray, threshold: float = 2.0) -> List[Dict]:
        """
        CUSUM 变点检测算法
        
        检测均值突变点
        """
        mean_val = np.mean(values)
        std_val = np.std(values) if np.std(values) > 0 else 1
        
        cusum_pos = np.zeros(len(values))
        cusum_neg = np.zeros(len(values))
        
        change_points = []
        
        for i in range(1, len(values)):
            # 标准化
            z = (values[i] - mean_val) / std_val
            
            # CUSUM 统计量
            cusum_pos[i] = max(0, cusum_pos[i-1] + z - 0.5)
            cusum_neg[i] = max(0, cusum_neg[i-1] - z - 0.5)
            
            # 检测阈值突破
            if cusum_pos[i] > threshold or cusum_neg[i] > threshold:
                change_points.append({
                    "index": i,
                    "value": float(values[i]),
                    "direction": "up" if cusum_pos[i] > threshold else "down",
                    "confidence": float(min(cusum_pos[i], cusum_neg[i]) / threshold)
                })
                # 重置
                cusum_pos[i] = 0
                cusum_neg[i] = 0
        
        return change_points
    
    def _detect_anomalies(self, df: pd.DataFrame) -> List[Dict]:
        """
        异常检测
        基于基线模型识别偏离
        """
        if not self.baseline_model:
            return []
        
        values = df['value'].dropna()
        anomalies = []
        
        baseline_mean = self.baseline_model.get("mean", 0)
        baseline_std = self.baseline_model.get("std", 1)
        
        if baseline_std == 0:
            return []
        
        for idx, value in values.items():
            z_score = abs(value - baseline_mean) / baseline_std
            
            if z_score > 2.5:  # 2.5 个标准差
                anomalies.append({
                    "timestamp": idx if isinstance(idx, str) else str(idx),
                    "value": float(value),
                    "z_score": float(z_score),
                    "severity": "high" if z_score > 3 else "medium"
                })
        
        return anomalies
    
    def _generate_insights(self, results: Dict) -> List[str]:
        """生成深度洞察"""
        insights = []
        
        # 基础统计洞察
        basic = results.get("basic_stats", {})
        if basic.get("zero_ratio", 0) > 0.5:
            insights.append(f"指标 {self.indicator_code} 有超过50%时间为零值，可能存在明显的启停周期")
        
        # 趋势洞察
        trend = results.get("trend", {})
        if trend.get("significant"):
            direction = trend.get("direction")
            if direction == "increasing":
                insights.append(f"检测到显著上升趋势（R²={trend.get('r_squared', 0):.2f}）")
            elif direction == "decreasing":
                insights.append(f"检测到显著下降趋势（R²={trend.get('r_squared', 0):.2f}）")
        
        # 周期性洞察
        seasonality = results.get("seasonality", {})
        if seasonality.get("has_seasonality"):
            period = seasonality.get("dominant_period")
            if period:
                insights.append(f"发现周期性模式，主导周期约为 {period} 个时间单位")
        
        # 异常洞察
        anomalies = results.get("anomalies", [])
        if anomalies:
            high_count = sum(1 for a in anomalies if a.get("severity") == "high")
            insights.append(f"检测到 {len(anomalies)} 个异常点，其中 {high_count} 个高风险")
        
        return insights
    
    def update_model(self, new_data: pd.DataFrame):
        """更新基线模型"""
        self.history_data.append(new_data)
        
        # 累积足够数据后建立基线
        if len(self.history_data) >= 3:
            all_data = pd.concat(self.history_data, ignore_index=True)
            values = all_data['value'].dropna()
            
            if len(values) > 0:
                self.baseline_model = {
                    "mean": float(values.mean()),
                    "std": float(values.std()),
                    "median": float(values.median()),
                    "established_at": pd.Timestamp.now().isoformat()
                }