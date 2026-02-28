"""
V4.4 周期性模式挖掘器

实现周/月级别的模式挖掘：
1. 时间序列对比（本周vs上周，本月vs上月）
2. 退化趋势识别
3. 季节性基准建立
4. 异常模式聚类
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import numpy as np
from scipy import stats


class PeriodicPatternMiner:
    """周期性模式挖掘器"""
    
    def __init__(self):
        self.seasonal_baseline_path = "memory/shared/seasonal_baseline.json"
        os.makedirs(os.path.dirname(self.seasonal_baseline_path), exist_ok=True)
    
    def compare_periods(self, current_data: List[Dict], 
                       previous_data: List[Dict],
                       period_name: str = "weekly") -> Dict:
        """
        对比两个时间段的数据
        
        Args:
            current_data: 当前周期数据
            previous_data: 上一周期数据
            period_name: 周期名称
        
        Returns:
            对比结果
        """
        result = {
            'period': period_name,
            'comparison_time': datetime.now().isoformat(),
            'current_sample_size': len(current_data),
            'previous_sample_size': len(previous_data),
            'metrics_comparison': {},
            'significant_changes': [],
            'degradation_signals': []
        }
        
        # 对比关键指标
        key_metrics = ['health_score', 'anomaly_count', 'stability_index']
        
        for metric in key_metrics:
            current_values = [d.get(metric) for d in current_data if d.get(metric) is not None]
            previous_values = [d.get(metric) for d in previous_data if d.get(metric) is not None]
            
            if not current_values or not previous_values:
                continue
            
            # 统计检验
            current_mean = np.mean(current_values)
            previous_mean = np.mean(previous_values)
            
            # t检验（如果样本足够）
            if len(current_values) >= 5 and len(previous_values) >= 5:
                t_stat, p_value = stats.ttest_ind(current_values, previous_values)
                significant = p_value < 0.05
            else:
                t_stat, p_value = None, None
                significant = abs(current_mean - previous_mean) > 5  # 简化判断
            
            result['metrics_comparison'][metric] = {
                'current_mean': round(current_mean, 2),
                'previous_mean': round(previous_mean, 2),
                'change': round(current_mean - previous_mean, 2),
                'change_percent': round((current_mean - previous_mean) / previous_mean * 100, 1) if previous_mean != 0 else 0,
                't_statistic': t_stat,
                'p_value': p_value,
                'significant': significant
            }
            
            # 记录显著变化
            if significant:
                result['significant_changes'].append({
                    'metric': metric,
                    'direction': 'increase' if current_mean > previous_mean else 'decrease',
                    'magnitude': abs(current_mean - previous_mean)
                })
            
            # 退化信号（健康相关指标下降）
            if metric in ['health_score', 'stability_index'] and current_mean < previous_mean:
                result['degradation_signals'].append({
                    'metric': metric,
                    'degradation': round(previous_mean - current_mean, 2),
                    'severity': 'high' if (previous_mean - current_mean) > 10 else 'moderate'
                })
        
        return result
    
    def identify_degradation_patterns(self, device_history: List[Dict],
                                     window_size: int = 7) -> List[Dict]:
        """
        识别退化模式
        
        Args:
            device_history: 设备历史数据
            window_size: 滑动窗口大小
        
        Returns:
            退化模式列表
        """
        patterns = []
        
        if len(device_history) < window_size * 2:
            return patterns
        
        # 滑动窗口分析
        for i in range(len(device_history) - window_size + 1):
            window = device_history[i:i + window_size]
            
            # 线性回归趋势
            x = np.arange(len(window))
            y = [d.get('health_score', 75) for d in window]
            
            if len(set(y)) <= 1:  # 无变化
                continue
            
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
            
            # 显著下降趋势
            if slope < -0.5 and r_value**2 > 0.5:  # 每天下降0.5分以上，拟合度>0.5
                patterns.append({
                    'start_date': window[0].get('date'),
                    'end_date': window[-1].get('date'),
                    'trend_slope': round(slope, 2),
                    'r_squared': round(r_value**2, 2),
                    'start_score': y[0],
                    'end_score': y[-1],
                    'pattern_type': 'linear_degradation'
                })
        
        # 去重：合并重叠的模式
        patterns = self._merge_overlapping_patterns(patterns)
        
        return patterns
    
    def build_seasonal_baseline(self, historical_data: List[Dict],
                               season: str = "summer") -> Dict:
        """
        建立季节性基准
        
        Args:
            historical_data: 历史数据
            season: 季节（summer/winter/spring/autumn）
        
        Returns:
            季节性基准
        """
        baseline = {
            'season': season,
            'built_at': datetime.now().isoformat(),
            'data_points': len(historical_data),
            'metrics': {}
        }
        
        # 计算各指标的统计基准
        metrics = ['health_score', 'anomaly_count', 'efficiency', 'power_factor']
        
        for metric in metrics:
            values = [d.get(metric) for d in historical_data if d.get(metric) is not None]
            
            if not values:
                continue
            
            baseline['metrics'][metric] = {
                'mean': round(np.mean(values), 2),
                'std': round(np.std(values), 2),
                'median': round(np.median(values), 2),
                'percentile_5': round(np.percentile(values, 5), 2),
                'percentile_95': round(np.percentile(values, 95), 2),
                'min': round(np.min(values), 2),
                'max': round(np.max(values), 2)
            }
        
        # 保存基准
        self._save_seasonal_baseline(season, baseline)
        
        return baseline
    
    def detect_anomaly_clusters(self, anomaly_records: List[Dict],
                               time_window_hours: int = 24) -> List[Dict]:
        """
        异常事件聚类
        
        Args:
            anomaly_records: 异常记录列表
            time_window_hours: 时间窗口（小时）
        
        Returns:
            异常聚类
        """
        if not anomaly_records:
            return []
        
        # 按时间排序
        sorted_records = sorted(
            anomaly_records,
            key=lambda x: x.get('timestamp', '2000-01-01 00:00:00')
        )
        
        clusters = []
        current_cluster = [sorted_records[0]]
        
        for i in range(1, len(sorted_records)):
            prev_time = datetime.fromisoformat(
                current_cluster[-1].get('timestamp', '2000-01-01T00:00:00')
            )
            curr_time = datetime.fromisoformat(
                sorted_records[i].get('timestamp', '2000-01-01T00:00:00')
            )
            
            # 如果在时间窗口内，加入当前聚类
            if (curr_time - prev_time).total_seconds() <= time_window_hours * 3600:
                current_cluster.append(sorted_records[i])
            else:
                # 保存当前聚类，开始新聚类
                if len(current_cluster) >= 2:  # 至少2个异常才算聚类
                    clusters.append(self._summarize_cluster(current_cluster))
                current_cluster = [sorted_records[i]]
        
        # 处理最后一个聚类
        if len(current_cluster) >= 2:
            clusters.append(self._summarize_cluster(current_cluster))
        
        return clusters
    
    def _merge_overlapping_patterns(self, patterns: List[Dict]) -> List[Dict]:
        """合并重叠的退化模式"""
        if not patterns:
            return patterns
        
        # 按开始时间排序
        patterns = sorted(patterns, key=lambda x: x['start_date'])
        
        merged = [patterns[0]]
        
        for current in patterns[1:]:
            last = merged[-1]
            
            # 检查是否重叠
            if current['start_date'] <= last['end_date']:
                # 合并：扩展结束时间，取平均斜率
                last['end_date'] = max(last['end_date'], current['end_date'])
                last['trend_slope'] = (last['trend_slope'] + current['trend_slope']) / 2
                last['end_score'] = current['end_score']
            else:
                merged.append(current)
        
        return merged
    
    def _summarize_cluster(self, cluster: List[Dict]) -> Dict:
        """汇总异常聚类"""
        devices = set(r.get('device_sn') for r in cluster)
        types = set(r.get('anomaly_type') for r in cluster)
        
        start_time = min(r.get('timestamp', '2000-01-01T00:00:00') for r in cluster)
        end_time = max(r.get('timestamp', '2000-01-01T00:00:00') for r in cluster)
        
        return {
            'start_time': start_time,
            'end_time': end_time,
            'anomaly_count': len(cluster),
            'affected_devices': list(devices),
            'device_count': len(devices),
            'anomaly_types': list(types),
            'pattern_description': f"{len(devices)}台设备在{len(cluster)}个异常事件中呈现{', '.join(types)}模式"
        }
    
    def _save_seasonal_baseline(self, season: str, baseline: Dict):
        """保存季节性基准"""
        all_baselines = {}
        
        if os.path.exists(self.seasonal_baseline_path):
            with open(self.seasonal_baseline_path, 'r') as f:
                all_baselines = json.load(f)
        
        all_baselines[season] = baseline
        
        with open(self.seasonal_baseline_path, 'w') as f:
            json.dump(all_baselines, f, indent=2)
    
    def load_seasonal_baseline(self, season: str) -> Optional[Dict]:
        """加载季节性基准"""
        if not os.path.exists(self.seasonal_baseline_path):
            return None
        
        with open(self.seasonal_baseline_path, 'r') as f:
            all_baselines = json.load(f)
        
        return all_baselines.get(season)


# 便捷函数
def compare_weekly_data(current_week: List[Dict], last_week: List[Dict]) -> Dict:
    """对比两周数据"""
    miner = PeriodicPatternMiner()
    return miner.compare_periods(current_week, last_week, "weekly")


def detect_degradation_trends(device_history: List[Dict]) -> List[Dict]:
    """检测退化趋势"""
    miner = PeriodicPatternMiner()
    return miner.identify_degradation_patterns(device_history)


def build_seasonal_baseline(historical_data: List[Dict], season: str) -> Dict:
    """建立季节性基准"""
    miner = PeriodicPatternMiner()
    return miner.build_seasonal_baseline(historical_data, season)