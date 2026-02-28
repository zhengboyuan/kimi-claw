"""
逆变器集群横向对比分析器 (Cluster Benchmark Analyzer)

职责：
1. 管理16台逆变器集群 (XHDL_1NBQ ~ XHDL_16NBQ)
2. 每日计算群体基准 (中位数)
3. 计算各设备偏离度 (Z-score)
4. 设备健康排名与异常标记
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime


class ClusterBenchmarkAnalyzer:
    """
    逆变器集群横向对比分析器
    
    设备集群: XHDL_1NBQ 到 XHDL_16NBQ
    """
    
    # 16台逆变器SN列表
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    # 关键对比指标
    BENCHMARK_INDICATORS = {
        'string_consistency': {
            'name': '组串一致性',
            'codes': ['ai10', 'ai12', 'ai16', 'ai20'],
            'metric': 'std/mean'  # 变异系数
        },
        'efficiency': {
            'name': '转换效率',
            'codes': ['ai45', 'ai56'],
            'metric': 'ai56/ai45'
        },
        'grid_quality': {
            'name': '电网质量',
            'codes': ['ai49', 'ai50', 'ai51'],
            'metric': 'unbalance'
        }
    }
    
    def __init__(self):
        self.device_scores = {sn: {
            'daily_scores': [],  # 每日综合评分
            'rank_history': [],  # 排名历史
            'anomaly_count': 0,  # 异常天数
            'deviation_history': []  # 偏离度历史
        } for sn in self.DEVICE_CLUSTER}
    
    def analyze_cluster_daily(self, device_data: Dict[str, pd.DataFrame], 
                             date_str: str) -> Dict:
        """
        每日集群对比分析
        
        Args:
            device_data: {sn: df_day, ...}
            date_str: 日期
        
        Returns:
            {
                'cluster_median': {...},
                'device_scores': {...},
                'ranking': [...],
                'outliers': [...]
            }
        """
        results = {
            'date': date_str,
            'cluster_stats': {},
            'device_metrics': {},
            'ranking': [],
            'outliers': []
        }
        
        # 1. 计算每台设备的指标
        for sn, df_day in device_data.items():
            if df_day is None or df_day.empty:
                continue
            
            metrics = self._calculate_device_metrics(df_day)
            results['device_metrics'][sn] = metrics
        
        # 2. 计算群体基准 (中位数)
        if results['device_metrics']:
            results['cluster_stats'] = self._calculate_cluster_median(
                results['device_metrics']
            )
        
        # 3. 计算偏离度并排名
        if results['cluster_stats']:
            scores = self._calculate_deviation_scores(
                results['device_metrics'],
                results['cluster_stats']
            )
            
            # 排序生成排名
            sorted_devices = sorted(
                scores.items(),
                key=lambda x: x[1]['composite_score'],
                reverse=True
            )
            
            results['ranking'] = [
                {
                    'rank': i + 1,
                    'sn': sn,
                    'score': info['composite_score'],
                    'deviation': info['max_deviation']
                }
                for i, (sn, info) in enumerate(sorted_devices)
            ]
            
            # 更新历史记录
            for item in results['ranking']:
                self.device_scores[item['sn']]['rank_history'].append({
                    'date': date_str,
                    'rank': item['rank']
                })
                self.device_scores[item['sn']]['daily_scores'].append({
                    'date': date_str,
                    'score': item['score']
                })
            
            # 4. 标记异常设备 (>2σ偏离)
            for sn, info in scores.items():
                if info['max_deviation'] > 2.0:  # 2σ
                    results['outliers'].append({
                        'sn': sn,
                        'deviation': info['max_deviation'],
                        'severity': 'critical' if info['max_deviation'] > 3.0 else 'warning'
                    })
                    self.device_scores[sn]['anomaly_count'] += 1
                
                self.device_scores[sn]['deviation_history'].append({
                    'date': date_str,
                    'deviation': info['max_deviation']
                })
        
        return results
    
    def _calculate_device_metrics(self, df_day: pd.DataFrame) -> Dict:
        """计算单设备指标"""
        metrics = {}
        
        # 1. 组串一致性 (变异系数)
        pv_cols = ['ai10', 'ai12', 'ai16', 'ai20']
        pv_data = df_day[[c for c in pv_cols if c in df_day.columns]].replace(0, np.nan)
        if not pv_data.empty:
            cv = pv_data.std(axis=1) / pv_data.mean(axis=1)
            metrics['string_cv'] = float(cv.mean())
        
        # 2. 转换效率
        if 'ai45' in df_day.columns and 'ai56' in df_day.columns:
            efficiency = df_day['ai56'] / df_day['ai45'].replace(0, np.nan)
            efficiency = efficiency[(efficiency > 0.5) & (efficiency < 1.0)]
            if not efficiency.empty:
                metrics['efficiency'] = float(efficiency.mean())
        
        # 3. 电网不平衡度
        vol_cols = ['ai49', 'ai50', 'ai51']
        vol_data = df_day[[c for c in vol_cols if c in df_day.columns]]
        if len(vol_data.columns) == 3:
            mean_v = vol_data.mean(axis=1)
            unbalance = (vol_data.max(axis=1) - vol_data.min(axis=1)) / mean_v
            metrics['unbalance'] = float(unbalance.mean())
        
        return metrics
    
    def _calculate_cluster_median(self, device_metrics: Dict) -> Dict:
        """计算群体基准 (中位数)"""
        cluster_stats = {}
        
        for metric_name in ['string_cv', 'efficiency', 'unbalance']:
            values = [m[metric_name] for m in device_metrics.values() 
                     if metric_name in m and not np.isnan(m[metric_name])]
            if values:
                cluster_stats[metric_name] = {
                    'median': float(np.median(values)),
                    'std': float(np.std(values)),
                    'min': float(np.min(values)),
                    'max': float(np.max(values))
                }
        
        return cluster_stats
    
    def _calculate_deviation_scores(self, device_metrics: Dict, 
                                    cluster_stats: Dict) -> Dict:
        """计算偏离度评分"""
        scores = {}
        
        for sn, metrics in device_metrics.items():
            deviations = []
            metric_scores = {}
            
            for metric_name in ['string_cv', 'efficiency', 'unbalance']:
                if metric_name in metrics and metric_name in cluster_stats:
                    value = metrics[metric_name]
                    median = cluster_stats[metric_name]['median']
                    std = cluster_stats[metric_name]['std']
                    
                    if std > 0:
                        # Z-score
                        z_score = abs(value - median) / std
                        deviations.append(z_score)
                        metric_scores[metric_name] = z_score
            
            # 综合评分 (越高越好，所以用负的偏离度)
            max_dev = max(deviations) if deviations else 0
            composite = max(0, 100 - max_dev * 20)  # 偏离度转评分
            
            scores[sn] = {
                'composite_score': composite,
                'max_deviation': max_dev,
                'metric_scores': metric_scores
            }
        
        return scores
    
    def generate_cluster_report(self) -> str:
        """生成集群健康报告"""
        report = []
        report.append("=" * 70)
        report.append("逆变器集群横向对比报告")
        report.append("=" * 70)
        
        # 1. 设备异常统计
        report.append("\n【设备异常统计】")
        anomaly_list = [(sn, info['anomaly_count']) 
                       for sn, info in self.device_scores.items()]
        anomaly_list.sort(key=lambda x: x[1], reverse=True)
        
        for sn, count in anomaly_list[:5]:
            if count > 0:
                report.append(f"  ⚠️ {sn}: 异常{count}天")
        
        # 2. 平均排名
        report.append("\n【平均排名】")
        avg_ranks = []
        for sn, info in self.device_scores.items():
            if info['rank_history']:
                avg_rank = np.mean([r['rank'] for r in info['rank_history']])
                avg_ranks.append((sn, avg_rank))
        
        avg_ranks.sort(key=lambda x: x[1])
        for sn, rank in avg_ranks[:5]:
            report.append(f"  {sn}: 平均排名{rank:.1f}")
        
        # 3. 最差设备
        report.append("\n【需关注设备】")
        worst = self.get_worst_device()
        if worst:
            report.append(f"  🔴 {worst['sn']}")
            report.append(f"     平均排名: {worst['avg_rank']:.1f}")
            report.append(f"     异常天数: {worst['anomaly_count']}")
        
        report.append("\n" + "=" * 70)
        return "\n".join(report)
    
    def get_worst_device(self) -> Optional[Dict]:
        """获取表现最差的设备"""
        worst = None
        worst_score = float('inf')
        
        for sn, info in self.device_scores.items():
            if info['rank_history']:
                avg_rank = np.mean([r['rank'] for r in info['rank_history']])
                # 排名+异常惩罚
                score = avg_rank + info['anomaly_count'] * 2
                if score < worst_score:
                    worst_score = score
                    worst = {
                        'sn': sn,
                        'avg_rank': avg_rank,
                        'anomaly_count': info['anomaly_count']
                    }
        
        return worst


# 便捷函数
def analyze_cluster_daily(device_data: Dict[str, pd.DataFrame], 
                         date_str: str,
                         analyzer: Optional[ClusterBenchmarkAnalyzer] = None) -> Dict:
    """
    每日集群分析（便捷函数）
    """
    if analyzer is None:
        analyzer = ClusterBenchmarkAnalyzer()
    
    return analyzer.analyze_cluster_daily(device_data, date_str)
