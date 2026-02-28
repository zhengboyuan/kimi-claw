"""
组串健康追踪器 (String Health Tracker)

职责：
1. 独立追踪每路组串（PV1-PV8）的健康状态
2. 计算单组串效率、发电量、衰减趋势
3. 组串排名、老化预警
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime


class StringHealthTracker:
    """
    组串健康追踪器
    
    追踪8路组串：ai10(PV3), ai12(PV4), ai16(PV6), ai20(PV8)
    """
    
    # 组串配置
    STRING_CONFIG = {
        'ai10': {'name': 'PV3', 'desc': '第3路组串'},
        'ai12': {'name': 'PV4', 'desc': '第4路组串'},
        'ai16': {'name': 'PV6', 'desc': '第6路组串'},
        'ai20': {'name': 'PV8', 'desc': '第8路组串'}
    }
    
    def __init__(self):
        self.string_health = {code: {
            'daily_efficiency': [],  # 每日效率记录
            'daily_energy': [],      # 每日发电量记录
            'health_score': 1.0,     # 健康分数(0-1)
            'degradation_rate': 0.0, # 衰减率(%/月)
            'rank_history': [],      # 排名历史
            'anomaly_days': 0        # 异常天数
        } for code in self.STRING_CONFIG}
    
    def analyze_daily_strings(self, df_day: pd.DataFrame, date_str: str) -> Dict:
        """
        分析当日各组串健康状态
        
        Returns:
            {
                'string_ranking': [(code, efficiency, rank), ...],
                'health_alerts': [异常组串列表],
                'degradation_trend': {code: trend}
            }
        """
        results = {
            'date': date_str,
            'string_stats': {},
            'ranking': [],
            'alerts': []
        }
        
        # 1. 计算每路组串当日指标
        for code, config in self.STRING_CONFIG.items():
            if code not in df_day.columns:
                continue
            
            values = df_day[code].replace(0, np.nan).dropna()
            if len(values) == 0:
                continue
            
            # 计算组串效率（平均电流/最大电流）
            avg_current = values.mean()
            max_current = values.max()
            efficiency = avg_current / max_current if max_current > 0 else 0
            
            # 计算发电量（积分）
            energy = values.sum() * 0.25  # 假设15分钟间隔，0.25小时
            
            results['string_stats'][code] = {
                'name': config['name'],
                'avg_current': float(avg_current),
                'max_current': float(max_current),
                'efficiency': float(efficiency),
                'daily_energy': float(energy),
                'data_points': len(values)
            }
            
            # 更新历史记录
            self.string_health[code]['daily_efficiency'].append({
                'date': date_str,
                'value': float(efficiency)
            })
            self.string_health[code]['daily_energy'].append({
                'date': date_str,
                'value': float(energy)
            })
        
        # 2. 组串排名（按效率）
        if results['string_stats']:
            sorted_strings = sorted(
                results['string_stats'].items(),
                key=lambda x: x[1]['efficiency'],
                reverse=True
            )
            results['ranking'] = [
                {
                    'rank': i+1,
                    'code': code,
                    'name': stats['name'],
                    'efficiency': stats['efficiency'],
                    'energy': stats['daily_energy']
                }
                for i, (code, stats) in enumerate(sorted_strings)
            ]
            
            # 更新排名历史
            for item in results['ranking']:
                self.string_health[item['code']]['rank_history'].append({
                    'date': date_str,
                    'rank': item['rank']
                })
        
        # 3. 健康告警（效率低于0.5或排名持续垫底）
        for code, stats in results['string_stats'].items():
            if stats['efficiency'] < 0.5:
                results['alerts'].append({
                    'code': code,
                    'name': self.STRING_CONFIG[code]['name'],
                    'type': 'low_efficiency',
                    'value': stats['efficiency'],
                    'severity': 'warning' if stats['efficiency'] < 0.3 else 'info'
                })
                self.string_health[code]['anomaly_days'] += 1
        
        # 4. 计算衰减趋势（最近30天）
        results['degradation_trend'] = self._calculate_degradation_trend()
        
        return results
    
    def _calculate_degradation_trend(self) -> Dict:
        """计算各组串衰减趋势（%/月）"""
        trends = {}
        
        for code, health in self.string_health.items():
            efficiencies = health['daily_efficiency']
            if len(efficiencies) < 30:
                trends[code] = {'trend': 'insufficient_data', 'rate': 0.0}
                continue
            
            # 取最近30天
            recent = efficiencies[-30:]
            x = np.arange(len(recent))
            y = np.array([d['value'] for d in recent])
            
            # 线性回归计算斜率
            if len(y) > 1 and np.std(y) > 0:
                slope = np.polyfit(x, y, 1)[0]
                # 转换为每月衰减率
                monthly_rate = slope * 30 * 100  # %/月
                trends[code] = {
                    'trend': 'degrading' if monthly_rate < -5 else 'stable',
                    'rate': float(monthly_rate)
                }
            else:
                trends[code] = {'trend': 'stable', 'rate': 0.0}
        
        return trends
    
    def generate_string_health_report(self) -> str:
        """生成组串健康报告"""
        report = []
        report.append("=" * 60)
        report.append("组串健康追踪报告")
        report.append("=" * 60)
        
        # 1. 总体排名
        report.append("\n【组串效率排名】")
        for code, health in self.string_health.items():
            if health['daily_efficiency']:
                avg_eff = np.mean([d['value'] for d in health['daily_efficiency']])
                report.append(f"  {self.STRING_CONFIG[code]['name']}: {avg_eff:.2%}")
        
        # 2. 衰减趋势
        report.append("\n【衰减趋势】")
        trends = self._calculate_degradation_trend()
        for code, trend in trends.items():
            name = self.STRING_CONFIG[code]['name']
            if trend['trend'] == 'degrading':
                report.append(f"  ⚠️ {name}: 月衰减{trend['rate']:.1f}%")
            else:
                report.append(f"  ✅ {name}: 稳定")
        
        # 3. 异常统计
        report.append("\n【异常统计】")
        for code, health in self.string_health.items():
            if health['anomaly_days'] > 0:
                name = self.STRING_CONFIG[code]['name']
                report.append(f"  {name}: 异常{health['anomaly_days']}天")
        
        report.append("\n" + "=" * 60)
        return "\n".join(report)
    
    def get_worst_string(self) -> Optional[Dict]:
        """获取表现最差的组串"""
        avg_efficiencies = {}
        for code, health in self.string_health.items():
            if health['daily_efficiency']:
                avg_efficiencies[code] = np.mean([d['value'] for d in health['daily_efficiency']])
        
        if not avg_efficiencies:
            return None
        
        worst_code = min(avg_efficiencies, key=avg_efficiencies.get)
        return {
            'code': worst_code,
            'name': self.STRING_CONFIG[worst_code]['name'],
            'avg_efficiency': avg_efficiencies[worst_code],
            'anomaly_days': self.string_health[worst_code]['anomaly_days']
        }


# 便捷函数
def analyze_string_health_daily(df_day: pd.DataFrame, date_str: str, 
                                tracker: Optional[StringHealthTracker] = None) -> Dict:
    """
    每日组串健康分析（便捷函数）
    
    Args:
        df_day: 当日数据
        date_str: 日期
        tracker: 健康追踪器实例（可选）
    
    Returns:
        组串健康分析结果
    """
    if tracker is None:
        tracker = StringHealthTracker()
    
    return tracker.analyze_daily_strings(df_day, date_str)
