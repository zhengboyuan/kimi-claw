"""
V5.1 竞赛核心指标计算器
实现等效利用小时数和发电时长的计算
"""
from typing import Dict, List, Optional
from datetime import datetime
import json


class CompetitionIndicatorCalculator:
    """竞赛指标计算器"""
    
    def __init__(self, station_config: Dict = None):
        """
        Args:
            station_config: 场站配置，包含装机容量等信息
        """
        self.station_config = station_config or {}
        self.installed_capacity = station_config.get('installed_capacity', 0) if station_config else 0
    
    def calculate_equivalent_utilization_hours(
        self, 
        generation_kwh: float, 
        installed_capacity_kw: Optional[float] = None
    ) -> Dict:
        """
        计算等效利用小时数
        
        公式: 等效利用小时数 = 发电量(kWh) / 装机容量(kW)
        
        Args:
            generation_kwh: 发电量(kWh)
            installed_capacity_kw: 装机容量(kW)，如不传使用配置中的值
        
        Returns:
            {
                'value': 计算结果,
                'unit': 'h',
                'formula': 'generation / installed_capacity',
                'inputs': {'generation': ..., 'installed_capacity': ...}
            }
        """
        capacity = installed_capacity_kw or self.installed_capacity
        
        if capacity <= 0:
            return {
                'value': None,
                'unit': 'h',
                'error': '装机容量未配置或为0',
                'formula': 'generation / installed_capacity',
                'inputs': {'generation': generation_kwh, 'installed_capacity': capacity}
            }
        
        hours = generation_kwh / capacity
        
        return {
            'value': round(hours, 2),
            'unit': 'h',
            'formula': 'generation / installed_capacity',
            'inputs': {
                'generation_kwh': generation_kwh,
                'installed_capacity_kw': capacity
            },
            'computable': True
        }
    
    def calculate_generation_duration(
        self, 
        status_series: List[str],
        sampling_interval_minutes: int = 5
    ) -> Dict:
        """
        计算发电时长
        
        统计状态为'generating'或功率>0的时间累计
        
        Args:
            status_series: 状态序列，如 ['generating', 'standby', 'generating', ...]
            sampling_interval_minutes: 采样间隔(分钟)，默认5分钟
        
        Returns:
            {
                'value': 小时数,
                'unit': 'h',
                'count': 发电状态点数,
                'total_points': 总点数
            }
        """
        # 统计发电状态点数
        generating_count = sum(1 for s in status_series if s in ['generating', '发电中', 'running'])
        total_points = len(status_series)
        
        # 计算小时数
        total_minutes = generating_count * sampling_interval_minutes
        hours = total_minutes / 60.0
        
        return {
            'value': round(hours, 2),
            'unit': 'h',
            'count': generating_count,
            'total_points': total_points,
            'sampling_interval_minutes': sampling_interval_minutes,
            'formula': 'count(generating) * interval / 60',
            'computable': True
        }
    
    def calculate_from_inverter_data(
        self,
        inverter_data: Dict,
        date_str: str
    ) -> Dict:
        """
        从逆变器数据计算竞赛指标
        
        Args:
            inverter_data: 逆变器当日数据
                {
                    'ai68': [...],  # 当日发电量累计值
                    'status': [...], # 状态序列
                    ...
                }
            date_str: 日期
        
        Returns:
            {
                'equivalent_utilization_hours': {...},
                'generation_duration': {...}
            }
        """
        results = {}
        
        # 1. 等效利用小时数
        # 从 ai68 (当日发电量) 获取
        if 'ai68' in inverter_data and len(inverter_data['ai68']) > 0:
            # ai68 是累计值，取最后一个点
            generation = inverter_data['ai68'][-1]
            results['equivalent_utilization_hours'] = self.calculate_equivalent_utilization_hours(generation)
        else:
            results['equivalent_utilization_hours'] = {
                'value': None,
                'error': '无发电量数据(ai68)',
                'computable': False
            }
        
        # 2. 发电时长
        if 'status' in inverter_data and len(inverter_data['status']) > 0:
            results['generation_duration'] = self.calculate_generation_duration(
                inverter_data['status']
            )
        else:
            # 如果没有状态数据，尝试从功率判断
            if 'ai56' in inverter_data and len(inverter_data['ai56']) > 0:
                # 功率>0认为是发电状态
                power_series = inverter_data['ai56']
                status_from_power = ['generating' if p > 0 else 'standby' for p in power_series]
                results['generation_duration'] = self.calculate_generation_duration(status_from_power)
            else:
                results['generation_duration'] = {
                    'value': None,
                    'error': '无状态或功率数据',
                    'computable': False
                }
        
        return results


# 便捷函数
def calculate_utilization_hours(generation_kwh: float, installed_capacity_kw: float) -> float:
    """便捷函数：计算等效利用小时数"""
    calc = CompetitionIndicatorCalculator({'installed_capacity': installed_capacity_kw})
    result = calc.calculate_equivalent_utilization_hours(generation_kwh)
    return result.get('value')


def calculate_generation_hours(status_series: List[str]) -> float:
    """便捷函数：计算发电时长"""
    calc = CompetitionIndicatorCalculator()
    result = calc.calculate_generation_duration(status_series)
    return result.get('value')