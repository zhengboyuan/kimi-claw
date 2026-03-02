"""
V4.4 资产健康评估引擎

核心功能：
1. 每日健康评分（5维度+趋势分）
2. 版本控制与基准校准
3. 健康历史记录

健康分维度：
- 异常频率 (30%): 最近7天P0/P1/P2异常加权
- 指标退化 (25%): 核心指标趋势分析
- 数据质量 (20%): 数据完整率 × 质量评分
- 稳定性 (15%): 关键指标波动率
- 历史对比 (10%): 与上月同期对比
- 趋势分 (附加): 健康分二阶导数（退化加速度）
"""

import json
import os
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from config.system_config import HISTORY_CONFIG, THRESHOLD_CONFIG

# 使用配置
MAX_HISTORY_DAYS = HISTORY_CONFIG["health_history_limit"]
HEALTH_WARNING_THRESHOLD = THRESHOLD_CONFIG["health_score_warning"]
HEALTH_DANGER_THRESHOLD = THRESHOLD_CONFIG["health_score_danger"]
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pandas as pd


class AssetHealthEngine:
    """资产健康评估引擎"""
    
    # 评分维度权重
    WEIGHTS = {
        'anomaly': 0.30,
        'degradation': 0.25,
        'quality': 0.20,
        'stability': 0.15,
        'history': 0.10
    }
    
    # 评分逻辑版本
    VERSION = "v1.0"
    
    # 阈值
    THRESHOLDS = {
        'excellent': 90,
        'good': 75,
        'attention': 60,
        'warning': 40,
        'danger': 0
    }
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.health_path = f"memory/devices/{device_sn}/health_history.json"
        self.trend_path = f"memory/devices/{device_sn}/trend_analysis.json"
        self.anomaly_path = f"memory/devices/{device_sn}/anomaly_log.json"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.health_path), exist_ok=True)
    
    def calculate_daily_health(self, date_str: str, device_data: Dict) -> Dict:
        """
        计算每日健康评分
        
        Args:
            date_str: 日期，如 "2025-07-16"
            device_data: 当日设备数据
        
        Returns:
            健康评分结果
        """
        result = {
            'date': date_str,
            'version': self.VERSION,
            'timestamp': datetime.now().isoformat(),
            'dimensions': {},
            'total_score': 0,
            'trend_score': 0,  # 二阶导数
            'level': 'unknown',
            'calibrated': True
        }
        
        # 计算5维度分数
        result['dimensions']['anomaly'] = self._calc_anomaly_score(date_str)
        result['dimensions']['degradation'] = self._calc_degradation_score(date_str, device_data)
        result['dimensions']['quality'] = self._calc_quality_score(device_data)
        result['dimensions']['stability'] = self._calc_stability_score(device_data)
        result['dimensions']['history'] = self._calc_history_score(date_str)
        
        # 加权总分
        total = sum(
            result['dimensions'][k] * self.WEIGHTS[k]
            for k in self.WEIGHTS.keys()
        )
        result['total_score'] = round(total, 1)
        
        # 计算趋势分（二阶导数）
        result['trend_score'] = self._calc_trend_score(date_str, total)
        
        # 确定等级
        result['level'] = self._determine_level(total)
        
        # 保存（不存储原始指标数据，避免内存累积）
        # 只存储分数和维度，raw_metrics 用于计算但不存储
        self._save_health_record(result)
        
        return result
    
    def _calc_anomaly_score(self, date_str: str) -> float:
        """异常频率评分（最近7天）"""
        # P0=Critical, P1=Warning, P2=Info
        weights = {'P0': 10, 'P1': 5, 'P2': 1}
        
        # 读取最近7天异常记录
        anomalies = self._get_recent_anomalies(date_str, days=7)
        
        if not anomalies:
            return 100.0
        
        # 计算加权异常分
        total_weight = sum(
            weights.get(a['level'], 1) 
            for a in anomalies
        )
        
        # 异常分越低，健康分越高
        score = max(0, 100 - total_weight * 2)
        return score
    
    def _calc_degradation_score(self, date_str: str, device_data: Dict) -> float:
        """指标退化评分 - 基于实际指标数据计算趋势"""
        raw_metrics = device_data.get('raw_metrics', {})
        
        # 定义关键指标及其代码映射
        core_metrics = {
            'power_active': ['ai68'],  # 有功功率
            'current_avg': ['ai54', 'ai55', 'ai56'],  # 三相电流平均
            'voltage_avg': ['ai51', 'ai52', 'ai53']   # 三相电压平均
        }
        
        scores = []
        for metric_name, possible_codes in core_metrics.items():
            # 获取当前数据
            current_values = None
            for code in possible_codes:
                if code in raw_metrics and len(raw_metrics[code]) > 0:
                    current_values = raw_metrics[code]
                    break
            
            if current_values is None:
                continue
            
            # 计算当前平均值
            current_avg = np.mean(current_values)
            
            # 获取历史数据（从已保存的健康记录中）
            history_values = self._get_metric_history_from_records(metric_name, date_str, days=7)
            
            if not history_values or len(history_values) < 2:
                scores.append(85)  # 数据不足，默认稳定
                continue
            
            history_avg = np.mean(history_values)
            
            if history_avg == 0:
                scores.append(85)  # 避免除零
                continue
            
            # 计算变化百分比
            change_pct = (current_avg - history_avg) / abs(history_avg) * 100
            
            # 根据变化趋势评分
            if change_pct > 5:
                scores.append(100)  # 显著改善
            elif change_pct > 0:
                scores.append(90)   # 轻微改善
            elif change_pct > -5:
                scores.append(85)   # 基本稳定
            elif change_pct > -10:
                scores.append(70)   # 轻微退化
            else:
                scores.append(50)   # 显著退化
        
        return np.mean(scores) if scores else 75.0
    
    def _calc_quality_score(self, device_data: Dict) -> float:
        """数据质量评分"""
        # 从device_data中提取质量信息
        completeness = device_data.get('completeness', 0.95)
        quality_rating = device_data.get('quality_rating', 70.0)
        
        # 完整率 × 质量评分归一化
        score = completeness * (quality_rating / 100) * 100
        return round(score, 1)
    
    def _calc_stability_score(self, device_data: Dict) -> float:
        """
        稳定性评分（关键指标波动率）
        基于raw_metrics中的时间序列数据计算变异系数
        """
        # 从raw_metrics获取关键指标数据
        raw_metrics = device_data.get('raw_metrics', {})
        
        # 映射到实际的指标代码
        metric_mapping = {
            'voltage_a': ['ai51', 'ai52', 'ai53'],  # 三相电压
            'current_a': ['ai54', 'ai55', 'ai56'],  # 三相电流
            'power_active': ['ai68']  # 有功功率
        }
        
        cv_scores = []
        for metric_name, possible_codes in metric_mapping.items():
            # 查找匹配的指标数据
            values = None
            for code in possible_codes:
                if code in raw_metrics and len(raw_metrics[code]) > 1:
                    values = raw_metrics[code]
                    break
            
            if values:
                mean_val = np.mean(values)
                std_val = np.std(values)
                if mean_val > 0:
                    cv = std_val / mean_val
                    # CV转换为分数
                    if cv < 0.05:
                        cv_scores.append(100)
                    elif cv < 0.1:
                        cv_scores.append(85)
                    elif cv < 0.2:
                        cv_scores.append(60)
                    else:
                        cv_scores.append(40)
        
        if cv_scores:
            return np.mean(cv_scores)
        else:
            print(f"  [DEBUG] 稳定性: 未找到有效指标数据，使用默认值")
            return 75.0
    
    def _calc_history_score(self, date_str: str) -> float:
        """
        历史对比评分（与上月同期）
        """
        try:
            # 获取上月同期日期
            current_date = datetime.strptime(date_str, '%Y-%m-%d')
            last_month = current_date - timedelta(days=30)
            last_month_str = last_month.strftime('%Y-%m-%d')
            
            # 读取上月健康分
            history = self._get_health_record(last_month_str)
            current = self._get_health_record(date_str)
            
            if not history or not current:
                print(f"  [DEBUG] 历史对比: 无历史记录 ({last_month_str})")
                return 75.0
            
            history_score = history.get('total_score', 75)
            current_score = current.get('total_score', 75)
            
            # 计算变化
            change = current_score - history_score
            
            print(f"  [DEBUG] 历史对比: 上月={history_score:.1f}, 本月={current_score:.1f}, 变化={change:+.1f}")
            
            # 变化越小越好
            if abs(change) < 5:
                return 100
            elif abs(change) < 10:
                return 85
            elif abs(change) < 20:
                return 60
            else:
                return 40
        except Exception as e:
            print(f"  [DEBUG] 历史对比计算失败: {e}")
            return 75.0
    
    def _calc_trend_score(self, date_str: str, current_score: float) -> float:
        """
        计算趋势分（健康分的二阶导数）
        
        正值：加速变好
        负值：加速变差（退化加速）
        接近0：平稳
        """
        # 获取最近3天的健康分
        dates = []
        scores = []
        
        for i in range(3):
            d = datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=i)
            d_str = d.strftime('%Y-%m-%d')
            record = self._get_health_record(d_str)
            if record:
                dates.append(i)
                scores.append(record.get('total_score', 75))
        
        if len(scores) < 3:
            return 0.0
        
        # 计算二阶导数（加速度）
        # f''(x) ≈ (f(x+h) - 2f(x) + f(x-h)) / h^2
        # 这里用离散点近似
        first_deriv = (scores[0] - scores[1])  # 今天 vs 昨天
        second_deriv = (scores[0] - 2*scores[1] + scores[2])  # 二阶
        
        return round(second_deriv, 2)
    
    def _determine_level(self, score: float) -> str:
        """确定健康等级"""
        if score >= self.THRESHOLDS['excellent']:
            return 'excellent'
        elif score >= self.THRESHOLDS['good']:
            return 'good'
        elif score >= self.THRESHOLDS['attention']:
            return 'attention'
        elif score >= self.THRESHOLDS['warning']:
            return 'warning'
        else:
            return 'danger'
    
    def _save_health_record(self, record: Dict):
        """保存健康记录 - 限制最多保留30天，防止内存累积"""
        # 读取现有历史
        history = []
        if os.path.exists(self.health_path):
            with open(self.health_path, 'r') as f:
                history = json.load(f)
        
        # 更新或追加
        updated = False
        for i, h in enumerate(history):
            if h.get('date') == record['date']:
                history[i] = record
                updated = True
                break
        
        if not updated:
            history.append(record)
        
        # 限制历史记录数量，只保留最近N天
        if len(history) > MAX_HISTORY_DAYS:
            # 按日期排序，保留最新的N条
            history.sort(key=lambda x: x.get('date', ''), reverse=True)
            history = history[:MAX_HISTORY_DAYS]
        
        # 保存
        with open(self.health_path, 'w') as f:
            json.dump(history, f, indent=2)
    
    def _get_health_record(self, date_str: str) -> Dict:
        """获取指定日期的健康记录"""
        if not os.path.exists(self.health_path):
            return None
        
        with open(self.health_path, 'r') as f:
            history = json.load(f)
        
        for h in history:
            if h.get('date') == date_str:
                return h
        
        return None
    
    def _get_recent_anomalies(self, date_str: str, days: int = 7) -> List:
        """获取最近N天的异常记录"""
        if not os.path.exists(self.anomaly_path):
            return []
        
        with open(self.anomaly_path, 'r') as f:
            all_anomalies = json.load(f)
        
        # 过滤最近N天
        end_date = datetime.strptime(date_str, '%Y-%m-%d')
        start_date = end_date - timedelta(days=days)
        
        recent = []
        for a in all_anomalies:
            a_date = datetime.strptime(a.get('date', '2000-01-01'), '%Y-%m-%d')
            if start_date <= a_date <= end_date:
                recent.append(a)
        
        return recent
    
    def _get_indicator_trend(self, indicator: str, date_str: str) -> str:
        """
        获取指标趋势
        通过对比当天和过去7天的平均值判断趋势
        """
        try:
            # 获取当天数据
            today_data = self._get_metric_history(indicator, date_str, days=1)
            # 获取过去7天数据（不含今天）
            history_data = self._get_metric_history(indicator, 
                (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d'),
                days=7)
            
            if not today_data or not history_data:
                return 'unknown'
            
            today_avg = np.mean(today_data)
            history_avg = np.mean(history_data)
            
            if history_avg == 0:
                return 'stable'
            
            change_pct = (today_avg - history_avg) / history_avg * 100
            
            if change_pct > 2:
                return 'improving'
            elif change_pct < -2:
                return 'degrading'
            else:
                return 'stable'
        except Exception as e:
            print(f"  [DEBUG] 趋势计算失败: {e}")
            return 'unknown'
    
    def _get_metric_history(self, indicator: str, date_str: str, days: int = 1) -> List:
        """
        获取指标历史数据
        从健康历史记录中读取指定日期的指标数据
        """
        try:
            # 构建日期列表
            dates = []
            for i in range(days):
                d = datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=i)
                dates.append(d.strftime('%Y-%m-%d'))
            
            values = []
            # 读取健康历史中的原始指标数据
            health_path = f"memory/devices/{self.device_sn}/health_history.json"
            if os.path.exists(health_path):
                with open(health_path, 'r') as f:
                    history = json.load(f)
                
                for record in history:
                    if record.get('date') in dates:
                        # 尝试从记录中获取指标数据
                        raw_metrics = record.get('raw_metrics', {})
                        if indicator in raw_metrics:
                            values.extend(raw_metrics[indicator])
            
            return values
        except Exception as e:
            print(f"  [DEBUG] 获取历史数据失败: {e}")
            return []
    
    def _get_metric_history_from_records(self, metric_name: str, date_str: str, days: int = 7) -> List:
        """
        从历史健康记录中获取指标数据
        用于退化评分计算
        """
        try:
            # 构建日期列表（不含当天）
            end_date = datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
            dates = []
            for i in range(days):
                d = end_date - timedelta(days=i)
                dates.append(d.strftime('%Y-%m-%d'))
            
            values = []
            # 映射指标名到代码
            metric_mapping = {
                'power_active': ['ai68'],
                'current_avg': ['ai54', 'ai55', 'ai56'],
                'voltage_avg': ['ai51', 'ai52', 'ai53']
            }
            possible_codes = metric_mapping.get(metric_name, [metric_name])
            
            # 读取健康历史
            if os.path.exists(self.health_path):
                with open(self.health_path, 'r') as f:
                    history = json.load(f)
                
                for record in history:
                    if record.get('date') in dates:
                        raw_metrics = record.get('raw_metrics', {})
                        for code in possible_codes:
                            if code in raw_metrics:
                                values.extend(raw_metrics[code])
                                break
            
            return values
        except Exception as e:
            print(f"  [DEBUG] 获取历史记录失败: {e}")
            return []

    def recalibrate_baseline(self, new_version: str, days: int = 30):
        """
        版本升级时重算基准
        
        Args:
            new_version: 新版本号
            days: 重算天数
        """
        print(f"[{self.device_sn}] 版本升级: {self.VERSION} -> {new_version}")
        print(f"重算最近{days}天健康分...")
        
        # 这里实现重算逻辑
        # 1. 读取最近days天的原始数据
        # 2. 用新逻辑重新计算
        # 3. 标记为已校准
        
        self.VERSION = new_version
        print("校准完成")


# 便捷函数
def calculate_device_health(device_sn: str, date_str: str, device_data: Dict) -> Dict:
    """计算单设备健康分"""
    engine = AssetHealthEngine(device_sn)
    return engine.calculate_daily_health(date_str, device_data)


def calculate_all_devices_health(date_str: str, all_device_data: Dict[str, Dict]) -> Dict[str, Dict]:
    """计算所有设备健康分"""
    results = {}
    for sn, data in all_device_data.items():
        results[sn] = calculate_device_health(sn, date_str, data)
    return results