"""
复合指标变异引擎 V3.1 (工业级时序分析平台)

核心升级：
1. 阈值动态化：基于负载率的门控（半载以上才判效率）
2. 滑动窗口：状态驻留检测（抗噪/抗干扰）
3. 异常分级：P0/P1/P2 三级告警（避免告警风暴）

职责：
1. 基于物理常识生成 A/B、A-B 复合指标组合
2. 计算残差、离散率、变异系数
3. 达尔文筛选：统计学 + 工业绝对阈值 双重探测

核心思想：
- Python 干苦力：穷举变异、计算统计量、筛选异常
- LLM 做决策：基于异常事实进行物理解释
"""
import pandas as pd
import numpy as np
import itertools
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class SeverityLevel(Enum):
    """异常严重级别"""
    INFO = "info"           # P2: 记录即可
    WARNING = "warning"     # P1: 需要关注
    CRITICAL = "critical"   # P0: 立即处理


@dataclass
class AnomalyConfig:
    """异常检测配置"""
    # 组串离散率阈值
    diff_pct_info: float = 0.05      # 5% - P2
    diff_pct_warning: float = 0.10   # 10% - P1
    diff_pct_critical: float = 0.20  # 20% - P0
    
    # 效率损失阈值（半载以上才判）
    efficiency_info: float = 0.03    # 3% - P2
    efficiency_warning: float = 0.06 # 6% - P1
    efficiency_critical: float = 0.10 # 10% - P0
    
    # 三相不平衡阈值
    unbalance_info: float = 0.03     # 3% - P2
    unbalance_warning: float = 0.05  # 5% - P1 (国标)
    unbalance_critical: float = 0.08 # 8% - P0
    
    # 负载率门控（半载以上才判效率）
    min_load_ratio: float = 0.50     # 50%额定功率
    
    # 滑动窗口配置
    rolling_window: int = 10         # 10个时间点（约50分钟）
    min_continuous_points: int = 5   # 至少连续5个点异常才确认
    
    # 设备额定值
    rated_power: float = 50000.0     # 50kW额定功率


class CompositeIndicatorEngineV31:
    """
    复合指标变异引擎 V3.1 (工业级)
    
    升级特性：
    1. 动态阈值：基于负载率门控
    2. 滑动窗口：状态驻留检测
    3. 异常分级：P0/P1/P2三级告警
    """
    
    # 物理分组配置
    PV_INPUT_CURRENTS = ['ai10', 'ai12', 'ai16', 'ai20']
    GRID_VOLTAGES = ['ai49', 'ai50', 'ai51']
    POWER_METRICS = ['ai45', 'ai56']
    
    def __init__(self, df_day: pd.DataFrame, config: Optional[AnomalyConfig] = None):
        """
        初始化引擎
        
        Args:
            df_day: 当日数据 DataFrame
            config: 异常检测配置（可选，使用默认配置）
        """
        self.df_day = df_day
        self.config = config or AnomalyConfig()
        self.mutated_df = pd.DataFrame(index=df_day.index)
        self.metadata = {}
        
    def generate_and_select(self) -> Dict:
        """
        一键完成：变异产生 -> 滑动窗口检测 -> 分级筛选
        
        Returns:
            survivors: {复合指标名: {formula, severity, anomaly_peak, ...}}
        """
        print("  🧬 启动复合指标变异引擎 V3.1...")
        
        # 1. 生成所有变异
        self._generate_mutations()
        
        if self.mutated_df.empty:
            print("  ✅ 无复合指标可分析")
            return {}
        
        # 2. 滑动窗口检测 + 分级筛选
        survivors = self._sliding_window_selection()
        
        if not survivors:
            print("  ✅ 今日设备运行平稳，未发现持续异常")
            return {}
        
        # 按严重程度排序
        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        sorted_survivors = dict(
            sorted(
                survivors.items(),
                key=lambda item: (severity_order.get(item[1]['severity'], 3), -item[1]['anomaly_peak'])
            )
        )
        
        # 统计
        critical_count = sum(1 for s in survivors.values() if s['severity'] == 'critical')
        warning_count = sum(1 for s in survivors.values() if s['severity'] == 'warning')
        info_count = sum(1 for s in survivors.values() if s['severity'] == 'info')
        
        print(f"  🚨 引擎抓取到 {len(survivors)} 个异常!")
        if critical_count:
            print(f"     🔴 P0 Critical: {critical_count}个")
        if warning_count:
            print(f"     🟡 P1 Warning: {warning_count}个")
        if info_count:
            print(f"     🔵 P2 Info: {info_count}个")
        
        return sorted_survivors
    
    def _generate_mutations(self):
        """生成所有复合指标变异"""
        self._generate_pv_string_differences()
        self._generate_efficiency_loss()
        self._generate_voltage_unbalance()
        self._generate_first_derivative()
        self._generate_rolling_variance()
    
    def _generate_pv_string_differences(self):
        """组串差异变异：|A-B|/A"""
        pv_cols = [c for c in self.PV_INPUT_CURRENTS if c in self.df_day.columns]
        if len(pv_cols) < 2:
            return
        
        for c1, c2 in itertools.combinations(pv_cols, 2):
            name = f"diff_pct_{c1}_{c2}"
            self.mutated_df[name] = np.where(
                self.df_day[c1] > 1.0,
                abs(self.df_day[c1] - self.df_day[c2]) / self.df_day[c1],
                0
            )
            self.metadata[name] = {
                "formula": f"Abs({c1}-{c2})/{c1}",
                "type": "组串一致性",
                "category": "diff_pct"
            }
    
    def _generate_efficiency_loss(self):
        """转换效率变异：1 - 输出/输入（带负载率门控）"""
        if 'ai45' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        name = "efficiency_loss"
        
        # 计算负载率
        load_ratio = self.df_day['ai45'] / self.config.rated_power
        
        # 半载以上才计算效率损失
        self.mutated_df[name] = np.where(
            load_ratio > self.config.min_load_ratio,
            1.0 - (self.df_day['ai56'] / self.df_day['ai45']),
            np.nan
        )
        
        self.metadata[name] = {
            "formula": "1.0 - (ai56/ai45) [load>50%]",
            "type": "动态损耗率",
            "category": "efficiency"
        }
    
    def _generate_voltage_unbalance(self):
        """三相不平衡变异：(Max-Min)/Mean"""
        vol_cols = [c for c in self.GRID_VOLTAGES if c in self.df_day.columns]
        if len(vol_cols) < 3:
            return
        
        name = "vol_unbalance"
        mean_v = self.df_day[vol_cols].mean(axis=1)
        max_v = self.df_day[vol_cols].max(axis=1)
        min_v = self.df_day[vol_cols].min(axis=1)
        
        self.mutated_df[name] = (max_v - min_v) / mean_v
        
        self.metadata[name] = {
            "formula": "(Max-Min)/Mean",
            "type": "三相电压不平衡度",
            "category": "unbalance"
        }
    
    def _generate_first_derivative(self):
        """一阶导数变异"""
        key_metrics = ['ai45', 'ai56', 'ai61']
        for col in key_metrics:
            if col not in self.df_day.columns:
                continue
            name = f"derivative_{col}"
            self.mutated_df[name] = self.df_day[col].diff().abs()
            self.metadata[name] = {
                "formula": f"d({col})/dt",
                "type": "变化率",
                "category": "derivative"
            }
    
    def _generate_rolling_variance(self):
        """滚动方差变异"""
        key_metrics = ['ai45', 'ai56']
        for col in key_metrics:
            if col not in self.df_day.columns:
                continue
            name = f"rolling_std_{col}"
            self.mutated_df[name] = self.df_day[col].rolling(
                window=self.config.rolling_window
            ).std()
            self.metadata[name] = {
                "formula": f"Std({col}, w={self.config.rolling_window})",
                "type": "抖动度",
                "category": "variance"
            }
    
    def _sliding_window_selection(self) -> Dict:
        """
        滑动窗口检测 + 分级筛选
        
        核心逻辑：
        1. 使用滑动窗口检测持续异常（抗噪）
        2. 基于工业绝对阈值分级（P0/P1/P2）
        3. 只保留持续N个点以上的异常
        """
        survivors = {}
        
        for col in self.mutated_df.columns:
            series = self.mutated_df[col].dropna()
            if len(series) < self.config.min_continuous_points:
                continue
            
            category = self.metadata[col].get("category", "")
            
            # 滑动窗口检测
            anomaly_windows = self._detect_anomaly_windows(series, category)
            
            if not anomaly_windows:
                continue
            
            # 取最严重的窗口
            worst_window = max(anomaly_windows, key=lambda x: x['severity_score'])
            
            survivors[col] = {
                "formula": self.metadata[col]["formula"],
                "category": self.metadata[col]["type"],
                "severity": worst_window['severity'],
                "anomaly_peak": round(worst_window['peak'], 4),
                "anomaly_mean": round(worst_window['mean'], 4),
                "continuous_points": worst_window['points'],
                "window_start": worst_window['start'],
                "window_end": worst_window['end']
            }
        
        return survivors
    
    def _detect_anomaly_windows(self, series: pd.Series, category: str) -> List[Dict]:
        """
        检测异常窗口
        
        Returns:
            异常窗口列表，每个窗口包含：
            - severity: 严重程度
            - peak: 峰值
            - mean: 均值
            - points: 连续点数
            - start/end: 窗口位置
        """
        windows = []
        window_size = self.config.rolling_window
        
        for i in range(len(series) - window_size + 1):
            window = series.iloc[i:i + window_size]
            
            # 计算窗口统计
            peak = window.max()
            mean = window.mean()
            
            # 判断严重程度
            severity = self._classify_severity(peak, category)
            
            if severity:
                # 检查连续异常点数（使用>=避免边界漏检）
                threshold = self._get_threshold_for_category(category, severity)
                continuous = (window >= threshold).sum()
                
                if continuous >= self.config.min_continuous_points:
                    windows.append({
                        'severity': severity.value,
                        'severity_score': {'critical': 3, 'warning': 2, 'info': 1}.get(severity.value, 0),
                        'peak': peak,
                        'mean': mean,
                        'points': continuous,
                        'start': i,
                        'end': i + window_size - 1,
                        'threshold': threshold
                    })
        
        return windows
    
    def _classify_severity(self, value: float, category: str) -> Optional[SeverityLevel]:
        """基于工业阈值分类严重程度"""
        if category == "diff_pct":
            if value > self.config.diff_pct_critical:
                return SeverityLevel.CRITICAL
            elif value > self.config.diff_pct_warning:
                return SeverityLevel.WARNING
            elif value > self.config.diff_pct_info:
                return SeverityLevel.INFO
                
        elif category == "efficiency":
            if value > self.config.efficiency_critical:
                return SeverityLevel.CRITICAL
            elif value > self.config.efficiency_warning:
                return SeverityLevel.WARNING
            elif value > self.config.efficiency_info:
                return SeverityLevel.INFO
                
        elif category == "unbalance":
            if value > self.config.unbalance_critical:
                return SeverityLevel.CRITICAL
            elif value > self.config.unbalance_warning:
                return SeverityLevel.WARNING
            elif value > self.config.unbalance_info:
                return SeverityLevel.INFO
        
        return None
    
    def _get_threshold_for_category(self, category: str, severity: SeverityLevel) -> float:
        """获取对应阈值"""
        thresholds = {
            "diff_pct": {
                SeverityLevel.CRITICAL: self.config.diff_pct_critical,
                SeverityLevel.WARNING: self.config.diff_pct_warning,
                SeverityLevel.INFO: self.config.diff_pct_info
            },
            "efficiency": {
                SeverityLevel.CRITICAL: self.config.efficiency_critical,
                SeverityLevel.WARNING: self.config.efficiency_warning,
                SeverityLevel.INFO: self.config.efficiency_info
            },
            "unbalance": {
                SeverityLevel.CRITICAL: self.config.unbalance_critical,
                SeverityLevel.WARNING: self.config.unbalance_warning,
                SeverityLevel.INFO: self.config.unbalance_info
            }
        }
        return thresholds.get(category, {}).get(severity, 0.0)


# 便捷函数（V3.1版本）
def analyze_daily_composites_v31(df_day: pd.DataFrame, config: Optional[AnomalyConfig] = None) -> Dict:
    """
    分析当日复合指标异常（V3.1便捷函数）
    
    Args:
        df_day: 当日数据
        config: 异常检测配置（可选）
    
    Returns:
        异常复合指标字典（含severity分级）
    """
    engine = CompositeIndicatorEngineV31(df_day, config)
    return engine.generate_and_select()
