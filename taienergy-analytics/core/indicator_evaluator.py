"""
指标有效性评价器
第一阶段改造：引入昼夜门控与物理分类评分

改造要点：
1. 废弃全局计算 CV 和信息熵，按 ai56 > 50W 切分日间数据
2. 差异化打分逻辑：
   - 功率/电流类：与 ai56 的皮尔逊相关系数（权重 80%）
   - 电压/频率类：日间稳定性（1 - 归一化方差，越稳越高）
   - 温度类：与 ai61 相关性
   - DI/状态类：状态翻转率
3. 调整阈值：L1 >= 0.5, L2 >= 0.8
"""
import numpy as np
import pandas as pd
from typing import Dict, Optional
from scipy import stats


class IndicatorEvaluator:
    """
    指标有效性评价器（第一阶段改造版）
    
    评价维度（按指标类型差异化）：
    1. 功率/电流类：
       - 主要：与 ai56（有功功率）的皮尔逊相关系数（权重 80%）
       - 次要：活跃度（权重 20%）
    
    2. 电压/频率类：
       - 主要：日间稳定性（1 - 归一化方差，越稳越高）（权重 80%）
       - 次要：数据完整率（权重 20%）
    
    3. 温度类：
       - 主要：与 ai61（内部温度）的相关性（权重 70%）
       - 次要：变化幅度（权重 30%）
    
    4. DI/状态类：
       - 主要：状态翻转率（有变化给高分）（权重 80%）
       - 次要：数据完整率（权重 20%）
    
    昼夜门控：
    - 日间：ai56 > 50W 且不为空的时间段
    - 夜间：ai56 <= 50W 或为空的时间段（仅用于温度类指标）
    """
    
    # 核心基准指标配置
    CORE_BENCHMARKS = {
        "daytime": "ai56",    # 日间基准：有功功率
        "nighttime": "ai61",  # 夜间基准：内部温度
    }
    
    # 哨兵指标（禁止自动淘汰）
    SENTINEL_INDICATORS = [
        "ai63",   # 设备状态
        "ai64",   # 故障码
        "di39", "di40", "di41", "di42", "di43", "di44",  # 告警类
    ]
    
    # 升级阈值（第一阶段调整）
    UPGRADE_THRESHOLD_L1 = 0.5  # 原 0.6
    UPGRADE_THRESHOLD_L2 = 0.8  # 保持不变
    
    def __init__(self, core_indicator: str = "ai56"):
        self.core_indicator = core_indicator
    
    def evaluate(
        self, 
        df: pd.DataFrame, 
        core_df: Optional[pd.DataFrame] = None,
        indicator_code: str = "",
        indicator_type: str = "other"
    ) -> Dict:
        """
        评价指标有效性（差异化评分）
        
        Args:
            df: 待评价指标数据（全天数据）
            core_df: 基准指标数据（如有功功率 ai56）
            indicator_code: 指标代码（用于判断是否为哨兵指标）
            indicator_type: 指标类型（power/current/voltage/frequency/temperature/status/other）
        
        Returns:
            {
                "score": 综合评分 (0-1),
                "primary_score": 主维度评分,
                "secondary_score": 次维度评分,
                "status": "active/dormant/invalid/sentinel",
                "is_sentinel": 是否为哨兵指标,
                "day_data_ratio": 日间数据占比,
                "evaluation_logic": 评分逻辑说明
            }
        """
        # 基础检查
        if df.empty or len(df) < 5:
            return {
                "score": 0.0,
                "primary_score": 0.0,
                "secondary_score": 0.0,
                "status": "invalid",
                "is_sentinel": self._is_sentinel(indicator_code),
                "day_data_ratio": 0.0,
                "evaluation_logic": "数据量不足"
            }
        
        values = df['value'].dropna()
        if len(values) < 5:
            return {
                "score": 0.0,
                "primary_score": 0.0,
                "secondary_score": 0.0,
                "status": "invalid",
                "is_sentinel": self._is_sentinel(indicator_code),
                "day_data_ratio": 0.0,
                "evaluation_logic": "有效数据不足"
            }
        
        # 昼夜门控：切分日间/夜间数据
        day_mask = self._get_day_mask(df)
        df_day = df[day_mask] if day_mask.any() else df
        df_night = df[~day_mask] if (~day_mask).any() else pd.DataFrame()
        
        day_data_ratio = len(df_day) / len(df) if len(df) > 0 else 0
        
        # 如果日间数据不足，降级处理
        if len(df_day) < 5:
            return {
                "score": 0.1,
                "primary_score": 0.0,
                "secondary_score": 0.1,
                "status": "dormant",
                "is_sentinel": self._is_sentinel(indicator_code),
                "day_data_ratio": day_data_ratio,
                "evaluation_logic": "日间运行数据不足（可能全天停机）"
            }
        
        # 根据指标类型选择评分策略
        if indicator_type in ["power", "current"]:
            score, primary, secondary, logic = self._evaluate_power_current(
                df_day, core_df, indicator_code
            )
        elif indicator_type in ["voltage", "frequency"]:
            score, primary, secondary, logic = self._evaluate_voltage_frequency(
                df_day, indicator_code
            )
        elif indicator_type == "temperature":
            score, primary, secondary, logic = self._evaluate_temperature(
                df, df_day, df_night, core_df, indicator_code
            )
        elif indicator_type == "status":
            score, primary, secondary, logic = self._evaluate_status(
                df_day, indicator_code
            )
        else:
            # 其他类型使用默认评分
            score, primary, secondary, logic = self._evaluate_default(
                df_day, core_df, indicator_code
            )
        
        # 判断状态
        is_sentinel = self._is_sentinel(indicator_code)
        status = self._determine_status(score, is_sentinel, values)
        
        return {
            "score": float(score),
            "primary_score": float(primary),
            "secondary_score": float(secondary),
            "status": status,
            "is_sentinel": is_sentinel,
            "day_data_ratio": float(day_data_ratio),
            "evaluation_logic": logic,
            "data_points": len(values),
            "zero_ratio": float((values == 0).sum() / len(values)),
            "indicator_type": indicator_type
        }
    
    def _get_day_mask(self, df: pd.DataFrame) -> pd.Series:
        """
        获取日间数据掩码
        定义：有功功率 ai56 > 50W 且不为空的时间段为"运行态(日间)"
        """
        # 如果 df 中有 ai56 列，使用它来判断
        if 'ai56' in df.columns:
            return (df['ai56'] > 50) & (df['ai56'].notna())
        
        # 如果没有 ai56，检查 df.attrs 中是否有核心指标信息
        # 或者使用自身的 value 列作为回退（假设是功率类指标）
        return (df['value'] > 50) & (df['value'].notna())
    
    def _evaluate_power_current(
        self, 
        df_day: pd.DataFrame, 
        core_df: Optional[pd.DataFrame],
        indicator_code: str
    ) -> tuple:
        """
        功率/电流类指标评分
        主维度：与 ai56 的皮尔逊相关系数（权重 80%）
        次维度：活跃度（权重 20%）
        """
        values_day = df_day['value'].dropna()
        
        if len(values_day) < 5:
            return 0.1, 0.0, 0.1, "日间数据不足"
        
        # 主维度：与核心指标的相关性
        correlation = 0.0
        if core_df is not None and not core_df.empty:
            merged = pd.merge(
                df_day[['timestamp', 'value']], 
                core_df[['timestamp', 'value']], 
                on='timestamp', 
                suffixes=('', '_core')
            )
            if len(merged) >= 5:
                corr = merged['value'].corr(merged['value_core'])
                correlation = abs(corr) if not pd.isna(corr) else 0.0
        
        # 次维度：活跃度（CV 变异系数，但归一化到 0-1）
        cv = values_day.std() / abs(values_day.mean()) if values_day.mean() != 0 else 0
        activity = min(cv, 1.0)  # CV > 1 认为高度活跃
        
        # 综合评分
        score = correlation * 0.8 + activity * 0.2
        
        logic = f"功率/电流类评分: 与ai56相关性={correlation:.3f}(权重80%) + 活跃度={activity:.3f}(权重20%)"
        
        return score, correlation, activity, logic
    
    def _evaluate_voltage_frequency(
        self, 
        df_day: pd.DataFrame,
        indicator_code: str
    ) -> tuple:
        """
        电压/频率类指标评分
        主维度：日间稳定性（1 - 归一化方差，越稳越高）（权重 80%）
        次维度：数据完整率（权重 20%）
        """
        values_day = df_day['value'].dropna()
        
        if len(values_day) < 5:
            return 0.1, 0.0, 0.1, "日间数据不足"
        
        # 主维度：稳定性（1 - 变异系数，越稳越接近 1）
        mean_val = values_day.mean()
        std_val = values_day.std()
        
        if mean_val == 0:
            stability = 0.5  # 均值为0，给中等分
        else:
            cv = std_val / abs(mean_val)
            stability = max(0, 1 - cv)  # CV 越小，稳定性越高
        
        # 次维度：数据完整率
        completeness = len(values_day) / len(df_day) if len(df_day) > 0 else 0
        
        # 综合评分
        score = stability * 0.8 + completeness * 0.2
        
        logic = f"电压/频率类评分: 稳定性={stability:.3f}(1-CV,权重80%) + 完整率={completeness:.3f}(权重20%)"
        
        return score, stability, completeness, logic
    
    def _evaluate_temperature(
        self, 
        df: pd.DataFrame,
        df_day: pd.DataFrame,
        df_night: pd.DataFrame,
        core_df: Optional[pd.DataFrame],
        indicator_code: str
    ) -> tuple:
        """
        温度类指标评分
        主维度：与 ai61（内部温度）的相关性（权重 70%）
        次维度：变化幅度（权重 30%）
        """
        values = df['value'].dropna()
        
        if len(values) < 5:
            return 0.1, 0.0, 0.1, "数据不足"
        
        # 主维度：与 ai61 的相关性
        correlation = 0.0
        if core_df is not None and not core_df.empty:
            merged = pd.merge(
                df[['timestamp', 'value']], 
                core_df[['timestamp', 'value']], 
                on='timestamp', 
                suffixes=('', '_core')
            )
            if len(merged) >= 5:
                corr = merged['value'].corr(merged['value_core'])
                correlation = abs(corr) if not pd.isna(corr) else 0.0
        
        # 次维度：变化幅度（使用日间数据的极差归一化）
        if not df_day.empty:
            day_values = df_day['value'].dropna()
            if len(day_values) > 0:
                value_range = day_values.max() - day_values.min()
                # 温度变化在 0-50 度范围内归一化
                variation = min(value_range / 50, 1.0)
            else:
                variation = 0.0
        else:
            variation = 0.0
        
        # 综合评分
        score = correlation * 0.7 + variation * 0.3
        
        logic = f"温度类评分: 与ai61相关性={correlation:.3f}(权重70%) + 变化幅度={variation:.3f}(权重30%)"
        
        return score, correlation, variation, logic
    
    def _evaluate_status(
        self, 
        df_day: pd.DataFrame,
        indicator_code: str
    ) -> tuple:
        """
        DI/状态类指标评分
        主维度：状态翻转率（有变化给高分）（权重 80%）
        次维度：数据完整率（权重 20%）
        """
        values_day = df_day['value'].dropna()
        
        if len(values_day) < 2:
            return 0.1, 0.0, 0.1, "日间数据不足"
        
        # 主维度：状态翻转率
        # 计算相邻值的变化次数
        changes = (values_day.diff() != 0).sum()
        flip_rate = changes / (len(values_day) - 1) if len(values_day) > 1 else 0
        
        # 有翻转给高分，长期为0或1给低分
        activity_score = min(flip_rate * 10, 1.0)  # 翻转率 10% 给满分
        
        # 次维度：数据完整率
        completeness = len(values_day) / len(df_day) if len(df_day) > 0 else 0
        
        # 综合评分
        score = activity_score * 0.8 + completeness * 0.2
        
        logic = f"状态类评分: 翻转率={flip_rate:.3f}->活跃度={activity_score:.3f}(权重80%) + 完整率={completeness:.3f}(权重20%)"
        
        return score, activity_score, completeness, logic
    
    def _evaluate_default(
        self, 
        df_day: pd.DataFrame,
        core_df: Optional[pd.DataFrame],
        indicator_code: str
    ) -> tuple:
        """
        默认评分策略（其他类型指标）
        使用改进前的综合评分逻辑
        """
        values_day = df_day['value'].dropna()
        
        if len(values_day) < 5:
            return 0.1, 0.0, 0.1, "数据不足"
        
        # 活跃度（CV）
        cv = values_day.std() / abs(values_day.mean()) if values_day.mean() != 0 else 0
        volatility = min(cv, 1.0)
        
        # 相关性
        correlation = 0.0
        if core_df is not None and not core_df.empty:
            merged = pd.merge(
                df_day[['timestamp', 'value']], 
                core_df[['timestamp', 'value']], 
                on='timestamp', 
                suffixes=('', '_core')
            )
            if len(merged) >= 5:
                corr = merged['value'].corr(merged['value_core'])
                correlation = abs(corr) if not pd.isna(corr) else 0.0
        
        # 信息熵
        entropy = self._calc_entropy(values_day)
        
        # 完整率
        completeness = len(values_day) / len(df_day) if len(df_day) > 0 else 0
        
        # 综合评分
        score = volatility * 0.25 + correlation * 0.30 + entropy * 0.25 + completeness * 0.20
        
        logic = f"默认评分: 活跃度={volatility:.3f} + 相关性={correlation:.3f} + 熵={entropy:.3f} + 完整率={completeness:.3f}"
        
        return score, correlation, volatility, logic
    
    def _calc_entropy(self, values: pd.Series, bins: int = 10) -> float:
        """计算信息熵（归一化到 0-1）"""
        if len(values) < bins:
            return 0.5
        
        hist, _ = np.histogram(values, bins=bins)
        prob = hist / len(values)
        entropy = -np.sum(prob * np.log2(prob + 1e-10))
        max_entropy = np.log2(bins)
        return entropy / max_entropy if max_entropy > 0 else 0
    
    def _is_sentinel(self, indicator_code: str) -> bool:
        """判断是否为哨兵指标（禁止淘汰）"""
        if not indicator_code:
            return False
        
        if indicator_code in self.SENTINEL_INDICATORS:
            return True
        
        if indicator_code.startswith('di'):
            return True
        
        sentinel_keywords = ['status', 'fault', 'alarm', 'error', 'state', '异常', '故障', '告警']
        indicator_lower = indicator_code.lower()
        return any(kw in indicator_lower for kw in sentinel_keywords)
    
    def _determine_status(
        self, 
        score: float, 
        is_sentinel: bool, 
        values: pd.Series
    ) -> str:
        """确定指标状态（使用新阈值）"""
        if is_sentinel:
            return "sentinel"
        
        if len(values) < 5:
            return "invalid"
        
        if (values == 0).all() or values.isna().all():
            return "dormant"
        
        # 使用新阈值
        if score >= self.UPGRADE_THRESHOLD_L2:
            return "active"
        elif score >= self.UPGRADE_THRESHOLD_L1:
            return "marginal"
        else:
            return "dormant"
    
    def evaluate_batch(
        self,
        data_dict: Dict[str, pd.DataFrame],
        core_indicator: str = "ai56",
        indicator_types: Optional[Dict[str, str]] = None
    ) -> Dict[str, Dict]:
        """
        批量评价多个指标（差异化评分）
        
        Args:
            data_dict: {indicator_code: df}
            core_indicator: 核心基准指标代码
            indicator_types: {indicator_code: type} 指标类型映射
        
        Returns:
            {indicator_code: evaluation_result}
        """
        results = {}
        
        # 获取核心指标数据
        core_df = data_dict.get(core_indicator)
        
        for indicator_code, df in data_dict.items():
            # 获取指标类型
            ind_type = indicator_types.get(indicator_code, "other") if indicator_types else "other"
            
            results[indicator_code] = self.evaluate(
                df=df,
                core_df=core_df,
                indicator_code=indicator_code,
                indicator_type=ind_type
            )
        
        return results
    
    def get_core_benchmark(self, data_dict: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        自动选择合适的核心基准
        
        策略：
        - 如果有功功率(ai56)有数据且不为0，使用有功功率
        - 否则使用内部温度(ai61)
        """
        if "ai56" in data_dict:
            df = data_dict["ai56"]
            values = df['value'].dropna()
            if len(values) > 0 and (values > 0).any():
                return df
        
        if "ai61" in data_dict:
            return data_dict["ai61"]
        
        for key in ["ai49", "ai50", "ai51"]:
            if key in data_dict:
                return data_dict[key]
        
        return None
