"""
复合指标变异引擎 (V3.0核心组件)

职责：
1. 基于物理常识生成 A/B、A-B 复合指标组合
2. 计算残差、离散率、变异系数
3. 达尔文筛选：只保留"平时如死水，今天有惊雷"的异常突变

核心思想：
- Python 干苦力：穷举变异、计算统计量、筛选异常
- LLM 做决策：基于异常事实进行物理解释
"""
import pandas as pd
import numpy as np
import itertools
from typing import Dict, List, Tuple, Optional


class CompositeIndicatorEngine:
    """
    复合指标变异引擎
    
    变异策略：
    1. 组串差异变异：|A-B|/A （找木桶短板）
    2. 转换效率变异：1 - A/B （找高温降载）
    3. 三相不平衡变异：(Max-Min)/Mean （找电网波动）
    4. 一阶导数变异：斜率突变 （找趋势变化）
    5. 滚动方差变异：抖动异常 （找不稳定）
    
    筛选策略（达尔文自然选择）：
    - 条件1：max > mean + 3σ （严重偏离均值）
    - 条件2：max > 阈值 （绝对值足够大）
    - 排序：按偏离度降序，取 Top N
    """
    
    # 达尔文筛选阈值
    SIGMA_THRESHOLD = 3      # 几倍标准差算异常
    ABS_THRESHOLD = 0.05     # 绝对值阈值
    TOP_N_SURVIVORS = 3      # 只保留最严重的N个
    
    # 物理分组配置
    PV_INPUT_CURRENTS = ['ai10', 'ai12', 'ai16', 'ai20']  # PV输入电流
    GRID_VOLTAGES = ['ai49', 'ai50', 'ai51']              # 电网三相电压
    POWER_METRICS = ['ai45', 'ai56']                       # 输入/输出功率
    
    def __init__(self, df_day: pd.DataFrame):
        """
        初始化引擎
        
        Args:
            df_day: 当日数据 DataFrame，列为指标代码
        """
        self.df_day = df_day
        self.mutated_df = pd.DataFrame(index=df_day.index)
        self.metadata = {}
        
    def generate_and_select(self) -> Dict:
        """
        一键完成：变异产生 -> 筛选幸存者
        
        Returns:
            survivors: {复合指标名: {formula, category, baseline_mean, anomaly_peak}}
        """
        print("  🧬 启动复合指标变异引擎...")
        
        # 1. 生成所有变异
        self._generate_mutations()
        
        # 2. 达尔文筛选
        survivors = self._darwin_selection()
        
        if not survivors:
            print("  ✅ 今日设备运行极其平稳，未发现任何长尾/离散异常突变。")
            return {}
        
        print(f"  🚨 引擎抓取到 {len(survivors)} 个复合指标异常突变！")
        return survivors
    
    def _generate_mutations(self):
        """生成所有复合指标变异"""
        # 1. 组串差异变异 (找木桶短板)
        self._generate_pv_string_differences()
        
        # 2. 转换效率变异 (找高温降载)
        self._generate_efficiency_loss()
        
        # 3. 三相不平衡变异 (找电网波动)
        self._generate_voltage_unbalance()
        
        # 4. 一阶导数变异 (找趋势突变)
        self._generate_first_derivative()
        
        # 5. 滚动方差变异 (找抖动异常)
        self._generate_rolling_variance()
    
    def _generate_pv_string_differences(self):
        """
        组串差异变异
        公式：|A-B|/A
        物理意义：寻找组串间的木桶短板（遮挡、老化差异）
        """
        # 获取存在的PV输入电流列
        pv_cols = [c for c in self.PV_INPUT_CURRENTS if c in self.df_day.columns]
        
        if len(pv_cols) < 2:
            return
        
        for c1, c2 in itertools.combinations(pv_cols, 2):
            name = f"diff_pct_{c1}_{c2}"
            
            # 计算偏差率，防止除零
            self.mutated_df[name] = np.where(
                self.df_day[c1] > 1.0,
                abs(self.df_day[c1] - self.df_day[c2]) / self.df_day[c1],
                0
            )
            
            self.metadata[name] = {
                "formula": f"Abs({c1}-{c2})/{c1}",
                "type": "组串一致性",
                "description": f"{c1}与{c2}的相对偏差率，反映组串间的不平衡",
                "inputs": [c1, c2]
            }
    
    def _generate_efficiency_loss(self):
        """
        转换效率变异
        公式：1 - 输出功率/输入功率
        物理意义：寻找高温降载、设备老化导致的效率损失
        """
        if 'ai45' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        name = "efficiency_loss"
        
        # 计算动态损耗率 (1 - 输出/输入)
        self.mutated_df[name] = np.where(
            self.df_day['ai45'] > 500,  # 输入功率大于500W才计算
            1.0 - (self.df_day['ai56'] / self.df_day['ai45']),
            np.nan
        )
        
        self.metadata[name] = {
            "formula": "1.0 - (ai56/ai45)",
            "type": "动态损耗率",
            "description": "逆变器转换效率损失率，高温时通常会升高",
            "inputs": ['ai45', 'ai56']
        }
    
    def _generate_voltage_unbalance(self):
        """
        三相不平衡变异
        公式：(Max - Min) / Mean
        物理意义：寻找电网三相电压不平衡（电网波动、故障）
        """
        vol_cols = [c for c in self.GRID_VOLTAGES if c in self.df_day.columns]
        
        if len(vol_cols) < 3:
            return
        
        name = "vol_unbalance"
        
        mean_v = self.df_day[vol_cols].mean(axis=1)
        max_v = self.df_day[vol_cols].max(axis=1)
        min_v = self.df_day[vol_cols].min(axis=1)
        
        self.mutated_df[name] = (max_v - min_v) / mean_v
        
        self.metadata[name] = {
            "formula": "(Max(Ua,Ub,Uc)-Min(Ua,Ub,Uc))/Mean(U)",
            "type": "三相电压不平衡度",
            "description": "电网三相电压不平衡程度，超过5%需关注",
            "inputs": vol_cols
        }
    
    def _generate_first_derivative(self):
        """
        一阶导数变异
        公式：ΔValue/ΔTime
        物理意义：寻找功率/电压的突变趋势（突变事件）
        """
        # 对关键指标计算一阶导数
        key_metrics = ['ai45', 'ai56', 'ai61']
        
        for col in key_metrics:
            if col not in self.df_day.columns:
                continue
            
            name = f"derivative_{col}"
            
            # 计算一阶差分（近似导数）
            self.mutated_df[name] = self.df_day[col].diff().abs()
            
            self.metadata[name] = {
                "formula": f"d({col})/dt",
                "type": "变化率",
                "description": f"{col}的变化速率，突变事件会呈现尖峰",
                "inputs": [col]
            }
    
    def _generate_rolling_variance(self):
        """
        滚动方差变异
        公式：Rolling Std
        物理意义：寻找指标的抖动异常（不稳定运行）
        """
        key_metrics = ['ai45', 'ai56']
        window = 5  # 5个时间点的滚动窗口
        
        for col in key_metrics:
            if col not in self.df_day.columns:
                continue
            
            name = f"rolling_std_{col}"
            
            # 计算滚动标准差
            self.mutated_df[name] = self.df_day[col].rolling(window=window).std()
            
            self.metadata[name] = {
                "formula": f"Std({col}, window={window})",
                "type": "抖动度",
                "description": f"{col}的滚动标准差，反映运行稳定性",
                "inputs": [col]
            }
    
    def _darwin_selection(self) -> Dict:
        """
        达尔文自然选择
        
        筛选条件：
        1. 最大值严重偏离均值（当天有异常突变）
        2. 绝对值超过阈值（不是噪声）
        3. 按偏离度排序，只保留 Top N
        
        Returns:
            survivors: 筛选后的异常复合指标
        """
        survivors = {}
        
        for col in self.mutated_df.columns:
            series = self.mutated_df[col].dropna()
            
            if len(series) < 10:  # 数据点太少，跳过
                continue
            
            mean_val = series.mean()
            max_val = series.max()
            std_val = series.std()
            
            # 跳过标准差为0的（没有变化）
            if std_val == 0:
                continue
            
            # 达尔文筛选条件
            condition_1 = max_val > (mean_val + self.SIGMA_THRESHOLD * std_val)
            condition_2 = max_val > self.ABS_THRESHOLD
            
            if condition_1 and condition_2:
                survivors[col] = {
                    "formula": self.metadata[col]["formula"],
                    "category": self.metadata[col]["type"],
                    "description": self.metadata[col]["description"],
                    "inputs": self.metadata[col]["inputs"],
                    "baseline_mean_today": round(mean_val, 4),
                    "anomaly_peak_today": round(max_val, 4),
                    "deviation_sigma": round((max_val - mean_val) / std_val, 2)
                }
        
        # 按偏离度降序排序，只保留 Top N
        sorted_survivors = dict(
            sorted(
                survivors.items(),
                key=lambda item: item[1]['anomaly_peak_today'],
                reverse=True
            )[:self.TOP_N_SURVIVORS]
        )
        
        return sorted_survivors
    
    def get_mutation_statistics(self) -> Dict:
        """
        获取变异统计信息（用于调试和监控）
        
        Returns:
            统计信息字典
        """
        return {
            "total_mutations_generated": len(self.mutated_df.columns),
            "mutation_types": list(set(m["type"] for m in self.metadata.values())),
            "mutation_details": self.metadata
        }


# 便捷函数
def analyze_daily_composites(df_day: pd.DataFrame, top_n: int = 3) -> Dict:
    """
    分析当日复合指标异常（便捷函数）
    
    Args:
        df_day: 当日数据
        top_n: 返回前N个最严重的异常
    
    Returns:
        异常复合指标字典
    """
    engine = CompositeIndicatorEngine(df_day)
    engine.TOP_N_SURVIVORS = top_n
    return engine.generate_and_select()
