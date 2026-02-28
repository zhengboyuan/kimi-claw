"""
复合指标变异引擎 V4.3 (迭代优化版)

核心改进：
- 动态阈值：每日自动降低（0.3%→0.2%→0.1%）
- 时序特征：突变检测、滚动统计、变化率
- 保证每日有新发现
"""
import pandas as pd
import numpy as np
import itertools
from typing import Dict, List, Optional


class CompositeIndicatorEngineV43:
    """V4.3 迭代优化版"""
    
    PV_INPUT_CURRENTS = ['ai10', 'ai12', 'ai16', 'ai20']
    GRID_VOLTAGES = ['ai49', 'ai50', 'ai51']
    
    # 动态阈值
    THRESHOLD_DAY1 = 0.003  # 0.3%
    THRESHOLD_DAY2 = 0.002  # 0.2%
    THRESHOLD_DAY3 = 0.001  # 0.1%
    
    def __init__(self, df_day: pd.DataFrame, day_index: int = 0, l3_formulas: Optional[Dict] = None):
        self.df_day = df_day
        self.day_index = day_index
        self.l3_formulas = l3_formulas or {}
        self.candidates = {}
        
        # 根据天数选择阈值
        if day_index == 0:
            self.threshold = self.THRESHOLD_DAY1
        elif day_index == 1:
            self.threshold = self.THRESHOLD_DAY2
        else:
            self.threshold = self.THRESHOLD_DAY3
    
    def generate_and_evaluate(self) -> Dict:
        """生成变异并评估"""
        print(f"  🧬 V4.3 迭代优化版 (Day{self.day_index+1}, 阈值{self.threshold*100:.2f}%)")
        
        self._discover_candidates()
        l3_values = self._calculate_registered_l3()
        
        print(f"  📊 发现 {len(self.candidates)} 个候选")
        return {'candidates': self.candidates, 'l3_values': l3_values}
    
    def _discover_candidates(self):
        """发现候选（动态阈值+时序特征）"""
        # 基础变异（动态阈值）
        self._discover_pv_candidates_v43()
        self._discover_efficiency_candidates_v43()
        self._discover_unbalance_candidates_v43()
        
        # 时序特征（每日都有新发现）
        self._discover_mutation_candidates()
        self._discover_rolling_stats_candidates()
        self._discover_rate_of_change_candidates()
    
    def _discover_pv_candidates_v43(self):
        """组串差异（V4.3动态阈值）"""
        pv_cols = [c for c in self.PV_INPUT_CURRENTS if c in self.df_day.columns]
        if len(pv_cols) < 2:
            return
        
        for c1, c2 in itertools.combinations(pv_cols, 2):
            s1 = self.df_day[c1].replace(0, np.nan).dropna()
            s2 = self.df_day[c2].replace(0, np.nan).dropna()
            
            min_len = min(len(s1), len(s2))
            if min_len < 10:
                continue
            
            s1, s2 = s1.iloc[:min_len], s2.iloc[:min_len]
            diff_pct = np.abs(s1 - s2) / s1
            
            if diff_pct.max() > self.threshold:
                name = f"diff_pct_{c1}_{c2}_d{self.day_index}"
                self.candidates[name] = {
                    'formula': f"abs({c1} - {c2}) / {c1}",
                    'feature': f"离散率{diff_pct.max()*100:.2f}%",
                    'max_value': float(diff_pct.max()),
                    'threshold_used': self.threshold
                }
    
    def _discover_efficiency_candidates_v43(self):
        """效率损耗（V4.3动态阈值）"""
        if 'ai45' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        loss = 1.0 - (self.df_day['ai56'] / self.df_day['ai45'])
        loss = loss.replace([np.inf, -np.inf], np.nan).dropna()
        loss = loss[loss > 0]
        
        if len(loss) > 0 and loss.max() > self.threshold:
            name = f"efficiency_loss_d{self.day_index}"
            self.candidates[name] = {
                'formula': "1.0 - (ai56 / ai45)",
                'feature': f"损耗率{loss.max()*100:.2f}%",
                'max_value': float(loss.max()),
                'threshold_used': self.threshold
            }
    
    def _discover_unbalance_candidates_v43(self):
        """三相不平衡（V4.3动态阈值）"""
        vol_cols = [c for c in self.GRID_VOLTAGES if c in self.df_day.columns]
        if len(vol_cols) < 3:
            return
        
        mean_v = self.df_day[vol_cols].mean(axis=1)
        unbalance = (self.df_day[vol_cols].max(axis=1) - self.df_day[vol_cols].min(axis=1)) / mean_v
        
        if unbalance.max() > self.threshold:
            name = f"vol_unbalance_d{self.day_index}"
            self.candidates[name] = {
                'formula': "(max(ai49,ai50,ai51) - min(ai49,ai50,ai51)) / mean([ai49,ai50,ai51])",
                'feature': f"不平衡度{unbalance.max()*100:.2f}%",
                'max_value': float(unbalance.max()),
                'threshold_used': self.threshold
            }
    
    # ==================== V4.3 时序特征 ====================
    
    def _discover_mutation_candidates(self):
        """突变检测（每日新发现）"""
        if 'ai56' not in self.df_day.columns:
            return
        
        power = self.df_day['ai56'].replace(0, np.nan).dropna()
        if len(power) < 5:
            return
        
        # 1点差分（即时突变）
        diff1 = power.diff().abs()
        if diff1.max() > 500:  # 500W突变
            name = f"power_mutation_1pt_d{self.day_index}"
            self.candidates[name] = {
                'formula': "abs(diff(ai56))",
                'feature': f"1点突变{diff1.max():.0f}W",
                'max_value': float(diff1.max())
            }
        
        # 3点差分（趋势突变）
        diff3 = power.diff(3).abs()
        if diff3.max() > 1000:
            name = f"power_mutation_3pt_d{self.day_index}"
            self.candidates[name] = {
                'formula': "abs(diff(ai56, 3))",
                'feature': f"3点突变{diff3.max():.0f}W",
                'max_value': float(diff3.max())
            }
    
    def _discover_rolling_stats_candidates(self):
        """滚动统计（每日新发现）"""
        if 'ai10' not in self.df_day.columns:
            return
        
        # 滚动标准差（波动性）
        rolling_std = self.df_day['ai10'].rolling(window=5).std()
        if rolling_std.max() > 0.5:  # 0.5A波动
            name = f"pv3_rolling_std_d{self.day_index}"
            self.candidates[name] = {
                'formula': "rolling_std(ai10, 5)",
                'feature': f"5点滚动std{rolling_std.max():.2f}A",
                'max_value': float(rolling_std.max())
            }
        
        # 滚动均值偏差
        rolling_mean = self.df_day['ai10'].rolling(window=10).mean()
        deviation = np.abs(self.df_day['ai10'] - rolling_mean) / rolling_mean
        if deviation.max() > 0.1:  # 10%偏差
            name = f"pv3_mean_deviation_d{self.day_index}"
            self.candidates[name] = {
                'formula': "abs(ai10 - rolling_mean(ai10, 10)) / rolling_mean(ai10, 10)",
                'feature': f"均值偏差{deviation.max()*100:.1f}%",
                'max_value': float(deviation.max())
            }
    
    def _discover_rate_of_change_candidates(self):
        """变化率（每日新发现）"""
        if 'ai61' not in self.df_day.columns:
            return
        
        temp = self.df_day['ai61']
        if len(temp) < 2:
            return
        
        # 温度变化率
        temp_change = temp.diff().abs()
        if temp_change.max() > 2:  # 2度变化
            name = f"temp_change_rate_d{self.day_index}"
            self.candidates[name] = {
                'formula': "abs(diff(ai61))",
                'feature': f"温度变化{temp_change.max():.1f}℃",
                'max_value': float(temp_change.max())
            }
        
        # 温度加速度
        if len(temp) >= 3:
            temp_accel = temp.diff().diff().abs()
            if temp_accel.max() > 1:
                name = f"temp_acceleration_d{self.day_index}"
                self.candidates[name] = {
                    'formula': "abs(diff(diff(ai61)))",
                    'feature': f"温度加速度{temp_accel.max():.1f}℃",
                    'max_value': float(temp_accel.max())
                }
    
    def _calculate_registered_l3(self) -> Dict:
        """计算已注册L3指标"""
        l3_values = {}
        for indicator_id, formula in self.l3_formulas.items():
            try:
                values = self.df_day.eval(formula)
                l3_values[indicator_id] = {
                    'values': values,
                    'mean': float(values.mean()),
                    'std': float(values.std()),
                    'max': float(values.max())
                }
            except:
                pass
        return l3_values


# 便捷函数
def discover_daily_candidates_v43(df_day: pd.DataFrame, day_index: int, l3_formulas: Optional[Dict] = None) -> Dict:
    """V4.3便捷函数"""
    engine = CompositeIndicatorEngineV43(df_day, day_index, l3_formulas)
    return engine.generate_and_evaluate()
