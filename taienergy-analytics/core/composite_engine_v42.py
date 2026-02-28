"""
复合指标变异引擎 V4.2 (扩展版)

V4.2升级：
- 阈值从5%降到3%
- 增加5种新变异类型
- 预期发现10-15个新指标
"""
import pandas as pd
import numpy as np
import itertools
from typing import Dict, List, Optional


class CompositeIndicatorEngineV42:
    """
    复合指标变异引擎 V4.2 (扩展版)
    """
    
    # 物理分组配置
    PV_INPUT_CURRENTS = ['ai10', 'ai12', 'ai16', 'ai20']
    GRID_VOLTAGES = ['ai49', 'ai50', 'ai51']
    POWER_METRICS = ['ai45', 'ai56']
    
    # V4.3: 动态分层阈值
    FLUCTUATION_THRESHOLD_L0 = 0.003  # 0.3% - Day1
    FLUCTUATION_THRESHOLD_L1 = 0.002  # 0.2% - Day2
    FLUCTUATION_THRESHOLD_L2 = 0.001  # 0.1% - Day3+
    
    def __init__(self, df_day: pd.DataFrame, l3_formulas: Optional[Dict] = None):
        self.df_day = df_day
        self.l3_formulas = l3_formulas or {}
        self.candidates = {}
        
    def generate_and_evaluate(self) -> Dict:
        """生成变异并评估"""
        print("  🧬 V4.2 扩展版物种孵化器启动...")
        
        # 发现候选
        self._discover_candidates()
        
        # 计算已注册L3
        l3_values = self._calculate_registered_l3()
        
        print(f"  📊 发现 {len(self.candidates)} 个候选突变")
        print(f"  📈 计算 {len(l3_values)} 个已注册L3指标")
        
        return {
            'candidates': self.candidates,
            'l3_values': l3_values
        }
    
    def _discover_candidates(self):
        """V4.2: 扩展候选发现"""
        # V4.3: 动态阈值 + 时序特征
        self._discover_pv_candidates_v43()
        self._discover_efficiency_candidates_v43()
        self._discover_unbalance_candidates_v43()
        self._discover_mutation_candidates()
        self._discover_rolling_stats_candidates()
    
    def _discover_pv_candidates(self):
        """组串差异（原有）"""
        pv_cols = [c for c in self.PV_INPUT_CURRENTS if c in self.df_day.columns]
        if len(pv_cols) < 2:
            return
        
        for c1, c2 in itertools.combinations(pv_cols, 2):
            s1 = self.df_day[c1].replace(0, np.nan).dropna().reset_index(drop=True)
            s2 = self.df_day[c2].replace(0, np.nan).dropna().reset_index(drop=True)
            
            min_len = min(len(s1), len(s2))
            if min_len < 10:
                continue
            
            s1, s2 = s1.iloc[:min_len], s2.iloc[:min_len]
            diff_pct = np.abs(s1 - s2) / s1
            
            if diff_pct.max() > self.FLUCTUATION_THRESHOLD_L0:
                name = f"diff_pct_{c1}_{c2}"
                self.candidates[name] = {
                    'formula': f"abs({c1} - {c2}) / {c1}",
                    'feature': f"离散率{diff_pct.max()*100:.1f}%",
                    'max_value': float(diff_pct.max()),
                    'mean_value': float(diff_pct.mean())
                }
    
    def _discover_efficiency_candidates(self):
        """效率损耗（原有）"""
        if 'ai45' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        loss = 1.0 - (self.df_day['ai56'] / self.df_day['ai45'])
        loss = loss.replace([np.inf, -np.inf], np.nan).dropna()
        loss = loss[loss > 0]  # 只保留正损耗
        
        if len(loss) > 0 and loss.max() > self.FLUCTUATION_THRESHOLD_L0:
            self.candidates['efficiency_loss'] = {
                'formula': "1.0 - (ai56 / ai45)",
                'feature': f"损耗率{loss.max()*100:.1f}%",
                'max_value': float(loss.max()),
                'mean_value': float(loss.mean())
            }
    
    def _discover_unbalance_candidates(self):
        """三相不平衡（原有）"""
        vol_cols = [c for c in self.GRID_VOLTAGES if c in self.df_day.columns]
        if len(vol_cols) < 3:
            return
        
        mean_v = self.df_day[vol_cols].mean(axis=1)
        unbalance = (self.df_day[vol_cols].max(axis=1) - self.df_day[vol_cols].min(axis=1)) / mean_v
        
        if unbalance.max() > self.FLUCTUATION_THRESHOLD_L0:
            self.candidates['vol_unbalance'] = {
                'formula': "(max(Ua,Ub,Uc) - min(Ua,Ub,Uc)) / mean(U)",
                'feature': f"不平衡度{unbalance.max()*100:.1f}%",
                'max_value': float(unbalance.max()),
                'mean_value': float(unbalance.mean())
            }
    
    # ==================== V4.2 新增 ====================
    
    def _discover_power_trend_candidates(self):
        """V4.2新增：功率趋势突变"""
        if 'ai56' not in self.df_day.columns:
            return
        
        power = self.df_day['ai56'].replace(0, np.nan).dropna()
        if len(power) < 5:
            return
        
        # 计算3点差分
        trend = power.diff(3).abs()
        
        if trend.max() > 2000:  # 2kW突变（从5kW降到2kW）
            self.candidates['power_trend_3pt'] = {
                'formula': "abs(diff(ai56, 3))",
                'feature': f"3点趋势突变{trend.max():.0f}W",
                'max_value': float(trend.max()),
                'mean_value': float(trend.mean())
            }
    
    def _discover_temp_correlation_candidates(self):
        """V4.2新增：温度-效率相关性"""
        if 'ai61' not in self.df_day.columns or 'ai56' not in self.df_day.columns or 'ai45' not in self.df_day.columns:
            return
        
        # 计算效率
        efficiency = self.df_day['ai56'] / self.df_day['ai45']
        efficiency = efficiency.replace([np.inf, -np.inf], np.nan).dropna()
        
        temp = self.df_day['ai61']
        
        # 对齐长度
        min_len = min(len(efficiency), len(temp))
        if min_len < 10:
            return
        
        eff = efficiency.iloc[:min_len]
        t = temp.iloc[:min_len]
        
        # 计算相关性
        corr = eff.corr(t)
        
        if not np.isnan(corr) and abs(corr) > 0.5:
            self.candidates['temp_efficiency_corr'] = {
                'formula': "corr(ai56/ai45, ai61)",
                'feature': f"温效相关性r={corr:.2f}",
                'max_value': abs(float(corr)),
                'mean_value': abs(float(corr))
            }
    
    def _discover_current_std_candidates(self):
        """V4.2新增：四组串电流标准差"""
        pv_cols = [c for c in self.PV_INPUT_CURRENTS if c in self.df_day.columns]
        if len(pv_cols) < 4:
            return
        
        # 计算四路电流的标准差
        currents = self.df_day[pv_cols].replace(0, np.nan)
        std = currents.std(axis=1).dropna()
        
        if len(std) > 0 and std.max() > 1.0:  # 1A标准差（从2A降到1A）
            self.candidates['current_std_4string'] = {
                'formula': "std(ai10, ai12, ai16, ai20)",
                'feature': f"四组串std{std.max():.1f}A",
                'max_value': float(std.max()),
                'mean_value': float(std.mean())
            }
    
    def _discover_io_ratio_candidates(self):
        """V4.2新增：输入输出比波动"""
        if 'ai45' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        ratio = self.df_day['ai56'] / self.df_day['ai45']
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        ratio = ratio[(ratio > 0.5) & (ratio < 1.0)]  # 合理范围
        
        if len(ratio) > 0 and ratio.std() > 0.01:  # 1%波动（从3%降到1%）
            self.candidates['io_ratio_var'] = {
                'formula': "ai56 / ai45",
                'feature': f"转换效率比波动{ratio.std()*100:.1f}%",
                'max_value': float(ratio.max()),
                'mean_value': float(ratio.mean())
            }
    
    def _discover_temp_normalized_candidates(self):
        """V4.2新增：单位功率温升"""
        if 'ai61' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        temp_rise = self.df_day['ai61'] / self.df_day['ai56'].replace(0, np.nan)
        temp_rise = temp_rise.replace([np.inf, -np.inf], np.nan).dropna()
        
        if len(temp_rise) > 0 and temp_rise.max() > 0.0005:  # 0.0005℃/W（从0.001降到0.0005）
            self.candidates['temp_per_watt'] = {
                'formula': "ai61 / ai56",
                'feature': f"单位功率温升{temp_rise.max()*1000:.2f}℃/kW",
                'max_value': float(temp_rise.max()),
                'mean_value': float(temp_rise.mean())
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
                    'max': float(values.max()),
                    'min': float(values.min())
                }
            except Exception as e:
                print(f"    ⚠️ 计算 {indicator_id} 失败")
        
        return l3_values


# 便捷函数
def discover_daily_candidates_v42(df_day: pd.DataFrame, l3_formulas: Optional[Dict] = None) -> Dict:
    """V4.2便捷函数"""
    engine = CompositeIndicatorEngineV42(df_day, l3_formulas)
    return engine.generate_and_evaluate()
