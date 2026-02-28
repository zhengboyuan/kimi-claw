"""
复合指标变异引擎 V4.0 (物种孵化器)

核心转变：
- V3.1: 检测异常 → 触发告警
- V4.0: 发现突变 → 提交候选 → 等待LLM评审

职责：
1. 穷举变异，生成候选公式
2. 检测强烈波动特征
3. 提交候选给分类学家评审
4. 每日计算已注册L3指标
"""
import pandas as pd
import numpy as np
import itertools
from typing import Dict, List, Optional


class CompositeIndicatorEngineV40:
    """
    复合指标变异引擎 V4.0 (物种孵化器)
    
    不再触发告警，而是发现新物种！
    """
    
    # 物理分组配置
    PV_INPUT_CURRENTS = ['ai10', 'ai12', 'ai16', 'ai20']
    GRID_VOLTAGES = ['ai49', 'ai50', 'ai51']
    POWER_METRICS = ['ai45', 'ai56']
    
    # 突变检测阈值（发现候选）
    FLUCTUATION_THRESHOLD = 0.05  # 5%波动即视为有特征
    
    def __init__(self, df_day: pd.DataFrame, l3_formulas: Optional[Dict] = None):
        """
        初始化引擎
        
        Args:
            df_day: 当日数据
            l3_formulas: 已注册的L3公式 {id: formula}
        """
        self.df_day = df_day
        self.l3_formulas = l3_formulas or {}
        self.mutated_df = pd.DataFrame(index=df_day.index)
        self.candidates = {}
        
    def generate_and_evaluate(self) -> Dict:
        """
        生成变异并评估
        
        Returns:
            {
                'candidates': {新发现的候选公式},
                'l3_values': {已注册L3指标的当日值}
            }
        """
        print("  🧬 V4.0 物种孵化器启动...")
        
        # 1. 穷举变异，发现候选
        self._discover_candidates()
        
        # 2. 计算已注册L3指标
        l3_values = self._calculate_registered_l3()
        
        print(f"  📊 发现 {len(self.candidates)} 个候选突变")
        print(f"  📈 计算 {len(l3_values)} 个已注册L3指标")
        
        return {
            'candidates': self.candidates,
            'l3_values': l3_values
        }
    
    def _discover_candidates(self):
        """发现候选公式（基于强烈波动特征）"""
        # 1. 组串差异候选
        self._discover_pv_candidates()
        
        # 2. 效率损耗候选
        self._discover_efficiency_candidates()
        
        # 3. 三相不平衡候选
        self._discover_unbalance_candidates()
    
    def _discover_pv_candidates(self):
        """发现组串差异候选"""
        pv_cols = [c for c in self.PV_INPUT_CURRENTS if c in self.df_day.columns]
        if len(pv_cols) < 2:
            return
        
        for c1, c2 in itertools.combinations(pv_cols, 2):
            # 获取数据，处理NaN和0，重置索引为位置索引
            s1 = self.df_day[c1].replace(0, np.nan).dropna().reset_index(drop=True)
            s2 = self.df_day[c2].replace(0, np.nan).dropna().reset_index(drop=True)
            
            # 使用最小长度对齐
            min_len = min(len(s1), len(s2))
            if min_len < 10:
                continue
            
            s1 = s1.iloc[:min_len]
            s2 = s2.iloc[:min_len]
            
            # 计算差异率
            diff_pct = np.abs(s1 - s2) / s1
            
            # 检查是否有强烈波动
            if diff_pct.max() > self.FLUCTUATION_THRESHOLD:
                name = f"diff_pct_{c1}_{c2}"
                self.candidates[name] = {
                    'formula': f"abs({c1} - {c2}) / {c1}",
                    'feature': f"最大离散率{diff_pct.max()*100:.1f}%",
                    'max_value': float(diff_pct.max()),
                    'mean_value': float(diff_pct.mean())
                }
    
    def _discover_efficiency_candidates(self):
        """发现效率损耗候选"""
        if 'ai45' not in self.df_day.columns or 'ai56' not in self.df_day.columns:
            return
        
        # 计算效率损耗
        efficiency_loss = np.where(
            self.df_day['ai45'] > 500,
            1.0 - (self.df_day['ai56'] / self.df_day['ai45']),
            np.nan
        )
        
        # 检查是否有强烈波动
        valid_loss = efficiency_loss[~np.isnan(efficiency_loss)]
        if len(valid_loss) > 0 and np.max(valid_loss) > self.FLUCTUATION_THRESHOLD:
            self.candidates['efficiency_loss'] = {
                'formula': "1.0 - (ai56 / ai45)",
                'feature': f"最大损耗率{np.max(valid_loss)*100:.1f}%",
                'max_value': float(np.max(valid_loss)),
                'mean_value': float(np.mean(valid_loss))
            }
    
    def _discover_unbalance_candidates(self):
        """发现三相不平衡候选"""
        vol_cols = [c for c in self.GRID_VOLTAGES if c in self.df_day.columns]
        if len(vol_cols) < 3:
            return
        
        # 计算不平衡度
        mean_v = self.df_day[vol_cols].mean(axis=1)
        max_v = self.df_day[vol_cols].max(axis=1)
        min_v = self.df_day[vol_cols].min(axis=1)
        unbalance = (max_v - min_v) / mean_v
        
        # 检查是否有强烈波动
        if np.max(unbalance) > self.FLUCTUATION_THRESHOLD:
            self.candidates['vol_unbalance'] = {
                'formula': "(max(ai49,ai50,ai51) - min(ai49,ai50,ai51)) / mean(ai49,ai50,ai51)",
                'feature': f"最大不平衡度{np.max(unbalance)*100:.1f}%",
                'max_value': float(np.max(unbalance)),
                'mean_value': float(np.mean(unbalance))
            }
    
    def _calculate_registered_l3(self) -> Dict:
        """计算已注册L3指标的当日值"""
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
                print(f"    ⚠️ 计算 {indicator_id} 失败: {str(e)[:30]}")
        
        return l3_values


# 便捷函数
def discover_daily_candidates(df_day: pd.DataFrame, l3_formulas: Optional[Dict] = None) -> Dict:
    """
    发现当日候选公式（便捷函数）
    
    Args:
        df_day: 当日数据
        l3_formulas: 已注册L3公式
    
    Returns:
        候选公式和L3计算值
    """
    engine = CompositeIndicatorEngineV40(df_day, l3_formulas)
    return engine.generate_and_evaluate()
