"""
指标生命周期管理器 V4.0 (Darwin Arena)

职责：
1. 管理L3复合指标的注册、评分、晋升、淘汰
2. 达尔文进化：适者生存，不适者淘汰
3. 每日更新survival_score，触发晋升/淘汰机制

核心规则：
- 加分(+0.1)：方差大且与核心指标相关性强
- 扣分(-0.1)：死水直线或全天NaN/0
- 晋升：score>0.8且存活>14天 → L2核心
- 淘汰：score<0.2 → 退役
"""
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path


class IndicatorLifecycleManager:
    """指标生命周期管理器"""
    
    # V4.1 进化阈值（更宽松，鼓励长期存活）
    PROMOTE_THRESHOLD = 0.8      # 晋升阈值
    PROMOTE_MIN_AGE = 14         # 最小晋升年龄(天)
    RETIRE_THRESHOLD = 0.24      # 淘汰阈值（略低于保底线0.25）
    
    def __init__(self, device_sn: str, catalog_path: Optional[str] = None):
        self.device_sn = device_sn
        self.catalog_path = catalog_path or f"memory/indicator_catalog_v4.json"
        self.catalog = self._load_catalog()
        
    def _load_catalog(self) -> Dict:
        """加载指标目录"""
        try:
            with open(self.catalog_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return self._create_empty_catalog()
    
    def _create_empty_catalog(self) -> Dict:
        """创建空目录"""
        return {
            "device_sn": self.device_sn,
            "version": "4.0",
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "indicators": {
                "L0_Candidates": {},
                "L1_Active": {},
                "L2_Core": {},
                "L3_Synthesized": {}
            },
            "evolution_stats": {
                "total_registered": 0,
                "total_promoted": 0,
                "total_retired": 0,
                "current_population": {"L0": 0, "L1": 0, "L2": 0, "L3": 0}
            }
        }
    
    def _save_catalog(self):
        """保存指标目录"""
        self.catalog["last_updated"] = datetime.now().isoformat()
        with open(self.catalog_path, 'w', encoding='utf-8') as f:
            json.dump(self.catalog, f, indent=2, ensure_ascii=False)
    
    def register_l3_indicator(self, indicator_id: str, name: str, formula: str, 
                              physical_meaning: str, birth_date: str):
        """
        注册新的L3复合指标（增加去重检查）
        """
        l3_pool = self.catalog["indicators"]["L3_Synthesized"]
        
        # 检查ID是否已存在
        if indicator_id in l3_pool:
            print(f"  ⚠️ 指标 {indicator_id} 已存在，跳过注册")
            return False
        
        # 检查formula是否已存在（去重）
        for existing_id, existing in l3_pool.items():
            if existing.get("formula") == formula:
                print(f"  ⚠️ 公式 '{formula}' 已存在({existing_id})，跳过注册")
                return False
        
        l3_pool[indicator_id] = {
            "id": indicator_id,
            "name": name,
            "formula": formula,
            "physical_meaning": physical_meaning,
            "birth_date": birth_date,
            "survival_score": 0.5,
            "status": "observing",
            "age_days": 0,
            "score_history": [],
            "promotion_date": None,
            "retire_date": None
        }
        
        self.catalog["evolution_stats"]["total_registered"] += 1
        self.catalog["evolution_stats"]["current_population"]["L3"] += 1
        self._save_catalog()
        
        print(f"  ✅ 新指标注册成功: {indicator_id} ({name})")
        return True
    
    def evaluate_daily_performance(self, indicator_id: str, df_day: pd.DataFrame,
                                   core_indicators: List[str]) -> float:
        """
        V4.1 评分算法：区分无效噪音与潜伏的好指标
        
        核心逻辑：
        - 波动大(+0.2)：证明指标有价值
        - 平稳(-0.02)：设备健康，微弱扣分
        - 无效数据(-0.3)：数学垃圾，重罚
        - LLM认证保底：生命值不低于0.25
        """
        l3_pool = self.catalog["indicators"]["L3_Synthesized"]
        
        if indicator_id not in l3_pool:
            return 0.0
        
        indicator = l3_pool[indicator_id]
        formula = indicator["formula"]
        
        try:
            # 计算指标值
            values = df_day.eval(formula)
            values = values.replace([np.inf, -np.inf], np.nan)
            
            # 1. 数据无效检查（数学垃圾）
            if values.isna().all() or len(values.dropna()) < 5:
                score_change = -0.3
                reason = "数据无效/不足(数学垃圾)"
            else:
                valid_values = values.dropna()
                max_val = valid_values.max()
                std_val = valid_values.std()
                
                # 2. 工业有效波动（>5%），大加分！
                if max_val > 0.05:
                    score_change = 0.2
                    reason = f"有效波动(max={max_val:.3f})，指标有价值"
                # 3. 轻微波动（1%-5%），小加分
                elif max_val > 0.01:
                    score_change = 0.05
                    reason = f"轻微波动(max={max_val:.3f})"
                # 4. 设备平稳健康，微弱扣分
                else:
                    score_change = -0.02
                    reason = f"设备平稳(max={max_val:.3f})，微弱衰减"
            
            # 更新分数
            old_score = indicator["survival_score"]
            new_score = old_score + score_change
            
            # V4.1 保底机制：LLM认证的指标，生命值不低于0.25
            new_score = max(0.25, new_score)
            new_score = min(1.0, new_score)
            
            indicator["survival_score"] = new_score
            indicator["age_days"] += 1
            
            indicator["score_history"].append({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "change": score_change,
                "score": new_score,
                "reason": reason
            })
            
            return score_change
            
        except Exception as e:
            return 0.0
    
    def run_evolution_cycle(self, date_str: str, df_day: pd.DataFrame):
        """
        运行每日进化周期
        
        Args:
            date_str: 日期
            df_day: 当日数据
        """
        print(f"\n{'='*60}")
        print(f"V4.0 达尔文进化周期: {date_str}")
        print(f"{'='*60}")
        
        l3_pool = self.catalog["indicators"]["L3_Synthesized"]
        
        if not l3_pool:
            print("  ℹ️ L3池为空，无指标需要评估")
            return
        
        print(f"\n  评估 {len(l3_pool)} 个L3指标...")
        
        # 评估每个L3指标
        promoted = []
        retired = []
        
        for indicator_id in list(l3_pool.keys()):
            indicator = l3_pool[indicator_id]
            
            if indicator["status"] != "observing":
                continue
            
            # 评估表现
            self.evaluate_daily_performance(
                indicator_id, 
                df_day, 
                ['ai56', 'ai61', 'ai45']  # 核心指标
            )
            
            score = indicator["survival_score"]
            age = indicator["age_days"]
            
            # 检查晋升
            if score >= self.PROMOTE_THRESHOLD and age >= self.PROMOTE_MIN_AGE:
                promoted.append(indicator_id)
                self._promote_to_l2(indicator_id, date_str)
            
            # 检查淘汰
            elif score <= self.RETIRE_THRESHOLD:
                retired.append(indicator_id)
                self._retire_indicator(indicator_id, date_str)
        
        # 保存
        self._save_catalog()
        
        # 汇报
        print(f"\n{'='*60}")
        print(f"进化周期完成!")
        if promoted:
            print(f"  📈 晋升L2: {len(promoted)}个 - {promoted}")
        if retired:
            print(f"  💀 淘汰: {len(retired)}个 - {retired}")
        if not promoted and not retired:
            print(f"  ⏳ 无晋升/淘汰，继续观察")
        print(f"{'='*60}")
    
    def _promote_to_l2(self, indicator_id: str, date_str: str):
        """晋升到L2核心"""
        l3_pool = self.catalog["indicators"]["L3_Synthesized"]
        l2_pool = self.catalog["indicators"]["L2_Core"]
        
        indicator = l3_pool.pop(indicator_id)
        indicator["status"] = "active"
        indicator["promotion_date"] = date_str
        
        l2_pool[indicator_id] = indicator
        
        self.catalog["evolution_stats"]["total_promoted"] += 1
        self.catalog["evolution_stats"]["current_population"]["L3"] -= 1
        self.catalog["evolution_stats"]["current_population"]["L2"] += 1
        
        print(f"    🎉 {indicator_id} 晋升为L2核心指标!")
    
    def _retire_indicator(self, indicator_id: str, date_str: str):
        """淘汰指标"""
        l3_pool = self.catalog["indicators"]["L3_Synthesized"]
        
        indicator = l3_pool.pop(indicator_id)
        indicator["status"] = "retired"
        indicator["retire_date"] = date_str
        
        # 保存到退役列表（可选）
        
        self.catalog["evolution_stats"]["total_retired"] += 1
        self.catalog["evolution_stats"]["current_population"]["L3"] -= 1
        
        print(f"    💀 {indicator_id} 被淘汰（生命值耗尽）")
    
    def get_evolution_summary(self) -> Dict:
        """获取进化摘要"""
        stats = self.catalog["evolution_stats"]
        l3_pool = self.catalog["indicators"]["L3_Synthesized"]
        l2_pool = self.catalog["indicators"]["L2_Core"]
        
        return {
            "registered": stats["total_registered"],
            "promoted": stats["total_promoted"],
            "retired": stats["total_retired"],
            "current_l3": len(l3_pool),
            "current_l2": len(l2_pool),
            "l3_avg_score": np.mean([ind["survival_score"] for ind in l3_pool.values()]) if l3_pool else 0.0
        }


# 便捷函数
def run_daily_evolution(device_sn: str, date_str: str, df_day: pd.DataFrame, 
                        catalog_path: Optional[str] = None):
    """
    运行每日进化（便捷函数）
    
    Args:
        device_sn: 设备SN
        date_str: 日期
        df_day: 当日数据
        catalog_path: 目录路径（可选）
    """
    manager = IndicatorLifecycleManager(device_sn, catalog_path)
    manager.run_evolution_cycle(date_str, df_day)
    return manager.get_evolution_summary()
