"""
指标进化引擎 - 三轮迭代发现新指标

符合V5.1规范：
- 候选指标进入 registry.json candidates 池
- 已批准指标进入 indicators
- 进化历史记录到 evolution_history

V5.1.1更新：第一轮改为从原始数据计算（最小改动）
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import numpy as np

# 添加技能路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from skills.skill_1_data_collector import DataCollector
    from config.device_config import DEVICES
    HAS_RAW_DATA = True
except ImportError:
    HAS_RAW_DATA = False
    print("⚠️ 无法导入 DataCollector，将使用注册表模式")


class IndicatorRegistry:
    """指标注册表管理"""
    
    def __init__(self, path: str = "config/indicators/registry.json"):
        self.path = path
        self.data = self._load()
    
    def _load(self) -> Dict:
        if os.path.exists(self.path):
            with open(self.path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"version": "v5.1", "indicators": {}, "candidates": {}, "evolution_history": []}
    
    def save(self):
        self.data["updated_at"] = datetime.now().isoformat()
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
    
    def get_indicators(self, status: str = "approved") -> Dict:
        """获取已批准指标"""
        return {k: v for k, v in self.data["indicators"].items() 
                if v.get("lifecycle_status") == status}
    
    def get_candidates(self) -> Dict:
        """获取候选指标池"""
        return self.data.get("candidates", {})
    
    def add_candidate(self, candidate: Dict) -> bool:
        """添加候选指标，自动去重"""
        candidates = self.get_candidates()
        
        # 去重：公式结构相同
        for existing in candidates.values():
            if self._formula_similar(candidate.get("formula"), existing.get("formula")):
                return False
        
        cid = candidate["id"]
        candidates[cid] = candidate
        self.data["candidates"] = candidates
        return True
    
    def approve_candidate(self, cid: str) -> bool:
        """候选指标转正"""
        candidates = self.get_candidates()
        if cid not in candidates:
            return False
        
        candidate = candidates.pop(cid)
        candidate["lifecycle_status"] = "approved"
        candidate["approved_at"] = datetime.now().isoformat()
        
        self.data["indicators"][cid] = candidate
        self.data["candidates"] = candidates
        return True
    
    def add_evolution_record(self, round_num: int, found: int, approved: int):
        """记录进化历史"""
        record = {
            "round": round_num,
            "date": datetime.now().isoformat(),
            "candidates_found": found,
            "approved": approved
        }
        self.data.setdefault("evolution_history", []).append(record)
    
    def _formula_similar(self, f1: str, f2: str) -> bool:
        """判断公式是否相似（简单版本）"""
        if not f1 or not f2:
            return False
        # 标准化后比较
        s1 = f1.replace(" ", "").lower()
        s2 = f2.replace(" ", "").lower()
        return s1 == s2


class IndicatorEvolution:
    """指标进化引擎 - 三轮迭代"""
    
    ROUNDS = {
        1: "derive",      # 衍生：基础指标运算
        2: "compose",     # 组合：多指标融合  
        3: "semantic"     # 语义：业务场景化
    }
    
    def __init__(self, registry_path: str = "config/indicators/registry.json"):
        self.registry = IndicatorRegistry(registry_path)
    
    def suggest_next_round(self) -> int:
        """建议下一轮次"""
        history = self.registry.data.get("evolution_history", [])
        if not history:
            return 1
        
        last_round = history[-1]["round"]
        last_found = history[-1]["candidates_found"]
        
        # 收敛判断：第3轮后或发现<3个则停止
        if last_round >= 3 or last_found < 3:
            return 0  # 停止
        
        return last_round + 1
    
    def evolve(self, round_num: int, date_str: str) -> List[Dict]:
        """执行指定轮次的指标进化
        
        V5.1.1更新：传入date_str而非device_data，支持原始数据获取
        """
        strategy = self.ROUNDS.get(round_num)
        
        if strategy == "derive":
            return self._round1_derive(date_str)  # 传入日期
        elif strategy == "compose":
            return self._round2_compose(date_str)
        elif strategy == "semantic":
            return self._round3_semantic(date_str)
        
        return []
    
    def _round1_derive(self, date_str: str) -> List[Dict]:
        """第一轮：从原始数据发现衍生指标（V5.1.1最小改动）
        
        策略：
        1. 尝试从API获取原始数据
        2. 如果成功，基于原始测点发现关系
        3. 如果失败，回退到注册表模式
        """
        candidates = []
        
        # 尝试从原始数据发现
        if HAS_RAW_DATA and DEVICES:
            try:
                candidates = self._round1_from_raw_data(date_str)
                if candidates:
                    print(f"  ✓ 从原始数据发现 {len(candidates)} 个候选")
                    return candidates
            except Exception as e:
                print(f"  ⚠️ 原始数据获取失败: {e}，回退到注册表模式")
        
        # 回退到原有逻辑（基于注册表）
        return self._round1_from_registry()
    
    def _round1_from_raw_data(self, date_str: str) -> List[Dict]:
        """从原始数据发现衍生指标"""
        candidates = []
        
        # 取第一台设备作为样本
        device_sn = list(DEVICES.keys())[0]
        collector = DataCollector(device_sn)
        
        # 拉取原始数据
        raw_data = collector.collect_daily_data(date_str)
        
        # 提取数值型指标（取日均值）
        numeric_data = {}
        for code, df in raw_data.items():
            if not df.empty and 'value' in df.columns:
                numeric_data[code] = df['value'].mean()
        
        # 找相关指标对（简化规则）
        codes = list(numeric_data.keys())
        for i, code1 in enumerate(codes):
            for code2 in codes[i+1:]:
                # 如果指标相关，生成ratio候选
                if self._is_related(code1, code2):
                    cid = f"{code1}_over_{code2}"
                    if cid not in self.registry.get_candidates():
                        candidate = {
                            "id": cid,
                            "name": f"{code1}与{code2}比值",
                            "description": f"从原始数据发现的衍生指标: {code1}/{code2}",
                            "source": "llm",
                            "scope": "inverter",
                            "level": "L2",
                            "formula": f"{code1} / {code2}",
                            "inputs": [code1, code2],
                            "aggregation": "avg",
                            "unit": "ratio",
                            "lifecycle_status": "pending",
                            "discovered_at": datetime.now().isoformat(),
                            "round": 1,
                            "owner": "system",
                            "data_source": "raw_api"  # 标记来源
                        }
                        if self.registry.add_candidate(candidate):
                            candidates.append(candidate)
        
        return candidates
    
    def _is_related(self, code1: str, code2: str) -> bool:
        """判断两个指标是否相关（优化版过滤规则）
        
        优化策略：
        1. 只保留功率类核心指标（输入/输出功率比）
        2. 只保留组串电流间关系
        3. 过滤冗余的电压/电流交叉组合
        """
        # 高价值：输入输出功率比（转换效率）
        if (code1 == 'ai56' and code2 == 'ai45') or (code1 == 'ai45' and code2 == 'ai56'):
            return True
        
        # 高价值：组串电流间关系（组串一致性）
        string_currents = ['ai10', 'ai12', 'ai16', 'ai20']  # 4路组串电流
        if code1 in string_currents and code2 in string_currents and code1 != code2:
            return True
        
        # 高价值：三相电流间关系（三相不平衡）
        grid_currents = ['ai52', 'ai53', 'ai54']  # 三相电流
        if code1 in grid_currents and code2 in grid_currents and code1 != code2:
            return True
        
        # 中等价值：电压/功率（等效阻抗）- 只保留代表性组合
        if code1 == 'ai12' and code2 == 'ai56':  # PV电压/输出功率
            return True
        
        # 过滤：其他所有组合（避免125个冗余）
        return False
    
    def _round1_from_registry(self) -> List[Dict]:
        """原有逻辑：从注册表发现衍生指标（回退模式）"""
        candidates = []
        approved = self.registry.get_indicators("approved")
        
        # 获取数值型指标
        numeric_indicators = [
            (k, v) for k, v in approved.items()
            if v.get("scope") == "inverter"
        ]
        
        for i, (id1, ind1) in enumerate(numeric_indicators):
            for id2, ind2 in numeric_indicators[i+1:]:
                cid = f"{id1}_over_{id2}"
                if cid not in self.registry.get_candidates():
                    candidate = {
                        "id": cid,
                        "name": f"{ind1['name']}与{ind2['name']}比值",
                        "description": f"从注册表发现的衍生指标: {id1}/{id2}",
                        "source": "llm",
                        "scope": "inverter",
                        "level": "L2",
                        "formula": f"{id1} / {id2}",
                        "inputs": [id1, id2],
                        "aggregation": "avg",
                        "unit": "ratio",
                        "lifecycle_status": "pending",
                        "discovered_at": datetime.now().isoformat(),
                        "round": 1,
                        "owner": "system",
                        "data_source": "registry"  # 标记来源
                    }
                    if self.registry.add_candidate(candidate):
                        candidates.append(candidate)
        
        return candidates
    
    def _round2_compose(self, data: Dict) -> List[Dict]:
        """第二轮：组合构造
        
        策略：
        1. 用已批准的衍生指标
        2. 构造加权组合指标
        """
        candidates = []
        approved = self.registry.get_indicators("approved")
        
        # 找效率类指标组合成健康度
        efficiency_like = [k for k, v in approved.items() 
                          if "ratio" in v.get("unit", "") or "效率" in v.get("name", "")]
        
        if len(efficiency_like) >= 2:
            cid = "composite_health_score"
            if cid not in self.registry.get_candidates():
                weights = [0.5, 0.3, 0.2][:len(efficiency_like)]
                formula_parts = [f"{w}*{ind}" for w, ind in zip(weights, efficiency_like)]
                
                candidate = {
                    "id": cid,
                    "name": "综合健康度",
                    "description": "多维度效率指标加权组合",
                    "source": "constructed",
                    "scope": "inverter", 
                    "level": "L2",
                    "formula": " + ".join(formula_parts),
                    "inputs": efficiency_like,
                    "aggregation": "avg",
                    "unit": "score",
                    "lifecycle_status": "pending",
                    "discovered_at": datetime.now().isoformat(),
                    "round": 2,
                    "owner": "system"
                }
                if self.registry.add_candidate(candidate):
                    candidates.append(candidate)
        
        return candidates
    
    def _round3_semantic(self, data: Dict) -> List[Dict]:
        """第三轮：语义化场景指标
        
        策略：
        1. 给复合指标赋予业务语义
        2. 生成带条件的场景指标
        """
        candidates = []
        approved = self.registry.get_indicators("approved")
        
        # 构造场景指标：高温效率风险
        has_temp = any("温度" in v.get("name", "") for v in approved.values())
        has_eff = any("效率" in v.get("name", "") or "ratio" in v.get("unit", "") 
                   for v in approved.values())
        
        if has_temp and has_eff:
            cid = "high_temp_derating_risk"
            if cid not in self.registry.get_candidates():
                candidate = {
                    "id": cid,
                    "name": "高温降载风险",
                    "description": "温度高于45度且效率低于96%时触发",
                    "source": "llm",
                    "scope": "inverter",
                    "level": "L2",
                    "formula": "IF(temp > 45 AND efficiency < 0.96, 1, 0)",
                    "inputs": ["temp", "efficiency"],
                    "aggregation": "max",
                    "unit": "flag",
                    "lifecycle_status": "pending",
                    "discovered_at": datetime.now().isoformat(),
                    "round": 3,
                    "owner": "system"
                }
                if self.registry.add_candidate(candidate):
                    candidates.append(candidate)
        
        return candidates
    
    def run_full_evolution(self, device_data: Dict) -> Dict:
        """运行完整的三轮进化"""
        results = {
            "rounds": [],
            "total_candidates": 0,
            "converged": False
        }
        
        for round_num in [1, 2, 3]:
            candidates = self.evolve(round_num, device_data)
            
            results["rounds"].append({
                "round": round_num,
                "strategy": self.ROUNDS[round_num],
                "candidates_found": len(candidates),
                "candidates": [c["id"] for c in candidates]
            })
            results["total_candidates"] += len(candidates)
            
            # 记录历史
            self.registry.add_evolution_record(round_num, len(candidates), 0)
        
        # 收敛判断
        last_round = results["rounds"][-1]
        results["converged"] = last_round["candidates_found"] < 3
        
        self.registry.save()
        return results
