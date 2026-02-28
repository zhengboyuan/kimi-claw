"""
指标进化引擎 - 三轮迭代发现新指标

符合V5.1规范：
- 候选指标进入 registry.json candidates 池
- 已批准指标进入 indicators
- 进化历史记录到 evolution_history
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import numpy as np


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
    
    def evolve(self, round_num: int, device_data: Dict) -> List[Dict]:
        """执行指定轮次的指标进化"""
        strategy = self.ROUNDS.get(round_num)
        
        if strategy == "derive":
            return self._round1_derive(device_data)
        elif strategy == "compose":
            return self._round2_compose(device_data)
        elif strategy == "semantic":
            return self._round3_semantic(device_data)
        
        return []
    
    def _round1_derive(self, data: Dict) -> List[Dict]:
        """第一轮：发现衍生指标
        
        策略：
        1. 找高相关指标对 (>0.9)
        2. 生成 ratio/diff/product 运算
        """
        candidates = []
        approved = self.registry.get_indicators("approved")
        
        # 获取数值型指标
        numeric_indicators = [
            (k, v) for k, v in approved.items()
            if v.get("scope") == "inverter" and "ai" in k
        ]
        
        # 简单规则：功率类指标生成效率类衍生指标
        power_indicators = [k for k, v in numeric_indicators if "power" in v.get("name", "")]
        
        for i, (id1, ind1) in enumerate(numeric_indicators):
            for id2, ind2 in numeric_indicators[i+1:]:
                # 生成 ratio 候选
                cid = f"{id1}_over_{id2}"
                if cid not in self.registry.get_candidates():
                    candidate = {
                        "id": cid,
                        "name": f"{ind1['name']}与{ind2['name']}比值",
                        "description": f"自动发现的衍生指标: {id1}/{id2}",
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
                        "owner": "system"
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
