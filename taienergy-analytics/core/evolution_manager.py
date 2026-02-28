"""
指标体系进化管理器
管理四级指标体系：候选池(L0) → 活跃指标(L1) → 核心指标(L2) → 复合指标(L3)

第一阶段改造：
- 调整升级阈值：L1 >= 0.5 (原 0.6), L2 >= 0.85 (提高门槛)
- 增加稳定性要求：连续达标 + 低波动 + 无下降趋势
- 配合差异化评分，让优质指标自然上浮
"""
import json
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from pathlib import Path
import pandas as pd


class IndicatorEvolutionManager:
    """
    指标体系进化管理器
    
    四级体系：
    - L0 候选池 (Candidates): 新发现的点位，仅基础统计
    - L1 活跃指标 (Active): 评分 >= 0.5，记录趋势、变点检测
    - L2 核心指标 (Core): 评分 >= 0.85，每日深度巡检，触发 LLM
    - L3 复合指标 (Synthesized): LLM 建议的组合指标
    
    进化规则（从配置文件读取）：
    - 升级：连续 3 天评分达标 + 稳定性检查 → 升级
    - 降级：连续 7 天数据为 0/空 → 进入静默池
    - 静默：7 天无数据 → 静默池
    - 淘汰：30 天无数据 → 从候选池移除（哨兵指标除外）
    """
    
    # 默认阈值（当配置文件不存在时使用）
    DEFAULT_CONFIG = {
        'l1_threshold': {'score': 0.5, 'consecutive_days': 3},
        'l2_threshold': {'score': 0.85, 'consecutive_days': 3, 'max_std': 0.1, 'min_trend_slope': -0.05},
        'downgrade_threshold': {'dormant_days': 7, 'silent_days': 7, 'remove_days': 30}
    }
    
    # 级别定义
    LEVELS = {
        "L0": "candidates",
        "L1": "active", 
        "L2": "core",
        "L3": "synthesized"
    }
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.memory_dir = Path(memory_dir)
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载配置
        self.config = self._load_config()
        
        # 从配置读取阈值
        l1_config = self.config.get('l1_threshold', self.DEFAULT_CONFIG['l1_threshold'])
        l2_config = self.config.get('l2_threshold', self.DEFAULT_CONFIG['l2_threshold'])
        dg_config = self.config.get('downgrade_threshold', self.DEFAULT_CONFIG['downgrade_threshold'])
        
        self.UPGRADE_THRESHOLD_L1 = l1_config['score']
        self.UPGRADE_THRESHOLD_L2 = l2_config['score']
        self.UPGRADE_DAYS = l1_config['consecutive_days']
        self.UPGRADE_CONSECUTIVE_DAYS = l2_config['consecutive_days']
        self.UPGRADE_MAX_STD = l2_config['max_std']
        self.UPGRADE_MIN_TREND_SLOPE = l2_config['min_trend_slope']
        self.DOWNGRADE_DAYS = dg_config['dormant_days']
        self.SILENT_DAYS = dg_config['silent_days']
        self.REMOVE_DAYS = dg_config['remove_days']
        
        # 初始化目录和文件
        self.catalog_file = self.memory_dir / "indicator_catalog.json"
        self._init_catalog()
    
    def _load_config(self) -> Dict:
        """加载配置文件"""
        config_path = Path("config/evolution_config.yaml")
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f)
            except Exception as e:
                print(f"  ⚠️ 加载配置失败，使用默认配置: {e}")
        return self.DEFAULT_CONFIG
    
    def _init_catalog(self):
        """初始化指标档案库"""
        # 指标档案库路径
        self.catalog_file = self.memory_dir / "indicator_catalog.json"
        
        # 加载或初始化档案库
        self.catalog = self._load_catalog()
    
    def _load_catalog(self) -> Dict:
        """加载指标档案库"""
        if self.catalog_file.exists():
            try:
                with open(self.catalog_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:
                        # 文件为空，返回默认结构
                        return self._create_default_catalog()
                    catalog = json.loads(content)
                # 确保新字段存在（向后兼容）
                if "metadata" not in catalog:
                    catalog["metadata"] = {}
                return catalog
            except json.JSONDecodeError as e:
                print(f"  ⚠️ 指标档案损坏: {e}，将重新初始化")
                return self._create_default_catalog()
        
        return self._create_default_catalog()
    
    def _create_default_catalog(self) -> Dict:
        """创建默认档案库结构"""
        return {
            "device_sn": self.device_sn,
            "created_at": datetime.now().isoformat(),
            "indicators": {},
            "metadata": {},
            "silent_pool": {},
            "removed_pool": {},
            "composite_suggestions": []
        }
    
    def _save_catalog(self):
        """保存指标档案库（带异常处理和临时文件）"""
        import tempfile
        import os
        
        self.catalog["updated_at"] = datetime.now().isoformat()
        
        # 使用临时文件写入，成功后重命名，避免文件损坏
        temp_file = self.catalog_file.with_suffix('.tmp')
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.catalog, f, ensure_ascii=False, indent=2)
            # 原子性重命名
            os.replace(temp_file, self.catalog_file)
        except Exception as e:
            print(f"  ⚠️ 保存指标档案失败: {e}")
            if temp_file.exists():
                temp_file.unlink()
    
    def register_indicator(
        self,
        indicator_code: str,
        indicator_name: str = "",
        indicator_unit: str = "",
        indicator_type: str = "other",
        level: str = "L0",
        is_sentinel: bool = False
    ):
        """
        注册新指标到候选池
        
        Args:
            indicator_code: 指标代码
            indicator_name: 指标中文名称
            indicator_unit: 单位
            indicator_type: 类型（power/current/voltage/frequency/temperature/status/other）
            level: 初始级别 (L0/L1/L2/L3)
            is_sentinel: 是否为哨兵指标
        """
        if indicator_code in self.catalog["indicators"]:
            return  # 已存在，不重复注册
        
        self.catalog["indicators"][indicator_code] = {
            "code": indicator_code,
            "level": level,
            "is_sentinel": is_sentinel,
            "first_seen": datetime.now().isoformat(),
            "last_seen": datetime.now().isoformat(),
            "last_evaluated": None,
            "evaluation_history": [],
            "consecutive_active_days": 0,
            "consecutive_dormant_days": 0,
            "status": "new",
            "metadata": {
                "unit": indicator_unit,
                "dtype": "float"
            }
        }
        
        # 同时更新元数据映射表（第一阶段新增）
        self.catalog["metadata"][indicator_code] = {
            "name": indicator_name or indicator_code,
            "unit": indicator_unit,
            "type": indicator_type
        }
        
        self._save_catalog()
        print(f"  [进化管理] 新指标注册: {indicator_code} ({indicator_name}) -> {level}")
    
    def update_indicator_metadata(self, metadata_dict: Dict[str, Dict]):
        """
        批量更新指标元数据（第一阶段新增）
        
        Args:
            metadata_dict: {code: {name, unit, type}}
        """
        self.catalog["metadata"].update(metadata_dict)
        self._save_catalog()
        print(f"  [进化管理] 更新 {len(metadata_dict)} 个指标元数据")
    
    def get_indicator_metadata(self, indicator_code: str) -> Dict:
        """获取指标元数据（用于 LLM 交互时查表替换）"""
        return self.catalog["metadata"].get(indicator_code, {
            "name": indicator_code,
            "unit": "",
            "type": "other"
        })
    
    def get_all_metadata(self) -> Dict[str, Dict]:
        """获取所有指标元数据"""
        return self.catalog.get("metadata", {})
    
    def evaluate_and_evolve(
        self,
        indicator_code: str,
        evaluation_result: Dict,
        date_str: str
    ):
        """
        评价指标并执行进化（升级/降级/淘汰）
        
        Args:
            indicator_code: 指标代码
            evaluation_result: IndicatorEvaluator 的评价结果
            date_str: 日期字符串
        """
        if indicator_code not in self.catalog["indicators"]:
            # 自动注册新指标
            self.register_indicator(
                indicator_code=indicator_code,
                indicator_name=evaluation_result.get("name", indicator_code),
                indicator_unit=evaluation_result.get("unit", ""),
                indicator_type=evaluation_result.get("indicator_type", "other"),
                is_sentinel=evaluation_result.get("is_sentinel", False)
            )
        
        indicator = self.catalog["indicators"][indicator_code]
        
        # 更新最后观察时间
        indicator["last_seen"] = datetime.now().isoformat()
        indicator["last_evaluated"] = date_str
        
        # 记录评分历史
        score = evaluation_result.get("score", 0)
        indicator["evaluation_history"].append({
            "date": date_str,
            "score": score,
            "status": evaluation_result.get("status", "unknown"),
            "primary_score": evaluation_result.get("primary_score", 0),
            "secondary_score": evaluation_result.get("secondary_score", 0),
            "evaluation_logic": evaluation_result.get("evaluation_logic", "")
        })
        
        # 限制历史记录长度（保留最近 30 天）
        if len(indicator["evaluation_history"]) > 30:
            indicator["evaluation_history"] = indicator["evaluation_history"][-30:]
        
        # 更新连续天数计数（使用新阈值）
        if score >= self.UPGRADE_THRESHOLD_L1:
            indicator["consecutive_active_days"] += 1
            indicator["consecutive_dormant_days"] = 0
        elif score == 0:
            indicator["consecutive_dormant_days"] += 1
            indicator["consecutive_active_days"] = 0
        else:
            # 中间状态，不重置计数
            pass
        
        # 执行进化逻辑（使用新阈值）
        self._apply_evolution_rules(indicator, evaluation_result)
        
        self._save_catalog()
    
    def _apply_evolution_rules(self, indicator: Dict, evaluation_result: Dict):
        """应用进化规则（第一阶段调整阈值）"""
        code = indicator["code"]
        current_level = indicator["level"]
        is_sentinel = indicator.get("is_sentinel", False)
        score = evaluation_result.get("score", 0)
        
        # 哨兵指标永不淘汰，但可以升级
        if is_sentinel:
            if indicator["consecutive_active_days"] >= self.UPGRADE_DAYS and current_level == "L0":
                indicator["level"] = "L1"
                print(f"  [进化] {code}: L0 -> L1 (哨兵指标升级)")
            return
        
        # 升级逻辑（使用新阈值）
        if indicator["consecutive_active_days"] >= self.UPGRADE_DAYS:
            if current_level == "L0" and score >= self.UPGRADE_THRESHOLD_L1:
                indicator["level"] = "L1"
                indicator["status"] = "active"
                print(f"  [进化] {code}: L0 -> L1 (连续{self.UPGRADE_DAYS}天评分>={self.UPGRADE_THRESHOLD_L1})")
        
        # L1->L2升级：新标准（更严格）
        if current_level == "L1":
            history = indicator.get("evaluation_history", [])
            recent_7_days = [h.get("score", 0) for h in history[-7:]]
            
            # 新标准1: 连续3天评分>=0.85
            consecutive_high = 0
            max_consecutive = 0
            for s in recent_7_days:
                if s >= self.UPGRADE_THRESHOLD_L2:
                    consecutive_high += 1
                    max_consecutive = max(max_consecutive, consecutive_high)
                else:
                    consecutive_high = 0
            
            if max_consecutive < self.UPGRADE_CONSECUTIVE_DAYS:
                return  # 不满足连续要求
            
            # 新标准2: 评分标准差<0.1（稳定性）
            if len(recent_7_days) >= 3:
                import numpy as np
                score_std = np.std(recent_7_days)
                if score_std > self.UPGRADE_MAX_STD:
                    return  # 波动太大，不稳定
            
            # 新标准3: 趋势不能剧烈下降
            if len(recent_7_days) >= 3:
                # 简单线性回归计算斜率
                x = list(range(len(recent_7_days)))
                y = recent_7_days
                n = len(x)
                slope = (n * sum(x[i]*y[i] for i in range(n)) - sum(x)*sum(y)) / (n*sum(x[i]**2 for i in range(n)) - sum(x)**2) if (n*sum(x[i]**2 for i in range(n)) - sum(x)**2) != 0 else 0
                if slope < self.UPGRADE_MIN_TREND_SLOPE:
                    return  # 趋势下降太快
            
            # 全部通过，升级到L2
            indicator["level"] = "L2"
            print(f"  [进化] {code}: L1 -> L2 (连续{max_consecutive}天评分>={self.UPGRADE_THRESHOLD_L2}, 标准差<{self.UPGRADE_MAX_STD:.2f})")
        
        # 降级/静默逻辑
        if indicator["consecutive_dormant_days"] >= self.DOWNGRADE_DAYS:
            if current_level in ["L1", "L2"]:
                # 降级到 L0
                indicator["level"] = "L0"
                indicator["status"] = "dormant"
                print(f"  [退化] {code}: {current_level} -> L0 (连续{self.DOWNGRADE_DAYS}天无数据)")
        
        # 进入静默池
        if indicator["consecutive_dormant_days"] >= self.SILENT_DAYS:
            if code not in self.catalog["silent_pool"]:
                self.catalog["silent_pool"][code] = indicator.copy()
                indicator["status"] = "silent"
                print(f"  [静默] {code} 进入静默池")
        
        # 彻底移除（30天无数据且非哨兵）
        if indicator["consecutive_dormant_days"] >= self.REMOVE_DAYS:
            if not is_sentinel:
                self.catalog["removed_pool"][code] = indicator.copy()
                del self.catalog["indicators"][code]
                print(f"  [淘汰] {code} 已移除 (连续{self.REMOVE_DAYS}天无数据)")
    
    def get_indicators_by_level(self, level: str) -> List[str]:
        """获取指定级别的所有指标代码"""
        return [
            code for code, info in self.catalog["indicators"].items()
            if info["level"] == level
        ]
    
    def get_analysis_targets(self) -> Dict[str, List[str]]:
        """
        获取各级别的分析目标
        
        Returns:
            {
                "deep_analysis": [...],  # L2 核心指标（深度分析）
                "trend_tracking": [...],  # L1 活跃指标（趋势跟踪）
                "basic_stats": [...],     # L0 候选指标（基础统计）
                "silent": [...]           # 静默池指标
            }
        """
        return {
            "deep_analysis": self.get_indicators_by_level("L2"),
            "trend_tracking": self.get_indicators_by_level("L1"),
            "basic_stats": self.get_indicators_by_level("L0"),
            "silent": list(self.catalog["silent_pool"].keys())
        }
    
    def add_composite_suggestion(self, suggestion: str, related_indicators: List[str]):
        """
        添加复合指标建议（L3）
        
        Args:
            suggestion: LLM 的建议文本
            related_indicators: 相关指标代码列表
        """
        self.catalog["composite_suggestions"].append({
            "suggestion": suggestion,
            "related_indicators": related_indicators,
            "created_at": datetime.now().isoformat(),
            "status": "pending"  # pending/approved/rejected
        })
        self._save_catalog()
        print(f"  [L3建议] 记录复合指标建议: {suggestion[:50]}...")
    
    def get_catalog_summary(self) -> Dict:
        """获取档案库摘要"""
        indicators = self.catalog["indicators"]
        
        return {
            "total": len(indicators),
            "L0_candidates": len([i for i in indicators.values() if i["level"] == "L0"]),
            "L1_active": len([i for i in indicators.values() if i["level"] == "L1"]),
            "L2_core": len([i for i in indicators.values() if i["level"] == "L2"]),
            "L3_synthesized": len(self.catalog.get("composite_suggestions", [])),
            "silent": len(self.catalog.get("silent_pool", {})),
            "removed": len(self.catalog.get("removed_pool", {})),
            "sentinels": len([i for i in indicators.values() if i.get("is_sentinel")])
        }
    
    def print_evolution_report(self):
        """打印进化报告"""
        summary = self.get_catalog_summary()
        
        print(f"\n{'='*60}")
        print("指标体系进化报告 (第一阶段改造版)")
        print(f"{'='*60}")
        print(f"设备: {self.device_sn}")
        print(f"更新时间: {self.catalog.get('updated_at', 'N/A')}")
        print(f"\n指标分布:")
        print(f"  L0 候选池: {summary['L0_candidates']} 个")
        print(f"  L1 活跃指标: {summary['L1_active']} 个")
        print(f"  L2 核心指标: {summary['L2_core']} 个")
        print(f"  L3 复合建议: {summary['L3_synthesized']} 条")
        print(f"  静默池: {summary['silent']} 个")
        print(f"  已移除: {summary['removed']} 个")
        print(f"  哨兵指标: {summary['sentinels']} 个")
        
        # 显示核心指标
        core_indicators = self.get_indicators_by_level("L2")
        if core_indicators:
            print(f"\n核心指标 (L2):")
            for code in core_indicators[:10]:
                info = self.catalog["indicators"][code]
                meta = self.get_indicator_metadata(code)
                print(f"  - {code} ({meta['name']}) - 评分: {info['evaluation_history'][-1]['score']:.3f}" if info['evaluation_history'] else f"  - {code} ({meta['name']})")
            if len(core_indicators) > 10:
                print(f"  ... 还有 {len(core_indicators) - 10} 个")
        
        # 显示活跃指标
        active_indicators = self.get_indicators_by_level("L1")
        if active_indicators:
            print(f"\n活跃指标 (L1):")
            for code in active_indicators[:10]:
                info = self.catalog["indicators"][code]
                meta = self.get_indicator_metadata(code)
                print(f"  - {code} ({meta['name']})" if meta else f"  - {code}")
            if len(active_indicators) > 10:
                print(f"  ... 还有 {len(active_indicators) - 10} 个")
        
        # 显示复合建议
        composite = self.catalog.get("composite_suggestions", [])
        if composite:
            print(f"\n复合指标建议 (L3):")
            for sugg in composite[-5:]:  # 显示最近5条
                print(f"  - {sugg['suggestion'][:60]}...")
            if len(composite) > 5:
                print(f"  ... 还有 {len(composite) - 5} 条")
        
        print(f"{'='*60}\n")
    
    def run_llm_correlation_analysis(self) -> bool:
        """
        运行LLM关联分析（L1层多指标关联）
        
        Returns:
            是否成功
        """
        try:
            from core.llm_correlation import LLMCorrelationAnalyzer
            
            analyzer = LLMCorrelationAnalyzer(self.device_sn)
            return analyzer.run_analysis()
        except Exception as e:
            print(f"[LLM关联分析] 错误: {e}")
            return False
    
    def run_claw_agent_correlation_analysis(self) -> bool:
        """
        运行Claw Agent关联分析（利用Kimi Claw自身能力）
        
        Returns:
            是否成功
        """
        try:
            from core.claw_agent_correlation import ClawAgentCorrelationAnalyzer
            
            analyzer = ClawAgentCorrelationAnalyzer(self.device_sn)
            return analyzer.run_analysis()
        except Exception as e:
            print(f"[Claw Agent关联分析] 错误: {e}")
            return False
            
            analyzer = LLMCorrelationAnalyzer(self.device_sn)
            return analyzer.run_analysis()
        except Exception as e:
            print(f"[LLM关联分析] 错误: {e}")
            return False
