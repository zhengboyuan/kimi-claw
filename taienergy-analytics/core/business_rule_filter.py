"""
V4.5.0 业务规则过滤器
防止假阳性指标，确保候选指标有业务价值
"""

import re
import json
import os
from typing import List, Dict, Set
from dataclasses import dataclass
import numpy as np


@dataclass
class BusinessRuleConfig:
    """业务规则配置"""
    # 名称规范
    valid_patterns: List[str] = None
    
    # 依赖要求
    required_deps: List[str] = None  # 必须存在的指标
    optional_deps: List[str] = None  # 可选指标
    max_deps: int = 5  # 最大依赖数
    
    # 值范围
    value_range: List[float] = None  # [min, max]
    
    # 黑名单
    blacklisted_combinations: List[List[str]] = None
    
    def __post_init__(self):
        if self.valid_patterns is None:
            self.valid_patterns = [
                r".*_rate$",      # 比率类
                r".*_ratio$",     # 比例类
                r".*_index$",     # 指数类
                r".*_score$",     # 分数类
                r".*_std$",       # 标准差类
                r".*_cv$",        # 变异系数类
                r".*_slope$",     # 斜率类
                r".*_efficiency$", # 效率类
            ]
        if self.required_deps is None:
            self.required_deps = ["ai56", "ai62", "ai68"]  # 功率相关
        if self.optional_deps is None:
            self.optional_deps = ["ai51", "ai52", "ai53", "ai54", "ai55"]
        if self.value_range is None:
            self.value_range = [0, 100]  # 默认归一化到0-100
        if self.blacklisted_combinations is None:
            self.blacklisted_combinations = []


class BusinessRuleFilter:
    """业务规则过滤器"""
    
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.valid_ai_codes = self._load_valid_ai_codes()
        self.core_indicators = self._load_core_indicators()
        
    def _load_config(self, config_path: str = None) -> BusinessRuleConfig:
        """加载配置"""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                data = json.load(f)
            return BusinessRuleConfig(**data)
        return BusinessRuleConfig()  # 默认配置
    
    def _load_valid_ai_codes(self) -> Set[str]:
        """加载有效的AI指标代码"""
        # 从设备配置中加载
        try:
            from config.device_config import DeviceConfig
            config = DeviceConfig()
            return set(config.get_all_metric_codes())
        except:
            # 默认常用指标
            return {
                'ai51', 'ai52', 'ai53',  # 三相电压
                'ai54', 'ai55', 'ai56',  # 三相电流
                'ai62', 'ai63', 'ai64',  # 功率相关
                'ai68', 'ai69', 'ai70',  # 有功功率
                'ai99',  # 温度（如果有）
            }
    
    def _load_core_indicators(self) -> Dict[str, List[float]]:
        """加载现有核心指标的历史数据（用于冗余检查）"""
        core_indicators = {}
        
        # 从健康历史加载
        memory_path = "memory/devices"
        if os.path.exists(memory_path):
            for device_dir in os.listdir(memory_path):
                health_path = os.path.join(memory_path, device_dir, "health_history.json")
                if os.path.exists(health_path):
                    with open(health_path, 'r') as f:
                        history = json.load(f)
                    # 提取维度分数作为参考
                    for record in history:
                        for dim, score in record.get("dimensions", {}).items():
                            if dim not in core_indicators:
                                core_indicators[dim] = []
                            core_indicators[dim].append(score)
        
        return core_indicators
    
    def check(self, candidate) -> bool:
        """
        全面检查候选指标是否符合业务规则
        
        Returns:
            True: 通过所有检查
            False: 任一检查失败
        """
        checks = [
            ("名称规范", self._check_name_pattern(candidate.name)),
            ("依赖有效性", self._check_dependencies_valid(candidate.dependencies)),
            ("依赖数量", self._check_deps_count(candidate.dependencies)),
            ("黑名单", self._check_not_blacklisted(candidate.dependencies)),
            ("冗余度", self._check_not_redundant(candidate)),
        ]
        
        for name, result in checks:
            if not result:
                print(f"    ❌ 业务规则失败: {name}")
                return False
        
        return True
    
    def _check_name_pattern(self, name: str) -> bool:
        """检查名称是否符合规范（*_rate, *_index等）"""
        for pattern in self.config.valid_patterns:
            if re.match(pattern, name):
                return True
        return False
    
    def _check_dependencies_valid(self, dependencies: List[str]) -> bool:
        """检查依赖的指标是否存在"""
        for dep in dependencies:
            if dep not in self.valid_ai_codes:
                return False
        return True
    
    def _check_deps_count(self, dependencies: List[str]) -> bool:
        """检查依赖数量是否合理"""
        return len(dependencies) <= self.config.max_deps
    
    def _check_not_blacklisted(self, dependencies: List[str]) -> bool:
        """检查是否在黑名单组合中"""
        dep_set = set(dependencies)
        for blacklisted in self.config.blacklisted_combinations:
            if dep_set == set(blacklisted):
                return False
        return True
    
    def _check_not_redundant(self, candidate) -> bool:
        """检查是否与现有核心指标高度冗余"""
        # 简化版：检查名称相似度
        candidate_base = candidate.name.split('_')[0]
        
        for core_name in self.core_indicators.keys():
            # 如果名称高度相似，可能冗余
            if candidate_base in core_name or core_name in candidate_base:
                # 计算相关性（如果有历史数据）
                # 这里简化处理，实际应该计算指标值的相关性
                return False
        
        return True
    
    def get_check_details(self, candidate) -> Dict[str, bool]:
        """获取详细检查结果"""
        return {
            "名称规范": self._check_name_pattern(candidate.name),
            "依赖有效性": self._check_dependencies_valid(candidate.dependencies),
            "依赖数量": self._check_deps_count(candidate.dependencies),
            "黑名单": self._check_not_blacklisted(candidate.dependencies),
            "冗余度": self._check_not_redundant(candidate),
        }
    
    def explain_failure(self, candidate) -> str:
        """解释为什么失败"""
        details = self.get_check_details(candidate)
        failed = [k for k, v in details.items() if not v]
        return f"指标 {candidate.name} 未通过: {', '.join(failed)}"


# 便捷函数
def filter_candidates(candidates: List, config_path: str = None) -> List:
    """
    批量过滤候选指标
    
    Args:
        candidates: 候选指标列表
        config_path: 业务规则配置文件路径
    
    Returns:
        通过过滤的候选指标列表
    """
    filter_obj = BusinessRuleFilter(config_path)
    valid = []
    
    for c in candidates:
        if filter_obj.check(c):
            valid.append(c)
        else:
            print(f"  过滤掉: {filter_obj.explain_failure(c)}")
    
    return valid


if __name__ == "__main__":
    # 测试
    from daily_discovery import CandidateIndicator
    
    test_candidates = [
        CandidateIndicator(
            name="ai56_rolling_std",
            discovery_method="stat_features",
            formula="std",
            dependencies=["ai56"],
            pseudo_code="def calc(): pass",
            info_gain=0.05,
            missing_rate=0.0,
            cv=0.1,
            timestamp="2025-08-15"
        ),
        CandidateIndicator(
            name="invalid_metric",  # 不符合命名规范
            discovery_method="stat_features",
            formula="std",
            dependencies=["ai999"],  # 不存在的指标
            pseudo_code="def calc(): pass",
            info_gain=0.05,
            missing_rate=0.0,
            cv=0.1,
            timestamp="2025-08-15"
        ),
    ]
    
    valid = filter_candidates(test_candidates)
    print(f"\n通过过滤: {len(valid)}/{len(test_candidates)}")
