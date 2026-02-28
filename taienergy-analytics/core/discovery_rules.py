"""
V5.1 发现规则引擎
可配置的阈值规则，用于设备退化、异常检测、排名变化等发现
"""
import json
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, asdict
from enum import Enum


class RuleSeverity(Enum):
    """规则严重程度"""
    INFO = "info"           # 提示
    WARNING = "warning"     # 警告
    CRITICAL = "critical"   # 严重


class RuleType(Enum):
    """规则类型"""
    TREND = "trend"                 # 趋势变化
    THRESHOLD = "threshold"         # 阈值突破
    RANKING = "ranking"             # 排名变化
    VARIANCE = "variance"           # 方差异常
    CORRELATION = "correlation"     # 相关性异常


@dataclass
class DiscoveryRule:
    """发现规则定义"""
    id: str                         # 规则ID
    name: str                       # 规则名称
    description: str                # 规则描述
    type: RuleType                  # 规则类型
    severity: RuleSeverity          # 严重程度
    
    # 规则配置
    metric: str                     # 监控的指标
    condition: str                  # 条件（>, <, between, change_rate, etc.）
    threshold: Any                  # 阈值
    
    # 时间窗口
    lookback_days: int = 7          # 回看天数
    min_data_points: int = 3        # 最少数据点数
    
    # 抑制配置
    cooldown_days: int = 1          # 触发后冷却天数
    
    # 元数据
    enabled: bool = True            # 是否启用
    version: str = "1.0"            # 规则版本


class DiscoveryRuleEngine:
    """发现规则引擎"""
    
    # 默认规则集
    DEFAULT_RULES = [
        # ========== 健康分相关 ==========
        DiscoveryRule(
            id="health_decline_7d",
            name="健康分7日下滑",
            description="设备健康分在7天内下降超过10分",
            type=RuleType.TREND,
            severity=RuleSeverity.WARNING,
            metric="health_score",
            condition="change_rate",
            threshold=-10,
            lookback_days=7,
            min_data_points=3
        ),
        DiscoveryRule(
            id="health_critical",
            name="健康分临界",
            description="设备健康分低于60分（注意级别）",
            type=RuleType.THRESHOLD,
            severity=RuleSeverity.WARNING,
            metric="health_score",
            condition="<",
            threshold=60,
            lookback_days=1
        ),
        DiscoveryRule(
            id="health_danger",
            name="健康分危险",
            description="设备健康分低于40分（危险级别）",
            type=RuleType.THRESHOLD,
            severity=RuleSeverity.CRITICAL,
            metric="health_score",
            condition="<",
            threshold=40,
            lookback_days=1
        ),
        
        # ========== 发电相关 ==========
        DiscoveryRule(
            id="generation_drop_30d",
            name="发电量月度下降",
            description="设备发电量较上月同期下降超过20%",
            type=RuleType.TREND,
            severity=RuleSeverity.WARNING,
            metric="daily_generation",
            condition="change_rate",
            threshold=-20,
            lookback_days=30,
            min_data_points=7
        ),
        DiscoveryRule(
            id="generation_variance_high",
            name="发电量波动异常",
            description="设备发电量变异系数超过0.3（波动过大）",
            type=RuleType.VARIANCE,
            severity=RuleSeverity.INFO,
            metric="daily_generation",
            condition="cv>",
            threshold=0.3,
            lookback_days=14,
            min_data_points=7
        ),
        
        # ========== 排名相关 ==========
        DiscoveryRule(
            id="ranking_drop",
            name="排名大幅下滑",
            description="设备健康分排名下降超过5位",
            type=RuleType.RANKING,
            severity=RuleSeverity.WARNING,
            metric="health_ranking",
            condition="change",
            threshold=5,
            lookback_days=7,
            min_data_points=3
        ),
        DiscoveryRule(
            id="consistent_bottom3",
            name="持续垫底",
            description="设备连续7天排名倒数前3",
            type=RuleType.RANKING,
            severity=RuleSeverity.WARNING,
            metric="health_ranking",
            condition="consecutive_bottom",
            threshold=3,
            lookback_days=7,
            min_data_points=5
        ),
        
        # ========== 功率相关 ==========
        DiscoveryRule(
            id="power_gap_anomaly",
            name="功率差异异常",
            description="设备功率与场站平均值差异超过20%",
            type=RuleType.VARIANCE,
            severity=RuleSeverity.WARNING,
            metric="avg_power",
            condition="deviation>",
            threshold=20,
            lookback_days=1
        ),
        DiscoveryRule(
            id="power_stability_low",
            name="功率稳定性差",
            description="设备功率变异系数超过0.2",
            type=RuleType.VARIANCE,
            severity=RuleSeverity.INFO,
            metric="power_cv",
            condition=">",
            threshold=0.2,
            lookback_days=7,
            min_data_points=3
        ),
        
        # ========== 竞赛指标相关 ==========
        DiscoveryRule(
            id="utilization_hours_low",
            name="利用小时数偏低",
            description="等效利用小时数低于3小时（晴天）",
            type=RuleType.THRESHOLD,
            severity=RuleSeverity.WARNING,
            metric="equivalent_utilization_hours",
            condition="<",
            threshold=3,
            lookback_days=1
        ),
        DiscoveryRule(
            id="generation_duration_short",
            name="发电时长不足",
            description="发电时长低于10小时",
            type=RuleType.THRESHOLD,
            severity=RuleSeverity.INFO,
            metric="generation_duration",
            condition="<",
            threshold=10,
            lookback_days=1
        )
    ]
    
    def __init__(self, custom_rules: List[DiscoveryRule] = None):
        """
        Args:
            custom_rules: 自定义规则，如不传使用默认规则
        """
        self.rules = custom_rules or self.DEFAULT_RULES
        self.rules_by_id = {r.id: r for r in self.rules}
    
    def get_rule(self, rule_id: str) -> Optional[DiscoveryRule]:
        """获取规则定义"""
        return self.rules_by_id.get(rule_id)
    
    def list_rules(self, enabled_only: bool = True, severity: RuleSeverity = None) -> List[DiscoveryRule]:
        """列出规则"""
        result = self.rules
        if enabled_only:
            result = [r for r in result if r.enabled]
        if severity:
            result = [r for r in result if r.severity == severity]
        return result
    
    def add_rule(self, rule: DiscoveryRule) -> bool:
        """添加自定义规则"""
        if rule.id in self.rules_by_id:
            return False
        self.rules.append(rule)
        self.rules_by_id[rule.id] = rule
        return True
    
    def update_rule(self, rule_id: str, updates: Dict) -> bool:
        """更新规则配置"""
        if rule_id not in self.rules_by_id:
            return False
        rule = self.rules_by_id[rule_id]
        for key, value in updates.items():
            if hasattr(rule, key):
                setattr(rule, key, value)
        return True
    
    def disable_rule(self, rule_id: str) -> bool:
        """禁用规则"""
        return self.update_rule(rule_id, {"enabled": False})
    
    def enable_rule(self, rule_id: str) -> bool:
        """启用规则"""
        return self.update_rule(rule_id, {"enabled": True})
    
    def export_rules(self) -> List[Dict]:
        """导出所有规则为字典列表"""
        return [
            {
                **asdict(rule),
                "type": rule.type.value,
                "severity": rule.severity.value
            }
            for rule in self.rules
        ]
    
    def import_rules(self, rules_data: List[Dict]) -> int:
        """导入规则"""
        count = 0
        for data in rules_data:
            try:
                rule = DiscoveryRule(
                    id=data["id"],
                    name=data["name"],
                    description=data["description"],
                    type=RuleType(data["type"]),
                    severity=RuleSeverity(data["severity"]),
                    metric=data["metric"],
                    condition=data["condition"],
                    threshold=data["threshold"],
                    lookback_days=data.get("lookback_days", 7),
                    min_data_points=data.get("min_data_points", 3),
                    cooldown_days=data.get("cooldown_days", 1),
                    enabled=data.get("enabled", True),
                    version=data.get("version", "1.0")
                )
                if self.add_rule(rule):
                    count += 1
            except Exception as e:
                print(f"导入规则失败: {e}")
        return count


# 便捷函数
def get_rule_engine() -> DiscoveryRuleEngine:
    """获取默认规则引擎"""
    return DiscoveryRuleEngine()