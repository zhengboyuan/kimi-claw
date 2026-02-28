"""
V4.5.0 指标自动发现模块 - 方案B实现
分层评估：48 → 30 → 20 → 5 → 3
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import json
import os

from core.memory_system import MemorySystem
from core.indicator_assessor import IndicatorAssessor


@dataclass
class CandidateIndicator:
    """候选指标定义"""
    name: str
    discovery_method: str
    formula: str
    dependencies: List[str]
    pseudo_code: str
    info_gain: float
    missing_rate: float
    cv: float
    timestamp: str
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "discovery_method": self.discovery_method,
            "formula": self.formula,
            "dependencies": self.dependencies,
            "pseudo_code": self.pseudo_code,
            "info_gain": self.info_gain,
            "missing_rate": self.missing_rate,
            "cv": self.cv,
            "timestamp": self.timestamp
        }


class IndicatorDiscovery:
    """指标发现器 - 3类特征挖掘 + 分层评估"""
    
    def __init__(self, device_sns: List[str] = None, llm_client=None):
        self.device_sns = device_sns or []
        self.candidates: List[CandidateIndicator] = []
        self.memory = MemorySystem()
        self.assessor = IndicatorAssessor(llm_client)
        
    def scan_daily(self, date_str: str, device_data: Dict) -> List[CandidateIndicator]:
        """
        每日扫描，分层评估发现候选指标
        
        流程: 48 -> 30 -> 20 -> 5 -> 3
        """
        print(f"[{date_str}] 开始指标发现扫描...")
        self.candidates = []
        
        # 提取raw_metrics
        metrics_data = {}
        for device_sn, data in device_data.items():
            if isinstance(data, dict):
                if 'raw_metrics' in data:
                    metrics_data[device_sn] = data['raw_metrics']
                else:
                    # 直接是 {metric_code: [values]} 结构
                    metrics_data[device_sn] = data
        
        if not metrics_data:
            print(f"  警告: 没有找到raw_metrics数据")
            return []
        
        # 1. 特征挖掘
        stat_candidates = self._stat_features(metrics_data, date_str)
        self.candidates.extend(stat_candidates)
        print(f"  统计特征: {len(stat_candidates)} 个")
        
        trend_candidates = self._trend_features(metrics_data, date_str)
        self.candidates.extend(trend_candidates)
        print(f"  趋势特征: {len(trend_candidates)} 个")
        
        # 交叉特征需要原始device_data结构
        cross_candidates = self._cross_features(device_data, date_str)
        self.candidates.extend(cross_candidates)
        print(f"  交叉特征: {len(cross_candidates)} 个")
        
        total = len(self.candidates)
        print(f"  总计: {total} 个候选")
        
        # 2. 分层评估
        final_candidates = self._evaluate_layers(self.candidates)
        print(f"  最终通过: {len(final_candidates)} 个")
        
        return final_candidates
    
    def _evaluate_layers(self, candidates: List[CandidateIndicator]) -> List[CandidateIndicator]:
        """
        分层评估: 48 -> 30 -> 20 -> 5 -> 3
        """
        if not candidates:
            return []
        
        # Layer 1: 规则过滤
        rule_passed = self._layer1_rule_filter(candidates)
        print(f"  Layer 1 (规则): {len(candidates)} -> {len(rule_passed)}")
        
        # Layer 2: 查重
        unique = self._layer2_deduplicate(rule_passed)
        print(f"  Layer 2 (查重): {len(rule_passed)} -> {len(unique)}")
        
        # Layer 3: LLM评估 (只评估前20个)
        assessed = self._layer3_llm_assess(unique[:20])
        top5 = sorted(assessed, key=lambda x: x['score'], reverse=True)[:5]
        print(f"  Layer 3 (LLM): {len(unique[:20])} -> {len(top5)}")
        
        # Layer 4: 智能决策
        final = self._layer4_smart_decide(top5)
        print(f"  Layer 4 (决策): {len(top5)} -> {len(final)}")
        
        return final
    
    def _layer1_rule_filter(self, candidates: List[CandidateIndicator]) -> List[CandidateIndicator]:
        """Layer 1: 规则过滤 - 大幅放宽条件"""
        passed = []
        
        print(f"    [DEBUG] Layer 1 输入: {len(candidates)} 个候选")
        
        for c in candidates:
            cv_threshold = 10.0 if c.discovery_method == "trend_features" else 5.0
            
            # 大幅放宽所有条件
            check1 = c.missing_rate < 0.95  # 允许95%缺失（原始数据质量差）
            check2 = c.cv < cv_threshold 
            check3 = c.info_gain > 0.0  # 只要有信息增益即可
            
            if check1 and check2 and check3:
                passed.append(c)
        
        print(f"    [DEBUG] Layer 1 通过: {len(passed)} 个")
        return passed
    
    def _layer2_deduplicate(self, candidates: List[CandidateIndicator]) -> List[CandidateIndicator]:
        """Layer 2: 查重 - 去重同名候选 + 与现有指标比较"""
        # 第一步：去重同名候选（保留第一个）
        seen_names = set()
        unique_by_name = []
        for c in candidates:
            if c.name not in seen_names:
                seen_names.add(c.name)
                unique_by_name.append(c)
        
        print(f"    [DEBUG] 去重前: {len(candidates)} 个, 去重后: {len(unique_by_name)} 个")
        
        # 第二步：与现有候选比较（检查文件是否存在）
        candidate_dir = "memory/indicators/candidate"
        existing_candidates = set()
        if os.path.exists(candidate_dir):
            for f in os.listdir(candidate_dir):
                if f.endswith('.md'):
                    # 提取指标名（去掉日期后缀）
                    name = '_'.join(f.replace('.md', '').split('_')[:-1])
                    existing_candidates.add(name)
        
        print(f"    [DEBUG] 现有候选: {existing_candidates}")
        
        # 过滤掉已存在的
        unique = []
        for c in unique_by_name:
            if c.name in existing_candidates:
                print(f"    [DEBUG] {c.name}: 已存在，跳过")
            else:
                unique.append(c)
        
        return unique
    
    def _layer3_llm_assess(self, candidates: List[CandidateIndicator]) -> List[Dict]:
        """Layer 3: LLM评估 - 质量评分"""
        # 转换为dict
        candidate_dicts = [c.to_dict() for c in candidates]
        
        # 读取现有指标作为上下文
        registry = self.memory.read_registry()
        existing = list(registry.get('indicators', {}).keys())
        
        context = {'existing_indicators': existing}
        
        # 批量评估
        results = self.assessor.assess_batch(candidate_dicts, context)
        
        # 转换为统一格式
        scored = []
        for r in results:
            scored.append({
                'candidate': next(c for c in candidates if c.name == r.name),
                'score': r.overall_score,
                'verdict': r.verdict,
                'reason': r.reason
            })
        
        return scored
    
    def _layer4_smart_decide(self, assessed: List[Dict]) -> List[CandidateIndicator]:
        """Layer 4: 智能决策 - 最终筛选"""
        final = []
        
        registry = self.memory.read_registry()
        relationships = self.memory.read_relationships()
        
        for item in assessed:
            candidate = item['candidate']
            score = item['score']
            verdict = item['verdict']
            
            print(f"    [DEBUG] {candidate.name}: score={score:.2f}, verdict={verdict}")
            
            # 放宽条件: 保留PROMOTE/EXTEND/REJECT但score>0.3的
            should_keep = False
            if verdict == "PROMOTE":
                should_keep = True
            elif verdict == "EXTEND" and score > 0.3:
                should_keep = True
            elif score > 0.4:
                should_keep = True
            
            if should_keep:
                # 再次查重
                decision = self.assessor.smart_decide(
                    candidate.to_dict(), registry, relationships
                )
                
                print(f"      -> decision: {decision}")
                
                if decision == "WRITE":
                    final.append(candidate)
                elif decision == "MERGE":
                    print(f"    {candidate.name}: 建议合并到现有指标")
        
        return final[:3]  # 最多3个
    
    def _stat_features(self, device_data: Dict, date_str: str) -> List[CandidateIndicator]:
        """统计特征：滑动均值/方差/偏度"""
        candidates = []
        
        # 关键指标代码
        key_metrics = ['ai51', 'ai52', 'ai53', 'ai54', 'ai55', 'ai56', 'ai68']
        
        for device_sn, metrics in device_data.items():
            # device_data 已经是 {device_sn: {metric_code: [values]}} 结构
            if not isinstance(metrics, dict):
                continue
            
            for metric_code in key_metrics:
                if metric_code not in metrics:
                    continue
                
                values = metrics[metric_code]
                if len(values) < 24:  # 至少需要24小时数据
                    continue
                
                # 清理数据
                clean_values = [v for v in values if v is not None and not (isinstance(v, float) and (np.isnan(v) or np.isinf(v)))]
                if len(clean_values) < 24:
                    continue
                
                # 计算统计特征
                mean_val = np.mean(clean_values)
                std_val = np.std(clean_values)
                
                if std_val > 0 and mean_val > 0:
                    cv = std_val / mean_val
                    
                    # 候选1: 滑动标准差（稳定性指标）
                    candidate = CandidateIndicator(
                        name=f"{metric_code}_rolling_std",
                        discovery_method="stat_features",
                        formula=f"rolling_std({metric_code}, window=24h)",
                        dependencies=[metric_code],
                        pseudo_code=f"""
def calculate_{metric_code}_rolling_std(data):
    return data['{metric_code}'].rolling(window=24).std()
                        """.strip(),
                        info_gain=cv,
                        missing_rate=1.0 - len(clean_values) / len(values),
                        cv=cv,
                        timestamp=date_str
                    )
                    candidates.append(candidate)
                    
                    # 候选2: 变异系数（波动率指标）
                    candidate = CandidateIndicator(
                        name=f"{metric_code}_cv",
                        discovery_method="stat_features",
                        formula=f"cv({metric_code}) = std / mean",
                        dependencies=[metric_code],
                        pseudo_code=f"""
def calculate_{metric_code}_cv(data):
    return data['{metric_code}'].std() / data['{metric_code}'].mean()
                        """.strip(),
                        info_gain=cv,
                        missing_rate=1.0 - len(clean_values) / len(values),
                        cv=cv,
                        timestamp=date_str
                    )
                    candidates.append(candidate)
        
        return candidates
    
    def _trend_features(self, device_data: Dict, date_str: str) -> List[CandidateIndicator]:
        """趋势特征：斜率/拐点/加速度"""
        candidates = []
        
        key_metrics = ['ai56', 'ai62', 'ai68']  # 电流、功率相关
        
        for device_sn, metrics in device_data.items():
            # device_data 已经是 {device_sn: {metric_code: [values]}} 结构
            if not isinstance(metrics, dict):
                continue
            
            for metric_code in key_metrics:
                if metric_code not in metrics:
                    continue
                
                values = metrics[metric_code]
                
                # 清理nan值
                clean_values = [v for v in values if v is not None and not (isinstance(v, float) and (np.isnan(v) or np.isinf(v)))]
                
                if len(clean_values) < 5:  # 至少5个有效数据
                    continue
                
                # 过滤全0值
                non_zero_values = [v for v in clean_values if v != 0]
                if len(non_zero_values) < 5:
                    continue
                
                # 计算趋势斜率
                x = np.arange(len(non_zero_values))
                try:
                    slope = np.polyfit(x, non_zero_values, 1)[0]
                except:
                    continue
                
                mean_val = np.mean(non_zero_values)
                std_val = np.std(non_zero_values)
                
                if mean_val <= 0:
                    continue
                
                cv = std_val / mean_val
                
                candidate = CandidateIndicator(
                    name=f"{metric_code}_trend_slope",
                    discovery_method="trend_features",
                    formula=f"linear_slope({metric_code})",
                    dependencies=[metric_code],
                    pseudo_code=f"""
def calculate_{metric_code}_trend_slope(data):
    x = np.arange(len(data))
    return np.polyfit(x, data['{metric_code}'], 1)[0]
                    """.strip(),
                    info_gain=abs(slope) / mean_val,
                    missing_rate=1.0 - len(clean_values) / len(values),
                    cv=cv,
                    timestamp=date_str
                )
                candidates.append(candidate)
        
        return candidates
    
    def _cross_features(self, device_data: Dict, date_str: str) -> List[CandidateIndicator]:
        """交叉特征：温度×功率/效率衰减率"""
        candidates = []
        
        # 有意义的交叉组合
        cross_pairs = [
            ('ai56', 'ai68', 'current_power_ratio'),  # 电流×功率
            ('ai51', 'ai68', 'voltage_power_efficiency'),  # 电压×功率效率
        ]
        
        for device_sn, data in device_data.items():
            # 获取raw_metrics
            metrics = data.get('raw_metrics', {}) if isinstance(data, dict) else {}
            
            for metric1, metric2, feature_name in cross_pairs:
                if metric1 not in metrics or metric2 not in metrics:
                    continue
                
                values1 = metrics[metric1]
                values2 = metrics[metric2]
                
                if len(values1) != len(values2) or len(values1) < 24:
                    continue
                
                # 清理数据
                clean_pairs = [(v1, v2) for v1, v2 in zip(values1, values2) 
                               if v1 is not None and v2 is not None
                               and not (isinstance(v1, float) and (np.isnan(v1) or np.isinf(v1)))
                               and not (isinstance(v2, float) and (np.isnan(v2) or np.isinf(v2)))]
                
                if len(clean_pairs) < 24:
                    continue
                
                clean_v1 = [p[0] for p in clean_pairs]
                clean_v2 = [p[1] for p in clean_pairs]
                
                # 计算相关系数
                try:
                    correlation = np.corrcoef(clean_v1, clean_v2)[0, 1]
                except:
                    continue
                
                if abs(correlation) > 0.3:  # 有一定相关性
                    candidate = CandidateIndicator(
                        name=feature_name,
                        discovery_method="cross_features",
                        formula=f"{metric1} * {metric2} / correlation",
                        dependencies=[metric1, metric2],
                        pseudo_code=f"""
def calculate_{feature_name}(data):
    return data['{metric1}'] * data['{metric2}'] / {abs(correlation):.2f}
                        """.strip(),
                        info_gain=abs(correlation),
                        missing_rate=1.0 - len(clean_pairs) / len(values1),
                        cv=np.std(clean_v1) / np.mean(clean_v1) if np.mean(clean_v1) > 0 else 0,
                        timestamp=date_str
                    )
                    candidates.append(candidate)
        
        return candidates
    
    def _quick_filter(self, candidates: List[CandidateIndicator]) -> List[CandidateIndicator]:
        """快速预筛 - 已废弃，使用 _evaluate_layers 替代"""
        # 保留此方法以兼容旧代码，实际使用分层评估
        return self._layer1_rule_filter(candidates)
    
    def generate_spec(self, candidate: CandidateIndicator) -> str:
        """生成Ralph spec文件内容"""
        
        spec_content = f"""# 指标规格: {candidate.name}

## 发现背景
- **发现日期**: {candidate.timestamp}
- **发现方式**: {candidate.discovery_method}
- **触发条件**: 数据扫描自动发现
- **信息增益**: {candidate.info_gain:.4f}
- **变异系数**: {candidate.cv:.4f}

## 计算定义
```python
{candidate.pseudo_code}
```

## 依赖指标
{candidate.dependencies}

## 验收标准（Ralph验证）
- [ ] 代码通过 flake8 检查
- [ ] 单元测试覆盖 3 种边界场景（空数据/异常值/缺失值）
- [ ] 用最近7天数据实测，输出格式正确
- [ ] 执行时间 < 500ms/设备/天

## 输出信号
<promise>DONE</promise> 当所有标准验证通过
"""
        return spec_content
    
    def save_to_candidate_pool(self, candidate: CandidateIndicator, base_path: str = "memory/indicators/candidate"):
        """保存spec到候选池"""
        
        os.makedirs(base_path, exist_ok=True)
        
        # 生成文件名
        filename = f"{candidate.name}_{candidate.timestamp}.md"
        filepath = os.path.join(base_path, filename)
        
        # 写入spec
        spec_content = self.generate_spec(candidate)
        with open(filepath, 'w') as f:
            f.write(spec_content)
        
        # 更新候选池索引
        self._update_candidate_index(candidate, base_path)
        
        print(f"  已保存: {filepath}")
        return filepath
    
    def _update_candidate_index(self, candidate: CandidateIndicator, base_path: str):
        """更新候选池索引文件"""
        
        index_path = os.path.join(base_path, "candidate_pool.json")
        
        # 读取现有索引
        if os.path.exists(index_path):
            with open(index_path, 'r') as f:
                index = json.load(f)
        else:
            index = {"candidates": [], "last_updated": ""}
        
        # 添加新候选
        index["candidates"].append({
            "name": candidate.name,
            "timestamp": candidate.timestamp,
            "method": candidate.discovery_method,
            "status": "pending",  # pending / processing / done / failed
            "spec_file": f"{candidate.name}_{candidate.timestamp}.md"
        })
        index["last_updated"] = datetime.now().isoformat()
        
        # 写回
        with open(index_path, 'w') as f:
            json.dump(index, f, indent=2)


# 便捷函数
def run_discovery(date_str: str, device_data: Dict, max_candidates: int = 5) -> List[CandidateIndicator]:
    """
    运行每日发现，返回Top-N候选
    
    Args:
        date_str: 日期
        device_data: 设备数据
        max_candidates: 每日最大候选数（默认5）
    
    Returns:
        候选指标列表
    """
    discovery = IndicatorDiscovery()
    candidates = discovery.scan_daily(date_str, device_data)
    
    # 按信息增益排序，取Top-N
    candidates.sort(key=lambda c: c.info_gain, reverse=True)
    top_candidates = candidates[:max_candidates]
    
    # 保存到候选池
    for c in top_candidates:
        discovery.save_to_candidate_pool(c)
    
    return top_candidates


if __name__ == "__main__":
    # 测试
    test_data = {
        "XHDL_1NBQ": {
            "ai56": [10.5, 10.8, 10.6, 10.7] * 6,  # 24小时数据
            "ai68": [1500, 1520, 1510, 1515] * 6,
        }
    }
    
    candidates = run_discovery("2025-08-15", test_data, max_candidates=3)
    print(f"\n发现 {len(candidates)} 个候选指标")
    for c in candidates:
        print(f"  - {c.name} ({c.discovery_method}, info_gain={c.info_gain:.4f})")
