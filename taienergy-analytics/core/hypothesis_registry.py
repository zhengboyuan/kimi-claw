"""
假设实验库管理器
第二阶段核心组件：管理假设的生成、验证和权重更新

数据结构：
- hypothesis_registry.json: 假设实验库
- cognitive_log.json: 每日认知增量记录
"""
import json
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path


class HypothesisRegistry:
    """
    假设实验库管理器
    
    职责：
    1. 管理假设的 CRUD
    2. 记录假设验证历史
    3. 计算认知增量
    4. 提供假设查询接口
    
    假设状态：
    - testing: 测试中
    - verified: 已验证（success_count >= 5）
    - failed: 已失败（连续 3 次验证失败）
    - deprecated: 已废弃（被新假设取代）
    """
    
    # 验证阈值
    VERIFIED_THRESHOLD = 5  # 成功 5 次升级为 verified
    FAILED_THRESHOLD = 3    # 连续 3 次失败标记为 failed
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.memory_dir = Path(memory_dir)
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        
        # 假设实验库路径
        self.registry_file = self.memory_dir / "hypothesis_registry.json"
        self.cognitive_log_file = self.memory_dir / "cognitive_log.json"
        
        # 加载或初始化
        self.registry = self._load_registry()
        self.cognitive_log = self._load_cognitive_log()
    
    def _load_registry(self) -> Dict:
        """加载假设实验库"""
        if self.registry_file.exists():
            with open(self.registry_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        return {
            "device_sn": self.device_sn,
            "created_at": datetime.now().isoformat(),
            "hypotheses": {},  # {hyp_id: hypothesis_data}
            "verified_rules": [],  # 已验证的规则（用于第三阶段固化）
            "statistics": {
                "total_generated": 0,
                "total_verified": 0,
                "total_failed": 0
            }
        }
    
    def _load_cognitive_log(self) -> List[Dict]:
        """加载认知增量日志"""
        if self.cognitive_log_file.exists():
            with open(self.cognitive_log_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    
    def _save_registry(self):
        """保存假设实验库"""
        self.registry["updated_at"] = datetime.now().isoformat()
        with open(self.registry_file, 'w', encoding='utf-8') as f:
            json.dump(self.registry, f, ensure_ascii=False, indent=2)
    
    def _save_cognitive_log(self):
        """保存认知增量日志"""
        with open(self.cognitive_log_file, 'w', encoding='utf-8') as f:
            json.dump(self.cognitive_log, f, ensure_ascii=False, indent=2)
    
    def create_hypothesis(
        self,
        logic: str,
        related_indicators: List[str],
        expected_range: Optional[Dict] = None,
        source: str = "llm_generation"
    ) -> str:
        """
        创建新假设
        
        Args:
            logic: 假设逻辑描述（如 "ai45 * 0.98 ≈ ai56"）
            related_indicators: 相关指标代码列表
            expected_range: 预期范围（如 {"min": 0.95, "max": 1.0}）
            source: 来源（llm_generation / user_input / auto_discovery）
        
        Returns:
            hyp_id: 假设唯一 ID
        """
        hyp_id = f"H{self.registry['statistics']['total_generated'] + 1:03d}"
        
        self.registry["hypotheses"][hyp_id] = {
            "id": hyp_id,
            "logic": logic,
            "related_indicators": related_indicators,
            "expected_range": expected_range or {},
            "status": "testing",
            "source": source,
            "created_at": datetime.now().isoformat(),
            "last_tested": None,
            "test_count": 0,
            "success_count": 0,
            "fail_count": 0,
            "consecutive_fails": 0,
            "weight": 0.5,  # 初始权重 0.5
            "test_history": [],  # 每次验证的详细记录
            "reflection": None  # 失败时的反思
        }
        
        self.registry["statistics"]["total_generated"] += 1
        self._save_registry()
        
        print(f"  [假设库] 新建假设 {hyp_id}: {logic[:50]}...")
        return hyp_id
    
    def verify_hypothesis(
        self,
        hyp_id: str,
        actual_value: float,
        expected_value: float,
        deviation: float,
        test_data: Optional[Dict] = None
    ) -> Dict:
        """
        验证假设
        
        Args:
            hyp_id: 假设 ID
            actual_value: 实际观测值
            expected_value: 预期值
            deviation: 偏差率（如 0.02 表示 2%）
            test_data: 测试数据详情
        
        Returns:
            验证结果
        """
        if hyp_id not in self.registry["hypotheses"]:
            return {"error": f"假设 {hyp_id} 不存在"}
        
        hyp = self.registry["hypotheses"][hyp_id]
        
        # 判断验证结果（偏差 < 5% 认为成功）
        is_success = deviation < 0.05
        
        # 更新计数
        hyp["test_count"] += 1
        hyp["last_tested"] = datetime.now().isoformat()
        
        if is_success:
            hyp["success_count"] += 1
            hyp["consecutive_fails"] = 0
            hyp["weight"] = min(1.0, hyp["weight"] + 0.1)  # 成功权重 +0.1
        else:
            hyp["fail_count"] += 1
            hyp["consecutive_fails"] += 1
            hyp["weight"] = max(0.0, hyp["weight"] - 0.15)  # 失败权重 -0.15
        
        # 记录测试历史
        hyp["test_history"].append({
            "timestamp": datetime.now().isoformat(),
            "actual": actual_value,
            "expected": expected_value,
            "deviation": deviation,
            "is_success": is_success,
            "test_data": test_data or {}
        })
        
        # 限制历史记录长度
        if len(hyp["test_history"]) > 30:
            hyp["test_history"] = hyp["test_history"][-30:]
        
        # 检查状态变更
        old_status = hyp["status"]
        
        if hyp["success_count"] >= self.VERIFIED_THRESHOLD:
            hyp["status"] = "verified"
            if old_status != "verified":
                self.registry["statistics"]["total_verified"] += 1
                print(f"  [假设库] {hyp_id} 升级为 verified（成功 {hyp['success_count']} 次）")
        
        elif hyp["consecutive_fails"] >= self.FAILED_THRESHOLD:
            hyp["status"] = "failed"
            if old_status != "failed":
                self.registry["statistics"]["total_failed"] += 1
                print(f"  [假设库] {hyp_id} 标记为 failed（连续失败 {hyp['consecutive_fails']} 次）")
        
        self._save_registry()
        
        return {
            "hyp_id": hyp_id,
            "is_success": is_success,
            "status": hyp["status"],
            "weight": hyp["weight"],
            "success_count": hyp["success_count"],
            "fail_count": hyp["fail_count"]
        }
    
    def add_reflection(self, hyp_id: str, reflection: str):
        """
        添加反思（假设失败时）
        
        Args:
            hyp_id: 假设 ID
            reflection: 反思文本（LLM 生成）
        """
        if hyp_id in self.registry["hypotheses"]:
            self.registry["hypotheses"][hyp_id]["reflection"] = {
                "text": reflection,
                "timestamp": datetime.now().isoformat()
            }
            self._save_registry()
    
    def record_cognitive_gain(
        self,
        date_str: str,
        new_insights: List[str],
        source: str = "deep_analysis"
    ) -> float:
        """
        记录认知增量
        
        Args:
            date_str: 日期
            new_insights: 新发现列表
            source: 来源
        
        Returns:
            cognitive_gain: 0-1 之间的认知增量分数
        """
        # 简单实现：有新发现就给 1.0，否则 0.0
        # 后续可以引入向量相似度判断是否真的"新"
        has_new = len(new_insights) > 0
        gain = 1.0 if has_new else 0.0
        
        log_entry = {
            "date": date_str,
            "timestamp": datetime.now().isoformat(),
            "cognitive_gain": gain,
            "new_insights_count": len(new_insights),
            "new_insights": new_insights,
            "source": source
        }
        
        self.cognitive_log.append(log_entry)
        self._save_cognitive_log()
        
        if has_new:
            print(f"  [认知增量] {date_str}: 发现 {len(new_insights)} 个新洞察")
        
        return gain
    
    def get_testing_hypotheses(self) -> List[Dict]:
        """获取所有 testing 状态的假设"""
        return [
            hyp for hyp in self.registry["hypotheses"].values()
            if hyp["status"] == "testing"
        ]
    
    def get_verified_hypotheses(self) -> List[Dict]:
        """获取所有 verified 状态的假设"""
        return [
            hyp for hyp in self.registry["hypotheses"].values()
            if hyp["status"] == "verified"
        ]
    
    def get_failed_hypotheses(self) -> List[Dict]:
        """获取所有 failed 状态的假设"""
        return [
            hyp for hyp in self.registry["hypotheses"].values()
            if hyp["status"] == "failed"
        ]
    
    def get_hypotheses_for_distillation(self, min_success_count: int = 10) -> List[Dict]:
        """
        获取可用于知识蒸馏的假设（第三阶段）
        
        Args:
            min_success_count: 最小成功次数
        
        Returns:
            符合条件的假设列表
        """
        return [
            hyp for hyp in self.registry["hypotheses"].values()
            if hyp["status"] == "verified" and hyp["success_count"] >= min_success_count
        ]
    
    def get_registry_summary(self) -> Dict:
        """获取假设库摘要"""
        hypotheses = self.registry["hypotheses"]
        
        return {
            "total": len(hypotheses),
            "testing": len([h for h in hypotheses.values() if h["status"] == "testing"]),
            "verified": len([h for h in hypotheses.values() if h["status"] == "verified"]),
            "failed": len([h for h in hypotheses.values() if h["status"] == "failed"]),
            "statistics": self.registry["statistics"],
            "ready_for_distillation": len(self.get_hypotheses_for_distillation())
        }
    
    def print_registry_report(self):
        """打印假设库报告"""
        summary = self.get_registry_summary()
        
        print(f"\n{'='*60}")
        print("假设实验库报告")
        print(f"{'='*60}")
        print(f"设备: {self.device_sn}")
        print(f"更新时间: {self.registry.get('updated_at', 'N/A')}")
        print(f"\n假设分布:")
        print(f"  测试中 (testing): {summary['testing']} 个")
        print(f"  已验证 (verified): {summary['verified']} 个")
        print(f"  已失败 (failed): {summary['failed']} 个")
        print(f"\n统计:")
        print(f"  累计生成: {summary['statistics']['total_generated']}")
        print(f"  累计验证: {summary['statistics']['total_verified']}")
        print(f"  累计失败: {summary['statistics']['total_failed']}")
        print(f"  可固化规则: {summary['ready_for_distillation']} 个")
        
        # 显示已验证的假设
        verified = self.get_verified_hypotheses()
        if verified:
            print(f"\n已验证假设 (Top 5):")
            for hyp in sorted(verified, key=lambda x: x['success_count'], reverse=True)[:5]:
                print(f"  - {hyp['id']}: {hyp['logic'][:40]}... (成功 {hyp['success_count']} 次)")
        
        print(f"{'='*60}\n")
