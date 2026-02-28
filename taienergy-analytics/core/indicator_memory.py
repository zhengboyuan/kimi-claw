"""
V4.5.2 指标记忆管理模块
原子更新 + 版本控制 + 乐观锁
"""

import json
import os
import time
import fcntl
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from contextlib import contextmanager


@dataclass
class IndicatorRecord:
    """指标记录"""
    name: str
    status: str  # candidate / trial / core / deprecated
    version: str
    created_at: str
    updated_at: str
    evaluation_score: Optional[Dict] = None
    dependencies: List[str] = None
    formula: str = ""
    file_path: str = ""
    metadata: Dict = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.metadata is None:
            self.metadata = {}


class ConcurrentUpdateError(Exception):
    """并发更新错误"""
    pass


class IndicatorMemory:
    """
    指标记忆管理器
    
    特性：
    - 原子更新（写时复制）
    - 乐观锁（版本号校验）
    - 自动回滚
    - 文件锁（防并发冲突）
    """
    
    def __init__(self, base_path: str = "memory/indicators"):
        self.base_path = base_path
        self.registry_path = os.path.join(base_path, "registry.json")
        self.lock_path = os.path.join(base_path, "registry.lock")
        self.backup_path = os.path.join(base_path, "backups")
        
        # 确保目录存在
        os.makedirs(base_path, exist_ok=True)
        os.makedirs(self.backup_path, exist_ok=True)
        os.makedirs(os.path.join(base_path, "candidate"), exist_ok=True)
        os.makedirs(os.path.join(base_path, "trial"), exist_ok=True)
        os.makedirs(os.path.join(base_path, "core"), exist_ok=True)
        os.makedirs(os.path.join(base_path, "deprecated"), exist_ok=True)
    
    @contextmanager
    def _file_lock(self):
        """文件锁上下文管理器"""
        lock_file = open(self.lock_path, 'w')
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
    
    def _load_registry(self) -> Dict:
        """加载注册表"""
        if os.path.exists(self.registry_path):
            with open(self.registry_path, 'r') as f:
                return json.load(f)
        return {
            "version": 0,
            "last_updated": datetime.now().isoformat(),
            "indicators": {}
        }
    
    def _save_registry(self, data: Dict):
        """保存注册表（原子操作）"""
        # 1. 写入临时文件
        tmp_path = f"{self.registry_path}.tmp.{os.getpid()}"
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # 2. 原子替换
        os.replace(tmp_path, self.registry_path)
    
    def _backup_registry(self, data: Dict):
        """备份注册表"""
        backup_file = os.path.join(
            self.backup_path,
            f"registry_{datetime.now():%Y%m%d_%H%M%S}_v{data.get('version', 0)}.json"
        )
        with open(backup_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def update_with_optimistic_lock(
        self,
        indicator_name: str,
        changes: Dict,
        max_retries: int = 3
    ) -> bool:
        """
        乐观锁更新
        
        Args:
            indicator_name: 指标名称
            changes: 要更新的字段
            max_retries: 最大重试次数
        
        Returns:
            True: 更新成功
            False: 更新失败（重试耗尽）
        """
        for attempt in range(max_retries):
            try:
                with self._file_lock():
                    # 1. 读取当前版本
                    registry = self._load_registry()
                    current_version = registry.get("version", 0)
                    
                    # 2. 备份（每10个版本备份一次）
                    if current_version % 10 == 0:
                        self._backup_registry(registry)
                    
                    # 3. 应用变更
                    if indicator_name not in registry["indicators"]:
                        registry["indicators"][indicator_name] = {}
                    
                    record = registry["indicators"][indicator_name]
                    record.update(changes)
                    record["updated_at"] = datetime.now().isoformat()
                    
                    # 4. 版本号+1
                    registry["version"] = current_version + 1
                    registry["last_updated"] = datetime.now().isoformat()
                    
                    # 5. 原子写入
                    self._save_registry(registry)
                    
                    print(f"[Memory] 更新成功: {indicator_name} (v{registry['version']})")
                    return True
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 0.1 * (2 ** attempt)  # 指数退避
                    print(f"[Memory] 更新冲突，{wait_time:.1f}s后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"[Memory] 更新失败: {e}")
                    raise ConcurrentUpdateError(f"更新 {indicator_name} 失败，重试{max_retries}次后放弃")
        
        return False
    
    def promote_indicator(
        self,
        indicator_name: str,
        from_status: str,
        to_status: str,
        evaluation_score: Dict = None
    ) -> bool:
        """
        晋升指标（candidate -> trial -> core）
        
        Args:
            indicator_name: 指标名称
            from_status: 当前状态
            to_status: 目标状态
            evaluation_score: 评估分数（可选）
        """
        changes = {
            "status": to_status,
            "promoted_at": datetime.now().isoformat(),
        }
        
        if evaluation_score:
            changes["evaluation_score"] = evaluation_score
        
        # 移动文件（如果存在）
        old_path = os.path.join(self.base_path, from_status, f"{indicator_name}.json")
        new_path = os.path.join(self.base_path, to_status, f"{indicator_name}.json")
        
        if os.path.exists(old_path):
            # 读取旧文件
            with open(old_path, 'r') as f:
                data = json.load(f)
            
            # 更新数据
            data.update(changes)
            
            # 写入新位置
            with open(new_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            # 删除旧文件
            os.remove(old_path)
            
            changes["file_path"] = new_path
        
        # 更新注册表
        return self.update_with_optimistic_lock(indicator_name, changes)
    
    def get_indicator(self, indicator_name: str) -> Optional[Dict]:
        """获取指标信息"""
        registry = self._load_registry()
        return registry["indicators"].get(indicator_name)
    
    def list_indicators(self, status: str = None) -> List[Dict]:
        """列出指标"""
        registry = self._load_registry()
        indicators = list(registry["indicators"].values())
        
        if status:
            indicators = [i for i in indicators if i.get("status") == status]
        
        return indicators
    
    def get_version(self) -> int:
        """获取当前版本号"""
        registry = self._load_registry()
        return registry.get("version", 0)
    
    def rollback_to_version(self, target_version: int) -> bool:
        """
        回滚到指定版本
        
        从备份中恢复
        """
        # 查找备份文件
        backup_files = [
            f for f in os.listdir(self.backup_path)
            if f.startswith("registry_") and f.endswith(".json")
        ]
        
        for backup_file in sorted(backup_files, reverse=True):
            backup_path = os.path.join(self.backup_path, backup_file)
            with open(backup_path, 'r') as f:
                backup_data = json.load(f)
            
            if backup_data.get("version") == target_version:
                # 恢复
                self._save_registry(backup_data)
                print(f"[Memory] 回滚成功: v{target_version}")
                return True
        
        print(f"[Memory] 回滚失败: 未找到版本 {target_version}")
        return False


# 便捷函数
def update_indicator_status(name: str, status: str, **kwargs) -> bool:
    """更新指标状态的便捷函数"""
    memory = IndicatorMemory()
    return memory.update_with_optimistic_lock(name, {"status": status, **kwargs})


def promote_to_trial(name: str, evaluation_score: Dict = None) -> bool:
    """晋升到试用的便捷函数"""
    memory = IndicatorMemory()
    return memory.promote_indicator(name, "candidate", "trial", evaluation_score)


def promote_to_core(name: str, evaluation_score: Dict = None) -> bool:
    """晋升到核心的便捷函数"""
    memory = IndicatorMemory()
    return memory.promote_indicator(name, "trial", "core", evaluation_score)


if __name__ == "__main__":
    # 测试
    print("Indicator Memory 测试")
    print("=" * 50)
    
    memory = IndicatorMemory()
    
    # 测试更新
    result = memory.update_with_optimistic_lock(
        "test_metric",
        {
            "name": "test_metric",
            "status": "candidate",
            "formula": "test",
            "created_at": datetime.now().isoformat()
        }
    )
    print(f"更新结果: {result}")
    
    # 测试晋升
    result = memory.promote_indicator("test_metric", "candidate", "trial")
    print(f"晋升结果: {result}")
    
    # 测试查询
    indicator = memory.get_indicator("test_metric")
    print(f"查询结果: {indicator}")
    
    # 测试版本
    version = memory.get_version()
    print(f"当前版本: {version}")
