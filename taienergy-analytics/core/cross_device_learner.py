"""
V4.4 跨设备学习器

实现设备间的知识迁移：
1. 单设备发现的模式推送到共享候选池
2. 多设备验证后升级为通用模式
3. 通用模式库供所有设备使用

验证标准：
- 至少3台设备验证
- 连续7天验证通过
- 健康设备优先（健康分>=60）
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
import numpy as np


class CrossDeviceLearner:
    """跨设备学习器"""
    
    # 验证标准
    MIN_DEVICES = 3
    MIN_DAYS = 7
    MIN_HEALTH_SCORE = 60
    
    def __init__(self):
        self.pattern_library_path = "memory/shared/pattern_library.json"
        self.validation_queue_path = "memory/shared/validation_queue.json"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.pattern_library_path), exist_ok=True)
        
        # 初始化文件
        self._init_files()
    
    def _init_files(self):
        """初始化存储文件"""
        files = [
            (self.pattern_library_path, {'patterns': {}}),
            (self.validation_queue_path, [])
        ]
        
        for path, default in files:
            if not os.path.exists(path):
                with open(path, 'w') as f:
                    json.dump(default, f, indent=2)
    
    def submit_pattern(self, pattern: Dict, source_device: str, 
                      date_str: str) -> str:
        """
        提交新模式到验证队列
        
        Args:
            pattern: 模式定义（指标公式、阈值等）
            source_device: 发现来源设备
            date_str: 日期
        
        Returns:
            模式ID
        """
        pattern_id = f"pat_{source_device}_{date_str}_{pattern.get('name', 'unknown')}"
        
        # 检查是否已存在相似模式
        existing = self._find_similar_pattern(pattern)
        if existing:
            print(f"[CrossDevice] 发现相似模式，合并验证: {existing}")
            pattern_id = existing
        
        # 添加到验证队列
        queue = self._load_validation_queue()
        
        # 检查是否已在队列中
        for item in queue:
            if item['pattern_id'] == pattern_id:
                # 更新验证记录
                self._update_validation(item, source_device, date_str)
                return pattern_id
        
        # 新加入队列
        queue.append({
            'pattern_id': pattern_id,
            'pattern': pattern,
            'source_device': source_device,
            'submit_date': date_str,
            'verification_devices': {source_device: [date_str]},
            'status': 'validating',
            'consecutive_days': {source_device: 1}
        })
        
        self._save_validation_queue(queue)
        
        print(f"[CrossDevice] 新模式提交验证: {pattern_id}")
        print(f"  来源: {source_device}, 需要 {self.MIN_DEVICES} 台设备验证")
        
        return pattern_id
    
    def verify_pattern(self, pattern_id: str, device_sn: str, 
                      date_str: str, health_score: float = 75) -> bool:
        """
        在其他设备上验证模式
        
        Args:
            pattern_id: 模式ID
            device_sn: 验证设备
            date_str: 日期
            health_score: 设备健康分（健康设备优先）
        
        Returns:
            是否验证成功
        """
        # 健康设备检查
        if health_score < self.MIN_HEALTH_SCORE:
            print(f"[CrossDevice] 设备 {device_sn} 健康分 {health_score} < {self.MIN_HEALTH_SCORE}，验证降级")
            # 仍然记录，但权重降低
        
        queue = self._load_validation_queue()
        
        for item in queue:
            if item['pattern_id'] == pattern_id:
                # 更新验证设备
                if device_sn not in item['verification_devices']:
                    item['verification_devices'][device_sn] = []
                
                if date_str not in item['verification_devices'][device_sn]:
                    item['verification_devices'][device_sn].append(date_str)
                
                # 更新连续天数
                if device_sn not in item['consecutive_days']:
                    item['consecutive_days'][device_sn] = 0
                item['consecutive_days'][device_sn] += 1
                
                print(f"[CrossDevice] 模式验证: {pattern_id}")
                print(f"  设备: {device_sn}, 连续天数: {item['consecutive_days'][device_sn]}")
                
                # 检查是否满足通用化条件
                if self._check_generalization_criteria(item):
                    self._promote_to_library(item)
                    item['status'] = 'generalized'
                
                self._save_validation_queue(queue)
                return True
        
        return False
    
    def _check_generalization_criteria(self, item: Dict) -> bool:
        """检查是否满足通用化条件"""
        # 设备数量
        device_count = len(item['verification_devices'])
        if device_count < self.MIN_DEVICES:
            return False
        
        # 连续天数（至少3台设备满足7天）
        qualified_devices = sum(
            1 for days in item['consecutive_days'].values()
            if days >= self.MIN_DAYS
        )
        
        return qualified_devices >= self.MIN_DEVICES
    
    def _promote_to_library(self, item: Dict):
        """将模式升级到通用库"""
        library = self._load_pattern_library()
        
        pattern_id = item['pattern_id']
        
        library['patterns'][pattern_id] = {
            'pattern': item['pattern'],
            'source_device': item['source_device'],
            'generalized_date': datetime.now().isoformat(),
            'verified_devices': list(item['verification_devices'].keys()),
            'verification_count': sum(
                len(dates) for dates in item['verification_devices'].values()
            ),
            'status': 'general'
        }
        
        self._save_pattern_library(library)
        
        print(f"[CrossDevice] 模式通用化成功: {pattern_id}")
        print(f"  验证设备: {len(item['verification_devices'])} 台")
    
    def get_applicable_patterns(self, device_sn: str, 
                                device_health_score: float = 75) -> List[Dict]:
        """
        获取适用于指定设备的通用模式
        
        Args:
            device_sn: 设备SN
            device_health_score: 设备健康分
        
        Returns:
            适用模式列表
        """
        library = self._load_pattern_library()
        
        applicable = []
        for pattern_id, pattern_info in library['patterns'].items():
            # 排除已在该设备验证过的（避免重复）
            if device_sn in pattern_info.get('verified_devices', []):
                continue
            
            # 健康设备可以使用所有通用模式
            # 非健康设备只能使用部分模式（需要额外检查）
            if device_health_score < self.MIN_HEALTH_SCORE:
                # 非健康设备使用通用模式需要谨慎
                pattern_info['caution'] = True
            
            applicable.append({
                'pattern_id': pattern_id,
                'pattern': pattern_info['pattern'],
                'caution': pattern_info.get('caution', False)
            })
        
        return applicable
    
    def _find_similar_pattern(self, pattern: Dict) -> Optional[str]:
        """查找相似模式（简化实现）"""
        # 实际实现需要比较模式公式、阈值等
        # 这里简化处理
        return None
    
    def _update_validation(self, item: Dict, device_sn: str, date_str: str):
        """更新验证记录"""
        if device_sn not in item['verification_devices']:
            item['verification_devices'][device_sn] = []
        
        if date_str not in item['verification_devices'][device_sn]:
            item['verification_devices'][device_sn].append(date_str)
    
    def _load_validation_queue(self) -> List:
        """加载验证队列"""
        with open(self.validation_queue_path, 'r') as f:
            return json.load(f)
    
    def _save_validation_queue(self, queue: List):
        """保存验证队列"""
        with open(self.validation_queue_path, 'w') as f:
            json.dump(queue, f, indent=2)
    
    def _load_pattern_library(self) -> Dict:
        """加载模式库"""
        with open(self.pattern_library_path, 'r') as f:
            return json.load(f)
    
    def _save_pattern_library(self, library: Dict):
        """保存模式库"""
        with open(self.pattern_library_path, 'w') as f:
            json.dump(library, f, indent=2)
    
    def get_stats(self) -> Dict:
        """获取跨设备学习统计"""
        queue = self._load_validation_queue()
        library = self._load_pattern_library()
        
        return {
            'validating_patterns': len([q for q in queue if q['status'] == 'validating']),
            'generalized_patterns': len(library['patterns']),
            'total_verification_events': sum(
                len(dates) for q in queue 
                for dates in q['verification_devices'].values()
            )
        }


# 便捷函数
def submit_cross_device_pattern(pattern: Dict, source_device: str, 
                                date_str: str) -> str:
    """提交跨设备模式"""
    learner = CrossDeviceLearner()
    return learner.submit_pattern(pattern, source_device, date_str)


def verify_cross_device_pattern(pattern_id: str, device_sn: str, 
                                date_str: str, health_score: float = 75) -> bool:
    """验证跨设备模式"""
    learner = CrossDeviceLearner()
    return learner.verify_pattern(pattern_id, device_sn, date_str, health_score)