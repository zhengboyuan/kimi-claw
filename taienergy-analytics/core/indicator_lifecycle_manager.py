"""
V4.4 指标生命周期管理器

管理指标从发现到淘汰的全生命周期：
1. 候选池管理（观察期）
2. 活跃度追踪
3. 入库评审
4. 淘汰清理

规则：
- 观察期：30天
- 活跃度阈值：30天内验证次数 >= 3
- 未达标：自动废弃
- 紧急指标：标记"临时"，需月度评审转正
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class IndicatorLifecycleManager:
    """指标生命周期管理器"""
    
    # 观察期配置
    OBSERVATION_DAYS = 30
    ACTIVITY_THRESHOLD = 3  # 最少验证次数
    
    def __init__(self):
        self.candidate_pool_path = "memory/indicators/candidate/candidate_pool.json"
        self.temp_indicators_path = "memory/indicators/temp_indicators.json"
        self.catalog_base_path = "memory/indicators/catalog_v{}.json"
        self.retirement_log_path = "memory/indicators/retirement_log.json"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.candidate_pool_path), exist_ok=True)
        
        # 初始化文件
        self._init_files()
    
    def _init_files(self):
        """初始化存储文件"""
        files = [
            (self.candidate_pool_path, []),
            (self.temp_indicators_path, []),
            (self.retirement_log_path, [])
        ]
        
        for path, default in files:
            if not os.path.exists(path):
                with open(path, 'w') as f:
                    json.dump(default, f, indent=2)
    
    def add_candidate(self, indicator: Dict, source_device: str, 
                      discovery_context: str = "periodic") -> str:
        """
        添加候选指标到观察池
        
        Args:
            indicator: 指标定义
            source_device: 发现来源设备
            discovery_context: 发现场景（periodic/emergency）
        
        Returns:
            候选指标ID
        """
        candidate_id = f"cand_{datetime.now().strftime('%Y%m%d%H%M%S')}_{source_device}"
        
        candidate = {
            'id': candidate_id,
            'indicator': indicator,
            'source_device': source_device,
            'discovery_context': discovery_context,  # periodic/emergency
            'created_date': datetime.now().isoformat(),
            'activity_count': 0,
            'verification_history': [],
            'status': 'observing',  # observing/verified/rejected
            'promoted': False
        }
        
        # 紧急指标特殊标记
        if discovery_context == 'emergency':
            candidate['is_temporary'] = True
            candidate['temp_deadline'] = (datetime.now() + timedelta(days=30)).isoformat()
        
        # 保存到候选池
        pool = self._load_candidate_pool()
        pool.append(candidate)
        self._save_candidate_pool(pool)
        
        print(f"[Lifecycle] 候选指标加入观察池: {candidate_id}")
        print(f"  来源: {source_device}, 场景: {discovery_context}")
        
        return candidate_id
    
    def verify_candidate(self, candidate_id: str, device_sn: str, 
                        verification_result: Dict) -> bool:
        """
        验证候选指标（在某台设备上验证）
        
        Args:
            candidate_id: 候选指标ID
            device_sn: 验证设备
            verification_result: 验证结果
        
        Returns:
            是否验证成功
        """
        pool = self._load_candidate_pool()
        
        for candidate in pool:
            if candidate['id'] == candidate_id:
                # 增加活跃度
                candidate['activity_count'] += 1
                
                # 记录验证历史
                candidate['verification_history'].append({
                    'date': datetime.now().isoformat(),
                    'device': device_sn,
                    'result': verification_result
                })
                
                print(f"[Lifecycle] 候选指标验证: {candidate_id}")
                print(f"  设备: {device_sn}, 活跃度: {candidate['activity_count']}/{self.ACTIVITY_THRESHOLD}")
                
                self._save_candidate_pool(pool)
                return True
        
        return False
    
    def review_candidates(self, date_str: str) -> Dict:
        """
        月度评审候选指标
        
        Returns:
            评审结果
        """
        pool = self._load_candidate_pool()
        
        promoted = []
        rejected = []
        extended = []
        
        for candidate in pool:
            if candidate['status'] != 'observing':
                continue
            
            created = datetime.fromisoformat(candidate['created_date'])
            now = datetime.now()
            days_in_pool = (now - created).days
            
            # 紧急临时指标检查
            if candidate.get('is_temporary'):
                deadline = datetime.fromisoformat(candidate['temp_deadline'])
                if now > deadline:
                    # 超期未转正，自动废弃
                    rejected.append(candidate)
                    candidate['status'] = 'rejected'
                    candidate['reject_reason'] = 'temp_expired'
                    print(f"[Lifecycle] 临时指标超期废弃: {candidate['id']}")
                    continue
            
            # 活跃度检查
            if candidate['activity_count'] >= self.ACTIVITY_THRESHOLD:
                # 验证通过，可以入库
                promoted.append(candidate)
                candidate['status'] = 'verified'
                candidate['promoted'] = True
                print(f"[Lifecycle] 候选指标入库: {candidate['id']}")
            elif days_in_pool >= self.OBSERVATION_DAYS:
                # 观察期结束，活跃度不足，废弃
                rejected.append(candidate)
                candidate['status'] = 'rejected'
                candidate['reject_reason'] = 'insufficient_activity'
                print(f"[Lifecycle] 候选指标废弃（活跃度不足）: {candidate['id']}")
            else:
                # 继续观察
                extended.append(candidate)
        
        # 保存更新后的候选池
        self._save_candidate_pool(pool)
        
        # 入库通过的指标
        for candidate in promoted:
            self._promote_to_catalog(candidate, date_str)
        
        # 记录淘汰
        for candidate in rejected:
            self._log_retirement(candidate, date_str)
        
        return {
            'review_date': date_str,
            'promoted': len(promoted),
            'rejected': len(rejected),
            'extended': len(extended),
            'promoted_ids': [c['id'] for c in promoted],
            'rejected_ids': [c['id'] for c in rejected]
        }
    
    def _promote_to_catalog(self, candidate: Dict, date_str: str):
        """将候选指标入库到正式指标库"""
        # 获取当前月份版本
        month_key = date_str[:7]  # 2025-07
        catalog_path = self.catalog_base_path.format(month_key)
        
        # 读取或创建指标库
        if os.path.exists(catalog_path):
            with open(catalog_path, 'r') as f:
                catalog = json.load(f)
        else:
            catalog = {
                'version': month_key,
                'created_at': datetime.now().isoformat(),
                'indicators': {}
            }
        
        # 生成正式指标ID
        ind_id = f"ind_{candidate['source_device']}_{len(catalog['indicators']):03d}"
        
        # 添加指标
        catalog['indicators'][ind_id] = {
            'id': ind_id,
            'name': candidate['indicator'].get('name', 'Unknown'),
            'formula': candidate['indicator'].get('formula', ''),
            'source_device': candidate['source_device'],
            'discovery_context': candidate['discovery_context'],
            'verified_devices': list(set([
                v['device'] for v in candidate['verification_history']
            ])),
            'promoted_date': date_str,
            'source': 'system' if candidate['discovery_context'] == 'periodic' else 'emergency_auto',
            'lifecycle': 'active'
        }
        
        # 保存
        with open(catalog_path, 'w') as f:
            json.dump(catalog, f, indent=2)
        
        print(f"[Lifecycle] 指标正式入库: {ind_id}")
    
    def _log_retirement(self, candidate: Dict, date_str: str):
        """记录淘汰日志"""
        log = []
        if os.path.exists(self.retirement_log_path):
            with open(self.retirement_log_path, 'r') as f:
                log = json.load(f)
        
        log.append({
            'date': date_str,
            'candidate_id': candidate['id'],
            'indicator_name': candidate['indicator'].get('name', 'Unknown'),
            'source_device': candidate['source_device'],
            'reason': candidate.get('reject_reason', 'unknown'),
            'activity_count': candidate['activity_count'],
            'days_observed': (datetime.now() - 
                datetime.fromisoformat(candidate['created_date'])).days
        })
        
        with open(self.retirement_log_path, 'w') as f:
            json.dump(log, f, indent=2)
    
    def _load_candidate_pool(self) -> List:
        """加载候选池"""
        with open(self.candidate_pool_path, 'r') as f:
            return json.load(f)
    
    def _save_candidate_pool(self, pool: List):
        """保存候选池"""
        with open(self.candidate_pool_path, 'w') as f:
            json.dump(pool, f, indent=2)
    
    def get_candidate_stats(self) -> Dict:
        """获取候选池统计"""
        pool = self._load_candidate_pool()
        
        observing = sum(1 for c in pool if c['status'] == 'observing')
        temporary = sum(1 for c in pool if c.get('is_temporary') and c['status'] == 'observing')
        
        return {
            'total_candidates': len(pool),
            'observing': observing,
            'temporary': temporary,
            'avg_activity': np.mean([c['activity_count'] for c in pool]) if pool else 0
        }


# 便捷函数
def add_indicator_candidate(indicator: Dict, source_device: str, 
                           context: str = "periodic") -> str:
    """添加候选指标"""
    manager = IndicatorLifecycleManager()
    return manager.add_candidate(indicator, source_device, context)


def verify_indicator_candidate(candidate_id: str, device_sn: str, 
                               result: Dict) -> bool:
    """验证候选指标"""
    manager = IndicatorLifecycleManager()
    return manager.verify_candidate(candidate_id, device_sn, result)


def review_monthly_candidates(date_str: str) -> Dict:
    """月度评审"""
    manager = IndicatorLifecycleManager()
    return manager.review_candidates(date_str)
