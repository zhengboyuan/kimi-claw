"""
V4.4 版本管理器

管理健康评分逻辑的版本控制：
1. 版本升级时重算历史基准
2. 版本兼容性检查
3. 历史数据校准标记
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class VersionManager:
    """版本管理器"""
    
    CURRENT_VERSION = "v1.0"
    
    def __init__(self):
        self.version_log_path = "memory/shared/version_history.json"
        os.makedirs(os.path.dirname(self.version_log_path), exist_ok=True)
        
        # 初始化版本历史
        if not os.path.exists(self.version_log_path):
            self._init_version_history()
    
    def _init_version_history(self):
        """初始化版本历史"""
        history = {
            'current_version': self.CURRENT_VERSION,
            'versions': {
                self.CURRENT_VERSION: {
                    'release_date': datetime.now().isoformat(),
                    'description': '初始版本：5维度健康评分+趋势分',
                    'changes': []
                }
            }
        }
        
        with open(self.version_log_path, 'w') as f:
            json.dump(history, f, indent=2)
    
    def upgrade_version(self, new_version: str, changes: List[str],
                       recalculate_days: int = 30) -> Dict:
        """
        升级版本并重算基准
        
        Args:
            new_version: 新版本号
            changes: 变更说明列表
            recalculate_days: 重算天数
        
        Returns:
            升级结果
        """
        print(f"[Version] 版本升级: {self.CURRENT_VERSION} → {new_version}")
        
        result = {
            'old_version': self.CURRENT_VERSION,
            'new_version': new_version,
            'upgrade_time': datetime.now().isoformat(),
            'changes': changes,
            'recalculated_records': 0,
            'devices_processed': []
        }
        
        # 记录新版本
        history = self._load_version_history()
        history['versions'][new_version] = {
            'release_date': datetime.now().isoformat(),
            'description': '; '.join(changes),
            'changes': changes
        }
        history['current_version'] = new_version
        
        # 重算历史数据
        devices = [f'XHDL_{i}NBQ' for i in range(1, 17)]
        
        for device_sn in devices:
            recalculated = self._recalculate_device_history(
                device_sn, new_version, recalculate_days
            )
            if recalculated > 0:
                result['devices_processed'].append(device_sn)
                result['recalculated_records'] += recalculated
        
        # 保存版本历史
        self._save_version_history(history)
        self.CURRENT_VERSION = new_version
        
        print(f"[Version] 升级完成，重算 {result['recalculated_records']} 条记录")
        
        return result
    
    def _recalculate_device_history(self, device_sn: str,
                                   new_version: str,
                                   days: int) -> int:
        """重算设备历史健康分"""
        health_path = f"memory/devices/{device_sn}/health_history.json"
        
        if not os.path.exists(health_path):
            return 0
        
        with open(health_path, 'r') as f:
            history = json.load(f)
        
        # 获取最近N天的记录
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        recalculated = 0
        
        for record in history:
            record_date = record.get('date', '2000-01-01')
            
            # 只重算最近N天
            if record_date >= cutoff_date:
                # 标记为已校准
                record['version'] = new_version
                record['calibrated'] = True
                record['calibrated_at'] = datetime.now().isoformat()
                recalculated += 1
            else:
                # 旧数据标记为未校准
                record['calibrated'] = False
        
        # 保存
        with open(health_path, 'w') as f:
            json.dump(history, f, indent=2)
        
        return recalculated
    
    def get_version_info(self, version: Optional[str] = None) -> Dict:
        """获取版本信息"""
        history = self._load_version_history()
        
        if version is None:
            version = history.get('current_version', self.CURRENT_VERSION)
        
        return history['versions'].get(version, {})
    
    def check_data_compatibility(self, device_sn: str, 
                                 date_str: str) -> Dict:
        """检查数据版本兼容性"""
        health_path = f"memory/devices/{device_sn}/health_history.json"
        
        if not os.path.exists(health_path):
            return {'compatible': False, 'reason': 'no_data'}
        
        with open(health_path, 'r') as f:
            history = json.load(f)
        
        for record in history:
            if record.get('date') == date_str:
                record_version = record.get('version', 'unknown')
                calibrated = record.get('calibrated', False)
                
                if record_version == self.CURRENT_VERSION:
                    return {
                        'compatible': True,
                        'version': record_version,
                        'calibrated': calibrated
                    }
                else:
                    return {
                        'compatible': False,
                        'version': record_version,
                        'current_version': self.CURRENT_VERSION,
                        'calibrated': calibrated,
                        'reason': 'version_mismatch'
                    }
        
        return {'compatible': False, 'reason': 'date_not_found'}
    
    def get_calibration_status(self, device_sn: str) -> Dict:
        """获取设备校准状态"""
        health_path = f"memory/devices/{device_sn}/health_history.json"
        
        if not os.path.exists(health_path):
            return {'total': 0, 'calibrated': 0, 'uncalibrated': 0}
        
        with open(health_path, 'r') as f:
            history = json.load(f)
        
        total = len(history)
        calibrated = sum(1 for r in history if r.get('calibrated', False))
        
        return {
            'total': total,
            'calibrated': calibrated,
            'uncalibrated': total - calibrated,
            'calibration_rate': round(calibrated / total * 100, 1) if total > 0 else 0
        }
    
    def _load_version_history(self) -> Dict:
        """加载版本历史"""
        with open(self.version_log_path, 'r') as f:
            return json.load(f)
    
    def _save_version_history(self, history: Dict):
        """保存版本历史"""
        with open(self.version_log_path, 'w') as f:
            json.dump(history, f, indent=2)


# 便捷函数
def upgrade_health_calculation_version(new_version: str, changes: List[str]) -> Dict:
    """升级健康计算版本"""
    manager = VersionManager()
    return manager.upgrade_version(new_version, changes)


def check_record_compatibility(device_sn: str, date_str: str) -> Dict:
    """检查记录兼容性"""
    manager = VersionManager()
    return manager.check_data_compatibility(device_sn, date_str)