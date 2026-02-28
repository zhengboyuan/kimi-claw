"""
V4.4 维护建议生成器

基于健康评分和风险识别，生成具体维护建议：
1. 紧急处理（48小时内）
2. 计划维护（本周/本月）
3. 观察监控（持续跟踪）
4. 延后维护（设备良好）

维护期数据排除：
- 提前24小时录入维护计划
- 临时维护自动标记
- 维护期数据不参与健康评分
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class MaintenanceAdvisor:
    """维护建议生成器"""
    
    # 建议等级
    LEVELS = {
        'emergency': {'hours': 48, 'icon': '🔴'},
        'urgent': {'days': 7, 'icon': '🟠'},
        'planned': {'days': 30, 'icon': '🟡'},
        'monitor': {'action': '观察', 'icon': '👁️'},
        'postpone': {'action': '延后', 'icon': '✅'}
    }
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.maintenance_path = f"memory/devices/{device_sn}/maintenance_log.json"
        self.health_path = f"memory/devices/{device_sn}/health_history.json"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.maintenance_path), exist_ok=True)
    
    def generate_advice(self, date_str: str, health_record: Dict) -> Dict:
        """
        生成维护建议
        
        Args:
            date_str: 日期
            health_record: 健康评分记录
        
        Returns:
            维护建议
        """
        score = health_record.get('total_score', 75)
        trend = health_record.get('trend_score', 0)
        level = health_record.get('level', 'unknown')
        
        advice = {
            'date': date_str,
            'device_sn': self.device_sn,
            'health_score': score,
            'trend_score': trend,
            'level': level,
            'recommendation': {},
            'reason': '',
            'actions': []
        }
        
        # 根据健康等级生成建议
        if level == 'danger':
            advice['recommendation'] = {
                'level': 'emergency',
                'icon': self.LEVELS['emergency']['icon'],
                'deadline': (datetime.strptime(date_str, '%Y-%m-%d') + 
                           timedelta(hours=48)).strftime('%Y-%m-%d %H:%M'),
                'action': '立即停机检查'
            }
            advice['reason'] = f'健康分 {score} 处于危险区间，需紧急处理'
            advice['actions'] = [
                '立即联系维护团队',
                '准备备用设备',
                '检查关键指标异常原因'
            ]
        
        elif level == 'warning':
            advice['recommendation'] = {
                'level': 'urgent',
                'icon': self.LEVELS['urgent']['icon'],
                'deadline': (datetime.strptime(date_str, '%Y-%m-%d') + 
                           timedelta(days=7)).strftime('%Y-%m-%d'),
                'action': '本周内安排维护'
            }
            advice['reason'] = f'健康分 {score} 处于预警区间，需计划维护'
            advice['actions'] = [
                '安排维护窗口',
                '准备备件',
                '关注退化趋势'
            ]
        
        elif level == 'attention':
            # 关注级别，看趋势
            if trend < -2:
                # 退化加速
                advice['recommendation'] = {
                    'level': 'planned',
                    'icon': self.LEVELS['planned']['icon'],
                    'deadline': (datetime.strptime(date_str, '%Y-%m-%d') + 
                               timedelta(days=14)).strftime('%Y-%m-%d'),
                    'action': '2周内安排检查'
                }
                advice['reason'] = f'健康分 {score} 尚可，但趋势分 {trend} 显示退化加速'
                advice['actions'] = [
                    '加强监控频率',
                    '准备维护计划',
                    '分析退化原因'
                ]
            else:
                advice['recommendation'] = {
                    'level': 'monitor',
                    'icon': self.LEVELS['monitor']['icon'],
                    'action': '持续观察'
                }
                advice['reason'] = f'健康分 {score} 需关注，但趋势平稳'
                advice['actions'] = [
                    '每日监控',
                    '记录异常',
                    '准备应急预案'
                ]
        
        elif level in ['good', 'excellent']:
            # 良好或优秀，看是否可以延后维护
            last_maintenance = self._get_last_maintenance()
            if last_maintenance:
                days_since = (datetime.strptime(date_str, '%Y-%m-%d') - 
                            datetime.fromisoformat(last_maintenance)).days
                if days_since > 60 and trend > -1:
                    # 运行良好，可以延后
                    advice['recommendation'] = {
                        'level': 'postpone',
                        'icon': self.LEVELS['postpone']['icon'],
                        'action': '下次维护可延后2周'
                    }
                    advice['reason'] = f'健康分 {score} 良好，运行稳定，维护可延后'
                    advice['actions'] = [
                        '继续正常监控',
                        '记录设备表现',
                        '更新维护计划'
                    ]
                else:
                    advice['recommendation'] = {
                        'level': 'monitor',
                        'icon': self.LEVELS['monitor']['icon'],
                        'action': '按计划维护'
                    }
                    advice['reason'] = f'健康分 {score} 良好，按计划维护'
                    advice['actions'] = ['按计划执行维护']
        
        # 保存建议
        self._save_advice(advice)
        
        return advice
    
    def schedule_maintenance(self, date_str: str, maintenance_type: str, 
                            description: str = "") -> bool:
        """
        录入维护计划（提前24小时）
        
        Args:
            date_str: 维护日期
            maintenance_type: 维护类型
            description: 描述
        
        Returns:
            是否成功
        """
        log = []
        if os.path.exists(self.maintenance_path):
            with open(self.maintenance_path, 'r') as f:
                log = json.load(f)
        
        log.append({
            'scheduled_date': date_str,
            'recorded_at': datetime.now().isoformat(),
            'type': maintenance_type,
            'description': description,
            'status': 'scheduled'
        })
        
        with open(self.maintenance_path, 'w') as f:
            json.dump(log, f, indent=2)
        
        print(f"[Maintenance] 维护计划已录入: {self.device_sn} @ {date_str}")
        return True
    
    def mark_maintenance_complete(self, date_str: str, 
                                  result: str = "completed") -> bool:
        """标记维护完成"""
        if not os.path.exists(self.maintenance_path):
            return False
        
        with open(self.maintenance_path, 'r') as f:
            log = json.load(f)
        
        for entry in log:
            if entry.get('scheduled_date') == date_str:
                entry['status'] = 'completed'
                entry['completed_at'] = datetime.now().isoformat()
                entry['result'] = result
        
        with open(self.maintenance_path, 'w') as f:
            json.dump(log, f, indent=2)
        
        return True
    
    def is_maintenance_period(self, date_str: str) -> bool:
        """检查指定日期是否在维护期"""
        if not os.path.exists(self.maintenance_path):
            return False
        
        with open(self.maintenance_path, 'r') as f:
            log = json.load(f)
        
        for entry in log:
            if entry.get('scheduled_date') == date_str:
                return True
            # 检查维护后恢复期（1天）
            if entry.get('status') == 'completed':
                complete_date = datetime.fromisoformat(
                    entry.get('completed_at', '2000-01-01')
                ).strftime('%Y-%m-%d')
                if complete_date == date_str:
                    return True
        
        return False
    
    def _get_last_maintenance(self) -> Optional[str]:
        """获取上次维护日期"""
        if not os.path.exists(self.maintenance_path):
            return None
        
        with open(self.maintenance_path, 'r') as f:
            log = json.load(f)
        
        completed = [
            e for e in log 
            if e.get('status') == 'completed'
        ]
        
        if not completed:
            return None
        
        # 返回最近的维护日期
        latest = max(completed, 
                    key=lambda x: x.get('completed_at', '2000-01-01'))
        return latest.get('completed_at')
    
    def _save_advice(self, advice: Dict):
        """保存建议"""
        # 建议可以单独存储或与维护日志合并
        pass


# 便捷函数
def generate_maintenance_advice(device_sn: str, date_str: str, 
                               health_record: Dict) -> Dict:
    """生成维护建议"""
    advisor = MaintenanceAdvisor(device_sn)
    return advisor.generate_advice(date_str, health_record)


def schedule_device_maintenance(device_sn: str, date_str: str, 
                               mtype: str, desc: str = "") -> bool:
    """录入维护计划"""
    advisor = MaintenanceAdvisor(device_sn)
    return advisor.schedule_maintenance(date_str, mtype, desc)