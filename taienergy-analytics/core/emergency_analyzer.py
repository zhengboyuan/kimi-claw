"""
V4.4 紧急异常分析器

重大异常时的快速指标发现：
1. 触发条件判断
2. 异常前后对比分析
3. 快速指标生成
4. 临时指标标记

规则：
- 健康分骤降>20分触发
- 或P0异常发生触发
- 生成的指标标记为"临时"
- 必须经月度评审才能转正
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import numpy as np


class EmergencyAnalyzer:
    """紧急异常分析器"""
    
    # 触发阈值
    HEALTH_DROP_THRESHOLD = 20  # 健康分骤降阈值
    
    def __init__(self):
        self.temp_indicators_path = "memory/indicators/temp_indicators.json"
        os.makedirs(os.path.dirname(self.temp_indicators_path), exist_ok=True)
        
        # 初始化存储
        if not os.path.exists(self.temp_indicators_path):
            with open(self.temp_indicators_path, 'w') as f:
                json.dump([], f)
    
    def check_emergency_trigger(self, current_health: Dict,
                                previous_health: Dict) -> bool:
        """
        检查是否触发紧急分析
        
        Args:
            current_health: 当前健康记录
            previous_health: 上一周期健康记录
        
        Returns:
            是否触发
        """
        # 触发条件1：健康分骤降
        current_score = current_health.get('total_score', 75)
        previous_score = previous_health.get('total_score', 75)
        
        if previous_score - current_score >= self.HEALTH_DROP_THRESHOLD:
            print(f"[Emergency] 健康分骤降触发: {previous_score} → {current_score}")
            return True
        
        # 触发条件2：P0异常
        current_level = current_health.get('level', 'unknown')
        if current_level == 'danger':
            print(f"[Emergency] 危险等级触发")
            return True
        
        return False
    
    def analyze_emergency(self, device_sn: str, date_str: str,
                         current_data: Dict,
                         historical_data: List[Dict]) -> Dict:
        """
        紧急异常分析
        
        Args:
            device_sn: 设备SN
            date_str: 日期
            current_data: 当日数据
            historical_data: 历史数据（用于对比）
        
        Returns:
            分析结果
        """
        print(f"\n[Emergency] 启动紧急分析: {device_sn} @ {date_str}")
        
        result = {
            'device_sn': device_sn,
            'date': date_str,
            'analysis_time': datetime.now().isoformat(),
            'trigger_reason': '',
            'comparative_analysis': {},
            'discovered_indicators': [],
            'immediate_actions': []
        }
        
        # 对比分析（异常前 vs 异常后）
        result['comparative_analysis'] = self._comparative_analysis(
            current_data, historical_data
        )
        
        # 快速指标发现
        result['discovered_indicators'] = self._rapid_indicator_discovery(
            device_sn, date_str, result['comparative_analysis']
        )
        
        # 生成即时行动建议
        result['immediate_actions'] = self._generate_immediate_actions(
            device_sn, result['comparative_analysis']
        )
        
        # 保存临时指标
        for indicator in result['discovered_indicators']:
            self._save_temp_indicator(indicator)
        
        print(f"[Emergency] 分析完成，发现 {len(result['discovered_indicators'])} 个临时指标")
        
        return result
    
    def _comparative_analysis(self, current_data: Dict,
                             historical_data: List[Dict]) -> Dict:
        """对比分析：异常前后"""
        # 取最近7天作为"正常期"对比
        normal_period = historical_data[-7:] if len(historical_data) >= 7 else historical_data
        
        analysis = {
            'normal_period_metrics': {},
            'current_metrics': {},
            'significant_deviations': []
        }
        
        # 对比关键指标
        key_indicators = ['voltage_unbalance', 'current_imbalance', 
                         'power_factor', 'efficiency', 'temperature']
        
        for indicator in key_indicators:
            # 正常期统计
            normal_values = [
                d.get(indicator) for d in normal_period
                if d.get(indicator) is not None
            ]
            
            if normal_values:
                normal_mean = np.mean(normal_values)
                normal_std = np.std(normal_values)
            else:
                normal_mean = 0
                normal_std = 0
            
            # 当前值
            current_value = current_data.get(indicator)
            
            analysis['normal_period_metrics'][indicator] = {
                'mean': round(normal_mean, 2),
                'std': round(normal_std, 2)
            }
            
            analysis['current_metrics'][indicator] = current_value
            
            # 显著偏离（超过2个标准差）
            if current_value is not None and normal_std > 0:
                z_score = abs(current_value - normal_mean) / normal_std
                if z_score > 2:
                    analysis['significant_deviations'].append({
                        'indicator': indicator,
                        'current_value': round(current_value, 2),
                        'normal_mean': round(normal_mean, 2),
                        'z_score': round(z_score, 2),
                        'deviation_direction': 'high' if current_value > normal_mean else 'low'
                    })
        
        return analysis
    
    def _rapid_indicator_discovery(self, device_sn: str, date_str: str,
                                   analysis: Dict) -> List[Dict]:
        """快速指标发现"""
        indicators = []
        
        deviations = analysis.get('significant_deviations', [])
        
        for dev in deviations:
            # 基于偏离生成指标
            indicator = {
                'id': f"temp_{device_sn}_{date_str}_{dev['indicator']}",
                'name': f"emergency_{dev['indicator']}_deviation",
                'formula': f"abs({dev['indicator']} - normal_mean) / normal_std",
                'description': f"{dev['indicator']} 偏离正常范围（Z-score: {dev['z_score']})",
                'trigger_event': 'emergency',
                'source_device': device_sn,
                'discovery_date': date_str,
                'is_temporary': True,
                'temp_deadline': (datetime.strptime(date_str, '%Y-%m-%d') + 
                                 timedelta(days=30)).strftime('%Y-%m-%d'),
                'deviation_info': dev
            }
            
            indicators.append(indicator)
        
        # 如果多个指标同时偏离，生成组合指标
        if len(deviations) >= 2:
            combo_indicator = {
                'id': f"temp_{device_sn}_{date_str}_combo",
                'name': f"emergency_composite_{device_sn}",
                'formula': f"composite_deviation_score",
                'description': f"组合偏离指标（{len(deviations)}个指标同时异常）",
                'trigger_event': 'emergency',
                'source_device': device_sn,
                'discovery_date': date_str,
                'is_temporary': True,
                'temp_deadline': (datetime.strptime(date_str, '%Y-%m-%d') + 
                                 timedelta(days=30)).strftime('%Y-%m-%d'),
                'component_deviations': [d['indicator'] for d in deviations]
            }
            
            indicators.append(combo_indicator)
        
        return indicators
    
    def _generate_immediate_actions(self, device_sn: str,
                                   analysis: Dict) -> List[Dict]:
        """生成即时行动建议"""
        actions = []
        
        deviations = analysis.get('significant_deviations', [])
        
        # 根据偏离类型生成建议
        high_priority_indicators = ['voltage_unbalance', 'current_imbalance']
        
        for dev in deviations:
            if dev['indicator'] in high_priority_indicators and dev['z_score'] > 3:
                actions.append({
                    'priority': 'P0',
                    'action': f"立即检查 {dev['indicator']}",
                    'reason': f"Z-score {dev['z_score']:.1f} 严重偏离",
                    'device': device_sn
                })
            elif dev['z_score'] > 2:
                actions.append({
                    'priority': 'P1',
                    'action': f"关注 {dev['indicator']}",
                    'reason': f"Z-score {dev['z_score']:.1f} 显著偏离",
                    'device': device_sn
                })
        
        # 通用建议
        if len(deviations) >= 3:
            actions.append({
                'priority': 'P0',
                'action': "启动全面检查",
                'reason': f"{len(deviations)}个指标同时异常，可能存在系统性故障",
                'device': device_sn
            })
        
        return actions
    
    def _save_temp_indicator(self, indicator: Dict):
        """保存临时指标"""
        temp_indicators = []
        
        if os.path.exists(self.temp_indicators_path):
            with open(self.temp_indicators_path, 'r') as f:
                temp_indicators = json.load(f)
        
        # 检查是否已存在
        exists = any(ti['id'] == indicator['id'] for ti in temp_indicators)
        
        if not exists:
            temp_indicators.append(indicator)
            
            with open(self.temp_indicators_path, 'w') as f:
                json.dump(temp_indicators, f, indent=2)
            
            print(f"[Emergency] 临时指标已保存: {indicator['id']}")
    
    def get_temp_indicators(self, device_sn: Optional[str] = None) -> List[Dict]:
        """获取临时指标"""
        if not os.path.exists(self.temp_indicators_path):
            return []
        
        with open(self.temp_indicators_path, 'r') as f:
            indicators = json.load(f)
        
        if device_sn:
            indicators = [i for i in indicators if i.get('source_device') == device_sn]
        
        return indicators
    
    def clean_expired_temp_indicators(self, date_str: str) -> int:
        """清理过期临时指标"""
        if not os.path.exists(self.temp_indicators_path):
            return 0
        
        with open(self.temp_indicators_path, 'r') as f:
            indicators = json.load(f)
        
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # 过滤未过期的
        valid_indicators = []
        expired_count = 0
        
        for ind in indicators:
            deadline = datetime.strptime(ind.get('temp_deadline', '2099-12-31'), '%Y-%m-%d')
            if current_date <= deadline:
                valid_indicators.append(ind)
            else:
                expired_count += 1
        
        # 保存
        with open(self.temp_indicators_path, 'w') as f:
            json.dump(valid_indicators, f, indent=2)
        
        if expired_count > 0:
            print(f"[Emergency] 清理 {expired_count} 个过期临时指标")
        
        return expired_count


# 便捷函数
def check_emergency_and_analyze(device_sn: str, date_str: str,
                               current_health: Dict,
                               previous_health: Dict,
                               current_data: Dict,
                               historical_data: List[Dict]) -> Optional[Dict]:
    """检查紧急状态并分析"""
    analyzer = EmergencyAnalyzer()
    
    if analyzer.check_emergency_trigger(current_health, previous_health):
        return analyzer.analyze_emergency(device_sn, date_str, current_data, historical_data)
    
    return None