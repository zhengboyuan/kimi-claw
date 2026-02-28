"""
V4.4 周度分析工作流

周期性执行（每周日）：
1. 本周vs上周对比
2. 退化趋势识别
3. 新异常模式挖掘
4. 候选指标生成
5. 推送到验证队列
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_lifecycle_manager import IndicatorLifecycleManager
from core.cross_device_learner import CrossDeviceLearner


class WeeklyAnalysisWorkflow:
    """V4.4 周度分析工作流"""
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.results_base_path = "memory/weekly"
        os.makedirs(self.results_base_path, exist_ok=True)
    
    def run_weekly(self, date_str: str) -> Dict:
        """
        运行周度分析
        
        Args:
            date_str: 周日日期，如 "2025-07-20"
        
        Returns:
            周分析结果
        """
        print(f"\n{'='*80}")
        print(f"V4.4 周度分析: {date_str}")
        print(f"{'='*80}")
        
        # 计算本周和上周的日期范围
        current_week = self._get_week_range(date_str)
        last_week = self._get_week_range(
            (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
        )
        
        result = {
            'date': date_str,
            'week_number': datetime.strptime(date_str, '%Y-%m-%d').isocalendar()[1],
            'current_week': current_week,
            'last_week': last_week,
            'generated_at': datetime.now().isoformat(),
            'degradation_trends': [],
            'new_anomaly_patterns': [],
            'candidate_indicators': [],
            'cross_device_patterns': []
        }
        
        # 阶段1: 本周vs上周健康分对比
        print(f"\n【阶段1】健康分周对比")
        result['degradation_trends'] = self._analyze_degradation(
            current_week, last_week
        )
        
        # 阶段2: 新异常模式挖掘
        print(f"\n【阶段2】异常模式挖掘")
        result['new_anomaly_patterns'] = self._mine_anomaly_patterns(
            current_week, last_week
        )
        
        # 阶段3: 生成候选指标
        print(f"\n【阶段3】生成候选指标")
        result['candidate_indicators'] = self._generate_candidates(
            result['degradation_trends'],
            result['new_anomaly_patterns']
        )
        
        # 阶段4: 跨设备模式提交
        print(f"\n【阶段4】跨设备模式验证")
        result['cross_device_patterns'] = self._submit_cross_device_patterns(
            result['new_anomaly_patterns']
        )
        
        # 保存周分析结果
        self._save_weekly_report(result)
        
        # 输出摘要
        self._print_summary(result)
        
        return result
    
    def _get_week_range(self, sunday_date: str) -> Dict:
        """获取周日期范围"""
        sunday = datetime.strptime(sunday_date, '%Y-%m-%d')
        monday = sunday - timedelta(days=6)
        
        return {
            'start': monday.strftime('%Y-%m-%d'),
            'end': sunday.strftime('%Y-%m-%d'),
            'days': [
                (monday + timedelta(days=i)).strftime('%Y-%m-%d')
                for i in range(7)
            ]
        }
    
    def _analyze_degradation(self, current_week: Dict, last_week: Dict) -> List:
        """分析退化趋势"""
        trends = []
        
        for sn in self.DEVICE_CLUSTER:
            # 读取两周的健康分
            current_scores = self._get_weekly_health_scores(sn, current_week['days'])
            last_scores = self._get_weekly_health_scores(sn, last_week['days'])
            
            if not current_scores or not last_scores:
                continue
            
            # 计算平均健康分
            current_avg = sum(current_scores) / len(current_scores)
            last_avg = sum(last_scores) / len(last_scores)
            
            # 计算变化
            change = current_avg - last_avg
            
            # 识别退化
            if change < -5:  # 下降超过5分
                trends.append({
                    'device': sn,
                    'current_avg': round(current_avg, 1),
                    'last_avg': round(last_avg, 1),
                    'change': round(change, 1),
                    'severity': 'significant' if change < -10 else 'moderate'
                })
        
        # 按退化程度排序
        trends.sort(key=lambda x: x['change'])
        
        return trends
    
    def _mine_anomaly_patterns(self, current_week: Dict, last_week: Dict) -> List:
        """挖掘新异常模式"""
        patterns = []
        
        # 简化实现：对比两周的异常类型
        current_anomalies = self._get_weekly_anomalies(current_week['days'])
        last_anomalies = self._get_weekly_anomalies(last_week['days'])
        
        # 找出本周新出现的异常类型
        current_types = set(a.get('type') for a in current_anomalies)
        last_types = set(a.get('type') for a in last_anomalies)
        
        new_types = current_types - last_types
        
        for anomaly_type in new_types:
            # 统计该类型异常
            type_anomalies = [a for a in current_anomalies if a.get('type') == anomaly_type]
            affected_devices = set(a.get('device') for a in type_anomalies)
            
            patterns.append({
                'type': anomaly_type,
                'count': len(type_anomalies),
                'affected_devices': list(affected_devices),
                'pattern_description': f"{anomaly_type} 在 {len(affected_devices)} 台设备出现"
            })
        
        return patterns
    
    def _generate_candidates(self, degradation_trends: List, 
                            anomaly_patterns: List) -> List:
        """生成候选指标"""
        candidates = []
        manager = IndicatorLifecycleManager()
        
        # 基于退化趋势生成候选
        for trend in degradation_trends:
            if trend['severity'] == 'significant':
                # 显著退化，生成针对性指标
                candidate = {
                    'name': f"degradation_indicator_{trend['device']}",
                    'formula': f"health_score_delta_{trend['device']}",
                    'description': f"{trend['device']} 健康分退化监测",
                    'trigger': 'degradation'
                }
                
                candidate_id = manager.add_candidate(
                    candidate, 
                    trend['device'],
                    'periodic'
                )
                
                candidates.append({
                    'id': candidate_id,
                    'candidate': candidate,
                    'source': trend['device']
                })
        
        # 基于异常模式生成候选
        for pattern in anomaly_patterns:
            if len(pattern['affected_devices']) >= 2:
                # 多设备共同异常，生成通用指标
                candidate = {
                    'name': f"pattern_{pattern['type']}",
                    'formula': f"anomaly_pattern_{pattern['type']}",
                    'description': pattern['pattern_description'],
                    'trigger': 'anomaly_pattern'
                }
                
                # 提交到跨设备验证
                learner = CrossDeviceLearner()
                pattern_id = learner.submit_pattern(
                    candidate,
                    pattern['affected_devices'][0],
                    datetime.now().strftime('%Y-%m-%d')
                )
                
                candidates.append({
                    'id': pattern_id,
                    'candidate': candidate,
                    'source': pattern['affected_devices'][0],
                    'cross_device': True
                })
        
        return candidates
    
    def _submit_cross_device_patterns(self, patterns: List) -> List:
        """提交跨设备模式"""
        submitted = []
        learner = CrossDeviceLearner()
        
        for pattern in patterns:
            if len(pattern['affected_devices']) >= 2:
                pattern_def = {
                    'name': pattern['type'],
                    'description': pattern['pattern_description'],
                    'affected_count': len(pattern['affected_devices'])
                }
                
                pattern_id = learner.submit_pattern(
                    pattern_def,
                    pattern['affected_devices'][0],
                    datetime.now().strftime('%Y-%m-%d')
                )
                
                submitted.append({
                    'pattern_id': pattern_id,
                    'pattern': pattern_def,
                    'devices': pattern['affected_devices']
                })
        
        return submitted
    
    def _get_weekly_health_scores(self, device_sn: str, days: List) -> List:
        """获取设备一周的健康分"""
        scores = []
        health_path = f"memory/devices/{device_sn}/health_history.json"
        
        if not os.path.exists(health_path):
            return scores
        
        with open(health_path, 'r') as f:
            history = json.load(f)
        
        for record in history:
            if record.get('date') in days:
                score = record.get('total_score')
                if score is not None:
                    scores.append(score)
        
        return scores
    
    def _get_weekly_anomalies(self, days: List) -> List:
        """获取一周的异常记录"""
        all_anomalies = []
        
        for sn in self.DEVICE_CLUSTER:
            anomaly_path = f"memory/devices/{sn}/anomaly_log.json"
            
            if not os.path.exists(anomaly_path):
                continue
            
            with open(anomaly_path, 'r') as f:
                device_anomalies = json.load(f)
            
            for a in device_anomalies:
                if a.get('date') in days:
                    a['device'] = sn
                    all_anomalies.append(a)
        
        return all_anomalies
    
    def _save_weekly_report(self, result: Dict):
        """保存周分析报告"""
        week_num = result['week_number']
        year = datetime.strptime(result['date'], '%Y-%m-%d').year
        report_path = f"{self.results_base_path}/{year}-w{week_num:02d}.json"
        
        with open(report_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        print(f"\n周分析报告已保存: {report_path}")
    
    def _print_summary(self, result: Dict):
        """打印摘要"""
        print(f"\n{'='*80}")
        print(f"V4.4 周度分析摘要 (第{result['week_number']}周)")
        print(f"{'='*80}")
        
        print(f"\n退化趋势: {len(result['degradation_trends'])} 台设备")
        for t in result['degradation_trends'][:3]:
            print(f"  {t['device']}: {t['last_avg']} → {t['current_avg']} ({t['change']:+.1f})")
        
        print(f"\n新异常模式: {len(result['new_anomaly_patterns'])} 种")
        for p in result['new_anomaly_patterns']:
            print(f"  {p['type']}: {p['count']} 次, 影响 {len(p['affected_devices'])} 台设备")
        
        print(f"\n候选指标: {len(result['candidate_indicators'])} 个")
        print(f"跨设备模式: {len(result['cross_device_patterns'])} 个")
        
        print(f"\n{'='*80}")


# 便捷函数
def run_v44_weekly_analysis(date_str: str) -> Dict:
    """运行V4.4周度分析"""
    workflow = WeeklyAnalysisWorkflow()
    return workflow.run_weekly(date_str)


if __name__ == '__main__':
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else '2025-07-20'
    run_v44_weekly_analysis(date)