"""
每日认知迭代引擎 (Daily Cognitive Iteration Engine)

职责：
1. 每日自动分析设备数据
2. 更新设备记忆档案
3. 验证昨日预测
4. 生成新的预测
"""
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np


class DailyCognitiveIterationEngine:
    """
    每日认知迭代引擎
    
    核心循环：分析 → 验证 → 更新 → 预测
    """
    
    def __init__(self, device_cluster: List[str]):
        self.device_cluster = device_cluster
        self.memory_base_path = "memory/devices"
        self.collective_memory_path = "memory/collective"
        
        # 确保目录存在
        os.makedirs(self.memory_base_path, exist_ok=True)
        os.makedirs(self.collective_memory_path, exist_ok=True)
    
    def run_daily_iteration(self, date_str: str) -> Dict:
        """
        运行每日认知迭代
        
        Args:
            date_str: 日期，如 "2025-08-01"
        
        Returns:
            迭代结果报告
        """
        print(f"\n{'='*70}")
        print(f"每日认知迭代: {date_str}")
        print(f"{'='*70}")
        
        results = {
            'date': date_str,
            'devices_processed': 0,
            'predictions_verified': 0,
            'memories_updated': 0,
            'new_patterns_found': 0,
            'alerts': []
        }
        
        # 1. 处理每台设备
        for sn in self.device_cluster:
            try:
                device_result = self._process_device_daily(sn, date_str)
                results['devices_processed'] += 1
                
                if device_result.get('prediction_verified'):
                    results['predictions_verified'] += 1
                
                if device_result.get('memory_updated'):
                    results['memories_updated'] += 1
                
                if device_result.get('new_pattern_found'):
                    results['new_patterns_found'] += 1
                
                if device_result.get('alert'):
                    results['alerts'].append(device_result['alert'])
                
            except Exception as e:
                print(f"  ❌ {sn} 处理失败: {str(e)[:50]}")
        
        # 2. 更新群体记忆
        self._update_collective_memory(date_str)
        
        # 3. 生成迭代报告
        self._save_iteration_report(date_str, results)
        
        print(f"\n{'='*70}")
        print(f"迭代完成:")
        print(f"  处理设备: {results['devices_processed']}/{len(self.device_cluster)}")
        print(f"  验证预测: {results['predictions_verified']}")
        print(f"  更新记忆: {results['memories_updated']}")
        print(f"  新发现模式: {results['new_patterns_found']}")
        print(f"  告警: {len(results['alerts'])}")
        print(f"{'='*70}")
        
        return results
    
    def _process_device_daily(self, sn: str, date_str: str) -> Dict:
        """处理单设备每日迭代"""
        print(f"\n  📊 {sn}")
        
        result = {
            'sn': sn,
            'prediction_verified': False,
            'memory_updated': False,
            'new_pattern_found': False,
            'alert': None
        }
        
        # 1. 加载设备记忆
        memory = self._load_device_memory(sn)
        
        # 2. 获取当日数据
        from skills.skill_1_data_collector import DataCollector
        collector = DataCollector(sn)
        daily_data = collector.collect_daily_data(date_str)
        
        if not daily_data:
            print(f"    ⚠️ 无数据")
            return result
        
        # 3. 分析当日表现
        daily_analysis = self._analyze_daily_performance(daily_data, memory)
        print(f"    效率: {daily_analysis.get('efficiency', 0):.1%}")
        print(f"    异常: {len(daily_analysis.get('anomalies', []))}个")
        
        # 4. 验证昨日预测（如果有）
        yesterday = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_prediction = memory.get('predictions', {}).get(yesterday)
        
        if yesterday_prediction:
            verification = self._verify_prediction(yesterday_prediction, daily_analysis)
            result['prediction_verified'] = True
            print(f"    预测验证: 准确度{verification['accuracy']:.1%}")
            
            # 如果准确度低，记录偏差
            if verification['accuracy'] < 0.7:
                memory['prediction_deviations'] = memory.get('prediction_deviations', [])
                memory['prediction_deviations'].append({
                    'date': date_str,
                    'predicted': yesterday_prediction,
                    'actual': daily_analysis,
                    'accuracy': verification['accuracy']
                })
        
        # 5. 更新设备记忆
        memory['daily_summaries'] = memory.get('daily_summaries', [])
        memory['daily_summaries'].append({
            'date': date_str,
            'analysis': daily_analysis,
            'timestamp': datetime.now().isoformat()
        })
        
        # 只保留最近90天
        if len(memory['daily_summaries']) > 90:
            memory['daily_summaries'] = memory['daily_summaries'][-90:]
        
        # 6. 检测新模式
        new_pattern = self._detect_new_pattern(sn, memory, daily_analysis)
        if new_pattern:
            memory['patterns'] = memory.get('patterns', [])
            memory['patterns'].append(new_pattern)
            result['new_pattern_found'] = True
            print(f"    🆕 新模式: {new_pattern['description'][:50]}")
        
        # 7. 生成明日预测
        tomorrow = (datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        prediction = self._generate_prediction(sn, memory, daily_analysis)
        memory['predictions'] = memory.get('predictions', {})
        memory['predictions'][tomorrow] = prediction
        print(f"    明日预测: 效率{prediction.get('efficiency', 0):.1%}")
        
        # 8. 检查告警
        if daily_analysis.get('anomalies'):
            for anomaly in daily_analysis['anomalies']:
                if anomaly['severity'] in ['warning', 'critical']:
                    result['alert'] = {
                        'sn': sn,
                        'date': date_str,
                        'type': anomaly['type'],
                        'severity': anomaly['severity'],
                        'message': anomaly['message']
                    }
                    print(f"    🚨 告警: {anomaly['message'][:50]}")
        
        # 9. 保存记忆
        self._save_device_memory(sn, memory)
        result['memory_updated'] = True
        
        return result
    
    def _load_device_memory(self, sn: str) -> Dict:
        """加载设备记忆"""
        memory_path = f"{self.memory_base_path}/{sn}/memory.json"
        if os.path.exists(memory_path):
            with open(memory_path, 'r') as f:
                return json.load(f)
        return {
            'sn': sn,
            'created_at': datetime.now().isoformat(),
            'daily_summaries': [],
            'patterns': [],
            'predictions': {},
            'prediction_deviations': []
        }
    
    def _save_device_memory(self, sn: str, memory: Dict):
        """保存设备记忆"""
        device_path = f"{self.memory_base_path}/{sn}"
        os.makedirs(device_path, exist_ok=True)
        
        memory_path = f"{device_path}/memory.json"
        with open(memory_path, 'w') as f:
            json.dump(memory, f, indent=2, default=str)
    
    def _analyze_daily_performance(self, daily_data: Dict, memory: Dict) -> Dict:
        """分析当日表现"""
        import pandas as pd
        
        df = {}
        for code, data in daily_data.items():
            if not data.empty and 'value' in data.columns:
                df[code] = data['value']
        
        if not df:
            return {'efficiency': 0, 'anomalies': []}
        
        df_merged = pd.DataFrame(df)
        
        # 计算效率
        efficiency = 0
        if 'ai45' in df_merged.columns and 'ai56' in df_merged.columns:
            eff = df_merged['ai56'] / df_merged['ai45'].replace(0, np.nan)
            eff = eff[(eff > 0.5) & (eff < 1.0)]
            if not eff.empty:
                efficiency = float(eff.mean())
        
        # 检测异常
        anomalies = []
        
        # 组串离散异常
        pv_cols = ['ai10', 'ai12', 'ai16', 'ai20']
        pv_data = df_merged[[c for c in pv_cols if c in df_merged.columns]].replace(0, np.nan)
        if not pv_data.empty:
            cv = (pv_data.std(axis=1) / pv_data.mean(axis=1)).mean()
            if cv > 0.1:  # 10%变异系数
                anomalies.append({
                    'type': 'string_divergence',
                    'severity': 'warning' if cv < 0.2 else 'critical',
                    'value': float(cv),
                    'message': f'组串离散度异常: {cv:.1%}'
                })
        
        return {
            'efficiency': efficiency,
            'anomalies': anomalies,
            'data_points': len(df_merged)
        }
    
    def _verify_prediction(self, prediction: Dict, actual: Dict) -> Dict:
        """验证预测准确度"""
        predicted_eff = prediction.get('efficiency', 0)
        actual_eff = actual.get('efficiency', 0)
        
        if predicted_eff > 0 and actual_eff > 0:
            error = abs(predicted_eff - actual_eff) / predicted_eff
            accuracy = max(0, 1 - error)
        else:
            accuracy = 0
        
        return {
            'predicted': predicted_eff,
            'actual': actual_eff,
            'error': error if predicted_eff > 0 else 1,
            'accuracy': accuracy
        }
    
    def _detect_new_pattern(self, sn: str, memory: Dict, daily_analysis: Dict) -> Optional[Dict]:
        """检测新模式"""
        summaries = memory.get('daily_summaries', [])
        
        if len(summaries) < 7:
            return None
        
        # 检查连续异常模式
        recent_anomalies = [
            s for s in summaries[-7:]
            if s.get('analysis', {}).get('anomalies')
        ]
        
        if len(recent_anomalies) >= 3:
            # 连续3天有异常，可能是新模式
            return {
                'type': 'consecutive_anomalies',
                'description': f'连续{len(recent_anomalies)}天出现异常',
                'dates': [s['date'] for s in recent_anomalies],
                'detected_at': datetime.now().isoformat()
            }
        
        return None
    
    def _generate_prediction(self, sn: str, memory: Dict, daily_analysis: Dict) -> Dict:
        """生成明日预测"""
        summaries = memory.get('daily_summaries', [])
        
        if len(summaries) >= 7:
            # 基于最近7天平均效率预测
            recent_efficiencies = [
                s['analysis'].get('efficiency', 0)
                for s in summaries[-7:]
            ]
            avg_efficiency = np.mean(recent_efficiencies)
            
            # 简单趋势：如果连续下降，预测继续下降
            if len(recent_efficiencies) >= 3:
                trend = recent_efficiencies[-1] - recent_efficiencies[-3]
                predicted_efficiency = max(0.5, min(0.95, avg_efficiency + trend * 0.3))
            else:
                predicted_efficiency = avg_efficiency
        else:
            # 数据不足，使用当日效率
            predicted_efficiency = daily_analysis.get('efficiency', 0.8)
        
        return {
            'efficiency': float(predicted_efficiency),
            'confidence': min(1.0, len(summaries) / 30),  # 数据越多信心越高
            'generated_at': datetime.now().isoformat()
        }
    
    def _update_collective_memory(self, date_str: str):
        """更新群体记忆"""
        # 加载所有设备记忆
        all_memories = []
        for sn in self.device_cluster:
            memory = self._load_device_memory(sn)
            if memory.get('daily_summaries'):
                all_memories.append(memory)
        
        if len(all_memories) < 2:
            return
        
        # 计算群体统计
        today_efficiencies = []
        for memory in all_memories:
            if memory['daily_summaries']:
                latest = memory['daily_summaries'][-1]
                eff = latest.get('analysis', {}).get('efficiency', 0)
                if eff > 0:
                    today_efficiencies.append(eff)
        
        if today_efficiencies:
            collective_stats = {
                'date': date_str,
                'device_count': len(today_efficiencies),
                'avg_efficiency': float(np.mean(today_efficiencies)),
                'std_efficiency': float(np.std(today_efficiencies)),
                'min_efficiency': float(np.min(today_efficiencies)),
                'max_efficiency': float(np.max(today_efficiencies)),
                'updated_at': datetime.now().isoformat()
            }
            
            # 保存群体记忆
            collective_path = f"{self.collective_memory_path}/daily_stats.json"
            
            # 加载现有数据
            existing_stats = []
            if os.path.exists(collective_path):
                with open(collective_path, 'r') as f:
                    existing_stats = json.load(f)
            
            # 添加新数据
            existing_stats.append(collective_stats)
            
            # 只保留最近90天
            if len(existing_stats) > 90:
                existing_stats = existing_stats[-90:]
            
            with open(collective_path, 'w') as f:
                json.dump(existing_stats, f, indent=2)
    
    def _save_iteration_report(self, date_str: str, results: Dict):
        """保存迭代报告"""
        report_path = f"memory/iteration_reports"
        os.makedirs(report_path, exist_ok=True)
        
        with open(f"{report_path}/{date_str}.json", 'w') as f:
            json.dump(results, f, indent=2)


# 便捷函数
def run_daily_iteration(device_cluster: List[str], date_str: str) -> Dict:
    """
    运行每日认知迭代（便捷函数）
    
    Args:
        device_cluster: 设备SN列表
        date_str: 日期
    
    Returns:
        迭代结果
    """
    engine = DailyCognitiveIterationEngine(device_cluster)
    return engine.run_daily_iteration(date_str)
