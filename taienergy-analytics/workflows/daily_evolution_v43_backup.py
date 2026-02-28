"""
V4.3 统一进化工作流 (Unified Evolution Workflow)

核心：每日"验证+发现"闭环 + 认知迭代 + 群体智慧
"""
import os
import sys
import json
from datetime import datetime
from typing import Dict
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_discovery_validation import IndicatorDiscoveryValidationEngine
from core.daily_cognitive_iteration import DailyCognitiveIterationEngine
from core.cluster_benchmark import ClusterBenchmarkAnalyzer
from skills.skill_1_data_collector import DataCollector


class UnifiedEvolutionWorkflowV43:
    """V4.3 统一进化工作流"""
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.results_base_path = "memory/evolution_results"
        os.makedirs(self.results_base_path, exist_ok=True)
        
        # 初始化引擎 - 修复参数
        self.indicator_engines = {sn: IndicatorDiscoveryValidationEngine(sn) for sn in self.DEVICE_CLUSTER}
        # DailyCognitiveIterationEngine需要device_cluster参数
        self.cognitive_engine = DailyCognitiveIterationEngine(self.DEVICE_CLUSTER)
        self.cluster_analyzer = ClusterBenchmarkAnalyzer()
    
    def run_daily_evolution(self, date_str: str) -> Dict:
        """运行每日统一进化"""
        print(f"\n{'='*80}")
        print(f"V4.3 统一进化工作流: {date_str}")
        print(f"{'='*80}")
        
        result = {
            'date': date_str,
            'start_time': datetime.now().isoformat(),
            'stage1_data': {},
            'stage2_indicators': {},
            'stage3_cognitive': {},
            'stage4_cluster': {},
            'end_time': None
        }
        
        # 阶段1: 获取数据
        print(f"\n【阶段1】获取16台设备数据")
        device_data = self._collect_device_data(date_str)
        result['stage1_data'] = {'total': 16, 'valid': sum(1 for d in device_data.values() if d is not None)}
        
        # 阶段2: 指标生命周期
        print(f"\n【阶段2】指标生命周期（验证+发现）")
        for sn, df_day in device_data.items():
            if df_day is not None:
                ind_result = self.indicator_engines[sn].validate_daily(df_day, date_str)
                result['stage2_indicators'][sn] = ind_result
        
        # 阶段3: 认知迭代
        print(f"\n【阶段3】每日认知迭代")
        cog_result = self.cognitive_engine.run_daily_iteration(date_str)
        result['stage3_cognitive'] = cog_result
        
        # 阶段4: 群体智慧
        print(f"\n【阶段4】群体智慧分析")
        cluster_result = self.cluster_analyzer.analyze_cluster_daily(device_data, date_str)
        result['stage4_cluster'] = cluster_result
        
        # 保存结果
        result['end_time'] = datetime.now().isoformat()
        result_path = f"{self.results_base_path}/{date_str}_result.json"
        with open(result_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        print(f"\nV4.3 进化完成! 结果保存: {result_path}")
        return result
    
    def _collect_device_data(self, date_str: str) -> Dict:
        """收集16台设备数据"""
        device_data = {}
        for i, sn in enumerate(self.DEVICE_CLUSTER, 1):
            print(f"  [{i}/16] {sn}...", end=' ', flush=True)
            try:
                collector = DataCollector(sn)
                daily_data = collector.collect_daily_data(date_str)
                
                df = {code: data['value'] for code, data in daily_data.items() 
                      if not data.empty and 'value' in data.columns}
                
                if df:
                    device_data[sn] = pd.DataFrame(df)
                    print(f"✅")
                else:
                    device_data[sn] = None
                    print(f"⚠️")
            except Exception as e:
                device_data[sn] = None
                print(f"❌")
        
        return device_data


def run_v43_daily_evolution(date_str: str) -> Dict:
    """便捷函数"""
    workflow = UnifiedEvolutionWorkflowV43()
    return workflow.run_daily_evolution(date_str)
