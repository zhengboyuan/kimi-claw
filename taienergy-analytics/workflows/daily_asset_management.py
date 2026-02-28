"""
V4.4 每日资产管理主工作流

整合：
1. 数据采集
2. 健康评分（5维度+趋势分）
3. 风险识别
4. 维护建议生成
5. 资产组合视图输出

指标发现改为周期性触发（周/月），非每日执行
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from skills.skill_1_data_collector import DataCollector
from core.asset_health_engine import AssetHealthEngine
from core.maintenance_advisor import MaintenanceAdvisor


class DailyAssetManagementWorkflow:
    """V4.4 每日资产管理工作流"""
    
    # 16台设备列表
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.results_base_path = "memory/daily"
        os.makedirs(self.results_base_path, exist_ok=True)
    
    def run_daily(self, date_str: str) -> Dict:
        """
        运行每日资产管理
        
        Args:
            date_str: 日期，如 "2025-07-16"
        
        Returns:
            日报结果
        """
        print(f"\n{'='*80}")
        print(f"V4.4 每日资产管理: {date_str}")
        print(f"{'='*80}")
        
        result = {
            'date': date_str,
            'generated_at': datetime.now().isoformat(),
            'device_status': {},
            'risk_summary': {},
            'maintenance_advice': {},
            'portfolio_view': {},
            'next_actions': []
        }
        
        # 阶段1: 采集所有设备数据
        print(f"\n【阶段1】采集16台设备数据")
        all_device_data = self._collect_all_devices(date_str)
        
        # 阶段2: 健康评分
        print(f"\n【阶段2】健康评分（5维度+趋势分）")
        for sn in self.DEVICE_CLUSTER:
            if sn in all_device_data:
                health_result = self._calculate_health(sn, date_str, all_device_data[sn])
                # 保留raw_metrics用于指标发现
                health_result['raw_metrics'] = all_device_data[sn].get('raw_metrics', {})
                result['device_status'][sn] = health_result
            else:
                result['device_status'][sn] = {
                    'date': date_str,
                    'total_score': None,
                    'level': 'offline',
                    'reason': '数据缺失'
                }
        
        # 阶段3: 风险识别
        print(f"\n【阶段3】风险识别")
        result['risk_summary'] = self._analyze_risks(result['device_status'])
        
        # 阶段4: 维护建议
        print(f"\n【阶段4】生成维护建议")
        for sn, health in result['device_status'].items():
            if health.get('total_score') is not None:
                advice = self._generate_advice(sn, date_str, health)
                result['maintenance_advice'][sn] = advice
        
        # 阶段5: 资产组合视图
        print(f"\n【阶段5】资产组合视图")
        result['portfolio_view'] = self._generate_portfolio_view(result)
        
        # 阶段6: 指标自动发现（V4.5新增）
        self._run_indicator_discovery(date_str, all_device_data)
        
        # 阶段7: 确定下一步行动
        result['next_actions'] = self._determine_next_actions(result)
        
        # 保存日报
        self._save_daily_report(result)
        
        # 输出摘要
        self._print_summary(result)
        
        return result
    
    def _collect_all_devices(self, date_str: str) -> Dict:
        """并发采集所有设备数据（4线程）"""
        device_data = {}
        
        print(f"  使用4线程并发采集16台设备...")
        
        def collect_single(sn):
            """采集单台设备"""
            try:
                collector = DataCollector(sn)
                raw_data = collector.collect_daily_data(date_str)
                
                # 提取关键指标用于稳定性计算
                key_metrics = {}
                for code, df in raw_data.items():
                    if not df.empty and 'value' in df.columns:
                        key_metrics[code] = df['value'].tolist()
                
                return sn, {
                    'completeness': 0.95,
                    'quality_rating': 70.0,
                    'raw_metrics': key_metrics,  # 添加原始指标数据
                    'raw_data': raw_data  # 保留完整数据用于退化分析
                }
            except Exception as e:
                print(f"  ❌ {sn}: {str(e)[:30]}")
                return sn, None
        
        # 使用4线程并发采集
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(collect_single, sn): sn for sn in self.DEVICE_CLUSTER}
            
            for i, future in enumerate(as_completed(futures), 1):
                sn, data = future.result()
                device_data[sn] = data
                status = "✅" if data else "❌"
                print(f"  [{i}/16] {sn}...{status}")
        
        return device_data
    
    def _calculate_health(self, device_sn: str, date_str: str, 
                         device_data: Dict) -> Dict:
        """计算单设备健康分"""
        engine = AssetHealthEngine(device_sn)
        
        # 简化数据传递
        simplified_data = {
            'completeness': device_data.get('completeness', 0.95),
            'quality_rating': device_data.get('quality_rating', 70.0)
        }
        
        return engine.calculate_daily_health(date_str, simplified_data)
    
    def _analyze_risks(self, device_status: Dict) -> Dict:
        """分析风险分布"""
        levels = {'excellent': 0, 'good': 0, 'attention': 0, 'warning': 0, 
                 'danger': 0, 'offline': 0}
        
        trend_concerns = []  # 趋势分转负的设备
        
        for sn, status in device_status.items():
            level = status.get('level', 'unknown')
            levels[level] = levels.get(level, 0) + 1
            
            # 检查趋势
            trend = status.get('trend_score', 0)
            if trend < -2:
                trend_concerns.append({
                    'device': sn,
                    'trend_score': trend,
                    'health_score': status.get('total_score', 0)
                })
        
        return {
            'level_distribution': levels,
            'trend_concerns': trend_concerns,
            'total_online': sum(v for k, v in levels.items() if k != 'offline'),
            'total_offline': levels['offline']
        }
    
    def _generate_advice(self, device_sn: str, date_str: str, 
                        health: Dict) -> Dict:
        """生成维护建议"""
        advisor = MaintenanceAdvisor(device_sn)
        return advisor.generate_advice(date_str, health)
    
    def _generate_portfolio_view(self, result: Dict) -> Dict:
        """生成资产组合视图"""
        risk = result['risk_summary']
        
        # 维护优先级排序
        maintenance_priority = []
        for sn, advice in result['maintenance_advice'].items():
            level = advice.get('recommendation', {}).get('level', 'monitor')
            if level in ['emergency', 'urgent']:
                maintenance_priority.append({
                    'device': sn,
                    'priority': level,
                    'deadline': advice.get('recommendation', {}).get('deadline', 'ASAP')
                })
        
        # 按紧急程度排序
        maintenance_priority.sort(
            key=lambda x: {'emergency': 0, 'urgent': 1}.get(x['priority'], 9)
        )
        
        return {
            'total_devices': 16,
            'online': risk.get('total_online', 0),
            'offline': risk.get('total_offline', 0),
            'risk_distribution': risk.get('level_distribution', {}),
            'maintenance_priority': maintenance_priority,
            'trend_alerts': len(risk.get('trend_concerns', []))
        }
    
    def _determine_next_actions(self, result: Dict) -> List:
        """确定下一步行动"""
        actions = []
        
        # 紧急处理
        emergency_devices = [
            d for d, a in result['maintenance_advice'].items()
            if a.get('recommendation', {}).get('level') == 'emergency'
        ]
        if emergency_devices:
            actions.append({
                'priority': 'P0',
                'action': '紧急处理',
                'devices': emergency_devices,
                'deadline': '48小时内'
            })
        
        # 趋势预警
        trend_concerns = result['risk_summary'].get('trend_concerns', [])
        if trend_concerns:
            actions.append({
                'priority': 'P1',
                'action': '趋势预警关注',
                'devices': [t['device'] for t in trend_concerns],
                'note': '健康分连续下降'
            })
        
        # 周分析触发检查
        # 如果是周日，建议触发周分析
        date = datetime.strptime(result['date'], '%Y-%m-%d')
        if date.weekday() == 6:  # 周日
            actions.append({
                'priority': 'P2',
                'action': '触发周度分析',
                'trigger': 'weekly_analysis'
            })
        
        return actions
    
    def _save_daily_report(self, result: Dict):
        """保存日报"""
        report_path = f"{self.results_base_path}/{result['date']}_report.json"
        with open(report_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n日报已保存: {report_path}")
    
    def _print_summary(self, result: Dict):
        """打印摘要"""
        print(f"\n{'='*80}")
        print("V4.4 资产管理日报摘要")
        print(f"{'='*80}")
        
        portfolio = result['portfolio_view']
        print(f"\n设备状态: 在线 {portfolio['online']}/16, 离线 {portfolio['offline']}/16")
        
        risk = portfolio['risk_distribution']
        print(f"\n风险分布:")
        print(f"  🟢 优秀/良好: {risk.get('excellent', 0) + risk.get('good', 0)} 台")
        print(f"  🟡 关注: {risk.get('attention', 0)} 台")
        print(f"  🟠 预警: {risk.get('warning', 0)} 台")
        print(f"  🔴 危险: {risk.get('danger', 0)} 台")
        
        if portfolio['trend_alerts'] > 0:
            print(f"\n⚠️ 趋势预警: {portfolio['trend_alerts']} 台设备退化加速")
        
        if portfolio['maintenance_priority']:
            print(f"\n维护优先级:")
            for item in portfolio['maintenance_priority'][:3]:
                print(f"  {item['priority']}: {item['device']} (截止: {item['deadline']})")
        
        print(f"\n{'='*80}")
    
    def _run_indicator_discovery(self, date_str: str, device_data: Dict):
        """
        V4.5.0 每日指标发现（新增）
        """
        print(f"\n【阶段6】指标自动发现（V4.5）")
        
        # 调试：检查数据
        sample_sn = list(device_data.keys())[0] if device_data else None
        if sample_sn:
            sample_data = device_data[sample_sn]
            has_raw = 'raw_metrics' in sample_data
            print(f"  调试: 样本设备 {sample_sn}, 有raw_metrics={has_raw}")
        
        try:
            from core.daily_discovery import IndicatorDiscovery, run_discovery
            from core.business_rule_filter import filter_candidates
            
            # 运行发现
            discovery = IndicatorDiscovery()
            candidates = discovery.scan_daily(date_str, device_data)
            
            # 业务过滤
            valid_candidates = filter_candidates(candidates)
            
            # 只取Top-5
            top_candidates = valid_candidates[:5]
            
            # 保存到候选池
            for c in top_candidates:
                discovery.save_to_candidate_pool(c)
            
            print(f"  发现 {len(candidates)} 个候选，通过过滤 {len(valid_candidates)} 个，保存Top-{len(top_candidates)}")
            
            # 触发Ralph处理（异步）
            if top_candidates:
                self._trigger_ralph_processing()
            
        except Exception as e:
            print(f"  指标发现失败: {e}")
    
    def _trigger_ralph_processing(self):
        """触发Ralph处理候选池"""
        # 简化版：只打印提示，实际应该启动后台进程
        print(f"  已触发Ralph处理候选池（请手动运行: python core/ralph_runner.py）")


# 便捷函数
def run_v44_daily_management(date_str: str) -> Dict:
    """运行V4.4每日资产管理"""
    workflow = DailyAssetManagementWorkflow()
    return workflow.run_daily(date_str)


if __name__ == '__main__':
    # 测试运行
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else '2025-07-16'
    run_v44_daily_management(date)