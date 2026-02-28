"""
V4.4 每日资产管理主工作流 - 精简版

核心改变：
1. 日报不再包含 raw_metrics（从400MB降到<50KB）
2. 使用 SmartMemoryWriter 进行智能写入
3. 设备记忆使用智能决策（有变化才更新）
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
from core.smart_memory import SmartMemoryWriter, MemoryGuard


class DailyAssetManagementWorkflowV2:
    """V4.4 每日资产管理 - 精简版"""
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self, llm_client=None):
        self.results_base_path = "memory/daily"
        os.makedirs(self.results_base_path, exist_ok=True)
        
        # 初始化智能记忆写入器
        self.smart_memory = SmartMemoryWriter(llm_client)
        self.memory_guard = MemoryGuard()
    
    def run_daily(self, date_str: str) -> Dict:
        """运行每日资产管理 - 精简版"""
        print(f"\n{'='*80}")
        print(f"V4.4 每日资产管理 (精简版): {date_str}")
        print(f"{'='*80}")
        
        # ========== 阶段1: 采集数据 ==========
        print(f"\n【阶段1】采集16台设备数据")
        all_device_data = self._collect_all_devices(date_str)
        
        # ========== 阶段2: 健康评分 + 设备记忆更新 ==========
        print(f"\n【阶段2】健康评分 + 智能记忆更新")
        device_summaries = {}
        
        for sn in self.DEVICE_CLUSTER:
            if sn in all_device_data and all_device_data[sn]:
                # 计算健康评分
                health = self._calculate_health(sn, date_str, all_device_data[sn])
                
                # 智能决策：是否更新设备记忆
                self._smart_update_device_memory(sn, health)
                
                # 只保留摘要（不包含raw_metrics）
                device_summaries[sn] = self._create_device_summary(health)
            else:
                device_summaries[sn] = {
                    'health_score': None,
                    'level': 'offline',
                    'reason': '数据缺失'
                }
        
        # ========== 阶段3: 风险识别 ==========
        print(f"\n【阶段3】风险识别")
        risk_summary = self._analyze_risks(device_summaries)
        
        # ========== 阶段4: 维护建议 ==========
        print(f"\n【阶段4】生成维护建议")
        maintenance_advice = {}
        for sn, summary in device_summaries.items():
            if summary.get('health_score') is not None:
                advice = self._generate_advice(sn, date_str, summary)
                maintenance_advice[sn] = advice
        
        # ========== 阶段5: 资产组合视图 ==========
        print(f"\n【阶段5】资产组合视图")
        portfolio_view = self._generate_portfolio_view(
            device_summaries, risk_summary, maintenance_advice
        )
        
        # ========== 阶段6: 指标发现（使用raw_metrics后立即释放） ==========
        discovery_summary = self._run_indicator_discovery(date_str, all_device_data)
        
        # ========== 阶段7: 生成精简日报 ==========
        print(f"\n【阶段7】生成精简日报")
        report = self._create_compact_report(
            date_str, device_summaries, risk_summary, 
            maintenance_advice, portfolio_view, discovery_summary
        )
        
        # 保存日报
        self._save_daily_report(report)
        
        # 输出摘要
        self._print_summary(report)
        
        return report
    
    def _collect_all_devices(self, date_str: str) -> Dict:
        """并发采集所有设备数据"""
        device_data = {}
        
        print(f"  使用4线程并发采集...")
        
        def collect_single(sn):
            try:
                collector = DataCollector(sn)
                raw_data = collector.collect_daily_data(date_str)
                
                # 提取关键指标
                key_metrics = {}
                for code, df in raw_data.items():
                    if not df.empty and 'value' in df.columns:
                        key_metrics[code] = df['value'].tolist()
                
                return sn, {
                    'completeness': 0.95,
                    'quality_rating': 70.0,
                    'raw_metrics': key_metrics,  # 仅用于指标发现，不保存
                }
            except Exception as e:
                print(f"  ❌ {sn}: {str(e)[:30]}")
                return sn, None
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(collect_single, sn): sn 
                      for sn in self.DEVICE_CLUSTER}
            
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
        
        simplified_data = {
            'completeness': device_data.get('completeness', 0.95),
            'quality_rating': device_data.get('quality_rating', 70.0)
        }
        
        return engine.calculate_daily_health(date_str, simplified_data)
    
    def _smart_update_device_memory(self, sn: str, health: Dict):
        """智能更新设备记忆（有变化才更新）"""
        memory_path = f"memory/devices/{sn}/memory.json"
        
        # 准备新数据
        new_memory = {
            'sn': sn,
            'date': health.get('date'),
            'health_score': health.get('total_score'),
            'level': health.get('level'),
            'dimensions': health.get('dimensions'),
            'anomaly': health.get('level') in ['warning', 'danger'],
            'timestamp': datetime.now().isoformat()
        }
        
        # 使用SmartMemoryWriter决策
        context = {'sn': sn, 'type': 'device_memory'}
        written = self.smart_memory.write(memory_path, new_memory, context)
        
        if written:
            print(f"    {sn}: 记忆已更新 (评分{health.get('total_score')})")
        else:
            print(f"    {sn}: 记忆无变化，跳过")
    
    def _create_device_summary(self, health: Dict) -> Dict:
        """创建设备摘要（不包含原始数据）"""
        return {
            'health_score': health.get('total_score'),
            'level': health.get('level'),
            'dimensions': health.get('dimensions'),
            'trend_score': health.get('trend_score'),
            'calibrated': health.get('calibrated')
        }
    
    def _analyze_risks(self, device_summaries: Dict) -> Dict:
        """分析风险分布"""
        levels = {'excellent': 0, 'good': 0, 'attention': 0, 
                 'warning': 0, 'danger': 0, 'offline': 0}
        trend_concerns = []
        
        for sn, summary in device_summaries.items():
            level = summary.get('level', 'unknown')
            levels[level] = levels.get(level, 0) + 1
            
            trend = summary.get('trend_score', 0)
            if trend < -2:
                trend_concerns.append({
                    'device': sn,
                    'trend_score': trend,
                    'health_score': summary.get('health_score')
                })
        
        return {
            'level_distribution': levels,
            'trend_concerns': trend_concerns,
            'total_online': sum(v for k, v in levels.items() if k != 'offline'),
            'total_offline': levels['offline']
        }
    
    def _generate_advice(self, device_sn: str, date_str: str, 
                        summary: Dict) -> Dict:
        """生成维护建议"""
        advisor = MaintenanceAdvisor(device_sn)
        
        # 构造伪health对象（兼容原有接口）
        pseudo_health = {
            'total_score': summary.get('health_score'),
            'level': summary.get('level'),
            'dimensions': summary.get('dimensions')
        }
        
        return advisor.generate_advice(date_str, pseudo_health)
    
    def _generate_portfolio_view(self, device_summaries: Dict,
                                  risk_summary: Dict,
                                  maintenance_advice: Dict) -> Dict:
        """生成资产组合视图"""
        # 维护优先级
        maintenance_priority = []
        for sn, advice in maintenance_advice.items():
            level = advice.get('recommendation', {}).get('level', 'monitor')
            if level in ['emergency', 'urgent']:
                maintenance_priority.append({
                    'device': sn,
                    'priority': level,
                    'deadline': advice.get('recommendation', {}).get('deadline', 'ASAP')
                })
        
        maintenance_priority.sort(
            key=lambda x: {'emergency': 0, 'urgent': 1}.get(x['priority'], 9)
        )
        
        return {
            'total_devices': 16,
            'online': risk_summary.get('total_online', 0),
            'offline': risk_summary.get('total_offline', 0),
            'avg_health_score': sum(
                s.get('health_score', 0) for s in device_summaries.values() 
                if s.get('health_score')
            ) / 16,
            'risk_distribution': risk_summary.get('level_distribution', {}),
            'maintenance_priority': maintenance_priority[:5],  # 只保留前5
            'trend_alerts': len(risk_summary.get('trend_concerns', []))
        }
    
    def _run_indicator_discovery(self, date_str: str, device_data: Dict) -> Dict:
        """运行指标发现 - 精简版"""
        print(f"\n【阶段6】指标自动发现（V4.5）")
        
        try:
            from core.daily_discovery import IndicatorDiscovery
            from core.business_rule_filter import filter_candidates
            
            discovery = IndicatorDiscovery()
            
            # 运行发现（使用raw_metrics）
            candidates = discovery.scan_daily(date_str, device_data)
            
            # 业务过滤
            valid_candidates = filter_candidates(candidates)
            
            # 只取Top-3
            top_candidates = valid_candidates[:3]
            
            # 智能决策：是否保存候选
            saved_count = 0
            for c in top_candidates:
                candidate_path = f"memory/indicators/candidate/{c['name']}_{date_str}.md"
                
                # 检查是否已存在相同候选
                written = self.smart_memory.write(candidate_path, c, {
                    'type': 'candidate',
                    'date': date_str
                })
                
                if written:
                    saved_count += 1
            
            print(f"  发现 {len(candidates)} 个，过滤后 {len(valid_candidates)} 个，保存 {saved_count} 个")
            
            return {
                'candidates_found': len(candidates),
                'candidates_filtered': len(valid_candidates),
                'candidates_saved': saved_count,
                'top_candidate': top_candidates[0]['name'] if top_candidates else None
            }
            
        except Exception as e:
            print(f"  指标发现失败: {e}")
            return {'candidates_found': 0, 'error': str(e)}
    
    def _create_compact_report(self, date_str: str,
                                device_summaries: Dict,
                                risk_summary: Dict,
                                maintenance_advice: Dict,
                                portfolio_view: Dict,
                                discovery_summary: Dict) -> Dict:
        """创建精简日报（目标<50KB）"""
        return {
            'date': date_str,
            'generated_at': datetime.now().isoformat(),
            
            # 组合摘要（聚合数据）
            'portfolio': {
                'total_devices': portfolio_view['total_devices'],
                'online': portfolio_view['online'],
                'avg_health_score': round(portfolio_view['avg_health_score'], 1),
                'risk_distribution': portfolio_view['risk_distribution'],
                'trend_alerts': portfolio_view['trend_alerts'],
                'maintenance_priority': portfolio_view['maintenance_priority']
            },
            
            # 设备摘要（只保留关键字段）
            'devices': {
                sn: {
                    'health_score': s.get('health_score'),
                    'level': s.get('level'),
                    'trend_score': s.get('trend_score')
                }
                for sn, s in device_summaries.items()
            },
            
            # 指标发现摘要
            'discovery': discovery_summary,
            
            # 注意：不包含 raw_metrics！
        }
    
    def _save_daily_report(self, report: Dict):
        """保存精简日报"""
        report_path = f"{self.results_base_path}/{report['date']}_report.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # 检查文件大小
        size_kb = os.path.getsize(report_path) / 1024
        print(f"\n✅ 日报已保存: {report_path}")
        print(f"   文件大小: {size_kb:.1f} KB")
        
        if size_kb > 50:
            print(f"   ⚠️ 警告: 超过50KB目标！")
    
    def _print_summary(self, report: Dict):
        """打印摘要"""
        print(f"\n{'='*80}")
        print("V4.4 资产管理日报摘要")
        print(f"{'='*80}")
        
        portfolio = report['portfolio']
        print(f"\n设备状态: 在线 {portfolio['online']}/16")
        print(f"平均健康分: {portfolio['avg_health_score']}")
        
        risk = portfolio['risk_distribution']
        print(f"\n风险分布:")
        print(f"  🟢 优秀/良好: {risk.get('excellent', 0) + risk.get('good', 0)} 台")
        print(f"  🟡 关注: {risk.get('attention', 0)} 台")
        print(f"  🟠 预警: {risk.get('warning', 0)} 台")
        print(f"  🔴 危险: {risk.get('danger', 0)} 台")
        
        if portfolio['trend_alerts'] > 0:
            print(f"\n⚠️ 趋势预警: {portfolio['trend_alerts']} 台")
        
        discovery = report.get('discovery', {})
        if discovery.get('candidates_found', 0) > 0:
            print(f"\n🔍 指标发现: {discovery['candidates_found']} 个候选")
        
        print(f"\n{'='*80}")


# 便捷函数
def run_v44_daily_management_v2(date_str: str, llm_client=None) -> Dict:
    """运行V4.4每日资产管理 - 精简版"""
    workflow = DailyAssetManagementWorkflowV2(llm_client)
    return workflow.run_daily(date_str)


if __name__ == '__main__':
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else '2025-08-15'
    run_v44_daily_management_v2(date)
