"""
资产运营分析 V4.5 - 整合版（含横向对比、时间维度分析、大模型增强）

使用新的 MemorySystem，实现三层数据流动
"""

import os
import sys
from datetime import datetime
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from skills.skill_1_data_collector import DataCollector
from core.asset_health_engine import AssetHealthEngine
from core.maintenance_advisor import MaintenanceAdvisor
from core.memory_system import MemorySystem


class DailyAssetManagementV5:
    """资产运营分析 V4.5 - 整合版"""
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.memory = MemorySystem()
    
    def run(self, date_str: str) -> Dict:
        """运行资产运营分析"""
        print(f"\n{'='*70}")
        print(f"资产运营分析 V4.5: {date_str}")
        print(f"{'='*70}")
        
        # ========== 阶段1: 数据采集 ==========
        print("\n【1/7】数据采集")
        device_data = self._collect_all(date_str)
        
        # ========== 阶段2: 健康评分 + 设备记忆 ==========
        print("\n【2/7】健康评分 + 设备记忆更新")
        device_results = {}
        for sn in self.DEVICE_CLUSTER:
            if device_data.get(sn):
                # 评分
                health = self._calculate_health(sn, date_str, device_data[sn])
                device_results[sn] = health
                
                # 智能更新设备记忆
                self.memory.write_device_memory(sn, {
                    'date': date_str,
                    'health_score': health.get('total_score'),
                    'level': health.get('level'),
                    'dimensions': health.get('dimensions')
                })
            else:
                device_results[sn] = {'health_score': None, 'level': 'offline'}
        
        # ========== 阶段3: 风险识别 ==========
        print("\n【3/7】风险识别")
        risk = self._analyze_risk(device_results)
        
        # ========== 阶段4: 维护建议 ==========
        print("\n【4/7】维护建议")
        advice = {}
        for sn, health in device_results.items():
            if health.get('health_score'):
                advice[sn] = self._generate_advice(sn, date_str, health)
        
        # ========== 阶段5: 横向对比 + 时间维度分析 ==========
        print("\n【5/7】横向对比 + 时间维度分析")
        comparison_result = self._run_horizontal_comparison(device_results, device_data, date_str)
        trend_result = self._run_trend_analysis(device_results, date_str)
        
        # ========== 阶段6: 大模型增强（条件触发）==========
        print("\n【6/7】大模型增强")
        insight_result = None
        if self._should_llm_enhance(comparison_result, trend_result):
            insight_result = self._llm_enhance(comparison_result, trend_result, device_results)
            if insight_result:
                for insight in insight_result.get('insights', []):
                    if insight.get('is_new_discovery'):
                        self.memory.write_comparison_insight({
                            'name': insight['name'],
                            'signature': insight['signature'],
                            'description': insight['description'],
                            'devices_involved': insight['devices_involved'],
                            'evidence': insight['evidence']
                        })
        
        # ========== 阶段7: 指标发现 + 认知层 ==========
        print("\n【7/7】指标发现 + 认知更新")
        discovery = self._run_discovery(date_str, device_data)
        
        # 如果有新发现，更新认知层
        if discovery.get('new_relations'):
            for rel in discovery['new_relations']:
                self.memory.write_relationship(rel)
        
        # ========== 生成日报 ==========
        print("\n【生成日报】")
        report_data = {
            'date': date_str,
            'total_devices': 16,
            'online': risk['online'],
            'avg_health_score': sum(
                d.get('health_score', 0) for d in device_results.values() if d.get('health_score')
            ) / 16,
            'risk_distribution': risk['distribution'],
            'trend_alerts': risk['trend_alerts'],
            'maintenance_priority': self._extract_priority(advice),
            'devices': device_results,
            'candidates_found': discovery.get('candidates_found', 0),
            'top_candidate': discovery.get('top_candidate'),
            # 新增对比洞察相关字段
            'comparison_anomaly': comparison_result.get('is_anomaly', False),
            'trend_analysis_anomaly': trend_result.get('has_anomaly', False),
            'insights_count': insight_result['count'] if insight_result else 0
        }
        
        self.memory.write_daily_report(date_str, report_data)
        
        # 输出摘要
        self._print_summary(date_str, report_data)
        
        return report_data
    
    def _collect_all(self, date_str: str) -> Dict:
        """并发采集"""
        results = {}
        
        def collect(sn):
            try:
                collector = DataCollector(sn)
                data = collector.collect_daily_data(date_str)
                
                # 保留原始DataFrame数据（用于横向对比）
                # 同时提取关键指标（用于指标发现）
                metrics = {}
                for code, df in data.items():
                    if not df.empty and 'value' in df.columns:
                        metrics[code] = df['value'].tolist()
                
                # 返回包含原始DataFrame和提取指标的数据
                return sn, {
                    'raw_data': data,  # 保留原始DataFrame
                    'raw_metrics': metrics,
                    'quality': 70.0
                }
            except Exception as e:
                print(f"  ❌ {sn}: {e}")
                return sn, None
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(collect, sn): sn for sn in self.DEVICE_CLUSTER}
            for future in as_completed(futures):
                sn, data = future.result()
                results[sn] = data
                status = "✅" if data else "❌"
                print(f"  [{len(results)}/16] {sn}...{status}")
        
        return results
    
    def _calculate_health(self, sn: str, date: str, data: Dict) -> Dict:
        """计算健康分"""
        engine = AssetHealthEngine(sn)
        return engine.calculate_daily_health(date, {
            'completeness': 0.95,
            'quality_rating': data.get('quality', 70.0)
        })
    
    def _analyze_risk(self, devices: Dict) -> Dict:
        """风险分析"""
        levels = {'excellent': 0, 'good': 0, 'attention': 0, 
                 'warning': 0, 'danger': 0, 'offline': 0}
        trends = []
        
        for sn, d in devices.items():
            level = d.get('level', 'unknown')
            levels[level] = levels.get(level, 0) + 1
            
            if d.get('trend_score', 0) < -2:
                trends.append(sn)
        
        return {
            'distribution': levels,
            'online': sum(v for k, v in levels.items() if k != 'offline'),
            'trend_alerts': len(trends)
        }
    
    def _generate_advice(self, sn: str, date: str, health: Dict) -> Dict:
        """生成建议"""
        advisor = MaintenanceAdvisor(sn)
        return advisor.generate_advice(date, health)
    
    def _extract_priority(self, advice: Dict) -> list:
        """提取优先级"""
        priority = []
        for sn, a in advice.items():
            level = a.get('recommendation', {}).get('level')
            if level in ['emergency', 'urgent']:
                priority.append({
                    'device': sn,
                    'priority': level,
                    'deadline': a.get('recommendation', {}).get('deadline', 'ASAP')
                })
        return sorted(priority, key=lambda x: {'emergency': 0, 'urgent': 1}.get(x['priority'], 9))[:5]
    
    def _run_discovery(self, date_str: str, device_data: Dict) -> Dict:
        """运行指标发现 - 方案B分层评估"""
        try:
            from core.discovery.pipeline import run_discovery_pipeline

            # 统一三层发现入口（当前默认不接LLM）
            result = run_discovery_pipeline(date_str, device_data, llm_client=None, write_pending=False)
            candidates = result.get("layer1", [])

            # 保存 Layer1 候选（兼容旧候选池）
            saved = 0
            for c in candidates:
                if self.memory.write_candidate(c.name, date_str, c.to_dict()):
                    saved += 1

            # 汇总结果
            l2_count = sum(len(r.get("candidates", {})) for r in result.get("layer2", {}).values())
            l3_count = len(result.get("layer3", {}).get("approved_indicators", []))

            return {
                'candidates_found': len(candidates),
                'candidates_saved': saved,
                'top_candidate': candidates[0].name if candidates else None,
                'layer2_candidates': l2_count,
                'layer3_semantic': l3_count
            }
            
        except Exception as e:
            print(f"  发现失败: {e}")
            import traceback
            traceback.print_exc()
            return {'candidates_found': 0, 'error': str(e)}
    
    def _print_summary(self, date: str, data: Dict):
        """打印摘要"""
        print(f"\n{'='*70}")
        print("日报摘要")
        print(f"{'='*70}")
        print(f"日期: {date}")
        print(f"设备: {data['online']}/16 在线")
        print(f"健康分: {data['avg_health_score']:.1f}")
        
        risk = data['risk_distribution']
        print(f"风险: 🟢{risk.get('good',0)} 🟡{risk.get('attention',0)} 🟠{risk.get('warning',0)} 🔴{risk.get('danger',0)}")
        
        if data.get('trend_alerts'):
            print(f"⚠️ 趋势预警: {data['trend_alerts']} 台")
        
        if data.get('candidates_found'):
            print(f"🔍 发现: {data['candidates_found']} 候选")
        
        # 显示对比洞察
        if data.get('comparison_anomaly'):
            print(f"⚠️ 横向对比异常")
        
        if data.get('trend_analysis_anomaly'):
            print(f"⚠️ 趋势分析异常")
        
        if data.get('insights_count', 0) > 0:
            print(f"💡 生成洞察: {data['insights_count']} 个")
        
        # 显示记忆系统统计
        stats = self.memory.get_stats()
        print(f"\n记忆统计: 日报{stats['daily_reports']} 设备{stats['device_memories']} 关系{stats['relationships']} 候选{stats['candidates']}")
        
        print(f"{'='*70}")
    
    # ========== 横向对比（代码写死）==========
    
    def _run_horizontal_comparison(self, device_results: Dict, device_data: Dict, date_str: str) -> Dict:
        """
        横向对比 - 代码写死
        对比所有设备的功率数据（从原始DataFrame中获取）
        """
        # 从原始数据中获取功率（ai56是有功功率）
        power_data = {}
        for sn, data in device_data.items():
            if data and 'raw_data' in data and 'ai56' in data['raw_data']:
                df = data['raw_data']['ai56']
                if hasattr(df, 'values') and 'value' in df.columns:
                    values = df['value'].dropna().values
                    if len(values) > 0:
                        avg_power = float(values.mean())
                        power_data[sn] = avg_power
        
        if len(power_data) < 2:
            print(f"  ⏭️ 功率数据不足，跳过横向对比")
            return {'devices_compared': len(power_data), 'is_anomaly': False, 'power_gap_pct': 0}
        
        # 功率排名
        power_ranking = sorted(
            [(sn, power) for sn, power in power_data.items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        max_dev = power_ranking[0]
        min_dev = power_ranking[-1]
        power_gap = max_dev[1] - min_dev[1]
        power_gap_pct = (power_gap / max_dev[1] * 100) if max_dev[1] > 0 else 0
        
        # 差异>20%视为异常
        is_anomaly = power_gap_pct > 20
        
        if is_anomaly:
            print(f"  ⚠️ 横向异常: {max_dev[0]}({max_dev[1]:.1f}kW) vs {min_dev[0]}({min_dev[1]:.1f}kW), 差异{power_gap_pct:.1f}%")
        else:
            print(f"  ✅ 横向正常: {len(power_data)}台设备, 最大差异{power_gap_pct:.1f}%")
        
        return {
            'date': date_str,
            'devices_compared': len(power_data),
            'power_ranking': power_ranking,
            'power_gap': power_gap,
            'power_gap_pct': power_gap_pct,
            'is_anomaly': is_anomaly
        }
    
    # ========== 时间维度分析（代码写死）==========
    
    def _run_trend_analysis(self, device_results: Dict, date_str: str) -> Dict:
        """
        时间维度分析 - 代码写死
        对比历史数据，分析趋势
        """
        recent_reports = self.memory.read_recent_reports(days=7)
        
        if not recent_reports:
            print(f"  ⏭️ 无历史数据，跳过趋势分析")
            return {'has_trend': False, 'trend_alerts': [], 'has_anomaly': False}
        
        trend_alerts = []
        
        for sn, data in device_results.items():
            if not data.get('health_score'):
                continue
            
            current_health = data['health_score']
            
            # 找该设备的历史数据
            historical = []
            for report in recent_reports:
                if sn in report.get('devices', {}):
                    historical.append(report['devices'][sn].get('health_score', 0))
            
            if len(historical) >= 3:
                avg_historical = sum(historical) / len(historical)
                change_pct = ((current_health - avg_historical) / avg_historical * 100) if avg_historical > 0 else 0
                
                # 变化>30%视为异常
                if abs(change_pct) > 30:
                    trend_alerts.append({
                        'device': sn,
                        'change_pct': change_pct,
                        'current': current_health,
                        'historical_avg': avg_historical
                    })
                    print(f"  ⚠️ {sn}: 健康分变化 {change_pct:+.1f}%")
        
        if not trend_alerts:
            print(f"  ✅ 趋势正常: 历史{len(recent_reports)}天, 无异常")
        
        return {
            'has_trend': True,
            'historical_days': len(recent_reports),
            'trend_alerts': trend_alerts,
            'has_anomaly': len(trend_alerts) > 0
        }
    
    # ========== 大模型增强（条件触发）==========
    
    def _should_llm_enhance(self, comparison: Dict, trend: Dict) -> bool:
        """判断是否触发大模型增强"""
        triggers = [
            comparison.get('is_anomaly'),           # 横向对比异常
            trend.get('has_anomaly'),                # 趋势异常
            comparison.get('power_gap_pct', 0) > 15,  # 功率差异较大
            len(trend.get('trend_alerts', [])) > 0   # 有趋势告警
        ]
        return any(triggers)
    
    def _llm_enhance(self, comparison: Dict, trend: Dict, device_results: Dict) -> Optional[Dict]:
        """
        大模型增强 - 生成洞察
        （当前为模拟，实际接入LLM）
        """
        print(f"  🤖 生成洞察...")
        
        insights = []
        
        # 基于横向对比生成洞察
        if comparison.get('is_anomaly'):
            max_dev = comparison['power_ranking'][0]
            min_dev = comparison['power_ranking'][-1]
            insights.append({
                'type': 'horizontal_anomaly',
                'name': f"power_gap_{comparison['date']}",
                'signature': f"power_gap_{max_dev[0]}_{min_dev[0]}_{comparison['date']}",
                'summary': f"{max_dev[0]}与{min_dev[0]}功率差异显著",
                'description': f"横向对比发现，{max_dev[0]}平均功率{max_dev[1]:.1f}kW，{min_dev[0]}平均功率{min_dev[1]:.1f}kW，差异达{comparison['power_gap_pct']:.1f}%，建议检查{min_dev[0]}的运行状态",
                'devices_involved': [max_dev[0], min_dev[0]],
                'evidence': {
                    'power_gap': comparison['power_gap'],
                    'power_gap_pct': comparison['power_gap_pct'],
                    'date': comparison['date']
                },
                'is_new_discovery': True
            })
        
        # 基于趋势生成洞察
        for alert in trend.get('trend_alerts', []):
            insights.append({
                'type': 'trend_anomaly',
                'name': f"trend_alert_{alert['device']}",
                'signature': f"trend_alert_{alert['device']}_{comparison['date']}",
                'summary': f"{alert['device']}健康分异常波动",
                'description': f"{alert['device']}当前健康分{alert['current']:.1f}，较历史平均{alert['historical_avg']:.1f}变化{alert['change_pct']:+.1f}%，建议关注",
                'devices_involved': [alert['device']],
                'evidence': alert,
                'is_new_discovery': True
            })
        
        if insights:
            print(f"  ✅ 生成 {len(insights)} 个洞察")
            for i in insights:
                print(f"     - {i['summary']}")
            return {'insights': insights, 'count': len(insights)}
        
        return None


# 便捷函数
def run_daily_v5(date_str: str) -> Dict:
    """运行V4.5每日资产管理"""
    workflow = DailyAssetManagementV5()
    return workflow.run(date_str)


if __name__ == '__main__':
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else '2025-08-15'
    run_daily_v5(date)
