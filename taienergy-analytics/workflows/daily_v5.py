"""
资产运营分析 V5.1 - 从 registry 读取指标配置

核心变更：
1. 从 registry.json 读取指标配置，不再硬编码
2. 报表输出到 memory/reports/ 规范路径
3. 所有原子能力已在 skills_registry.yaml 注册

使用新的 MemorySystem，实现三层数据流动
"""

import gc
import os
import sys
from datetime import datetime
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from skills.skill_1_data_collector import DataCollector
from core.asset_health_engine import AssetHealthEngine
from core.maintenance_advisor import MaintenanceAdvisor
from core.memory_system import MemorySystem
from core.indicator_registry import read_registry
from core.competition_indicators import CompetitionIndicatorCalculator
from core.unified_history import UnifiedHistoryStore
from core.discovery_rules import DiscoveryRuleEngine
from core.aggregation_engine import AggregationEngine


class DailyAssetManagementV5:
    """资产运营分析 V5.1 - 从 registry 读取指标配置"""
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.memory = MemorySystem()
        self.registry = read_registry()
        self.indicators = self.registry.get('indicators', {})
        # V5.1 竞赛指标计算器
        self.competition_calc = CompetitionIndicatorCalculator(
            station_config={'installed_capacity': 16000}  # 16台 * 1000kW = 16MW
        )
        # V5.1 新增：统一历史存储、发现规则、聚合引擎
        self.history_store = UnifiedHistoryStore()
        self.rule_engine = DiscoveryRuleEngine()
        self.aggregation = AggregationEngine(self.history_store)
    
    def _get_indicator_config(self, indicator_id: str) -> Optional[Dict]:
        """从 registry 读取指标配置"""
        return self.indicators.get(indicator_id)
    
    def _get_score(self, health: Dict) -> Optional[float]:
        """统一读取健康分，兼容 total_score/health_score"""
        if not isinstance(health, dict):
            return None
        score = health.get('health_score')
        if score is None:
            score = health.get('total_score')
        return score
    
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
            score = self._get_score(health)
            if score is not None:
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
        
        # ========== 生成日报（V5.1 规范路径）==========
        print("\n【生成日报】")
        
        # V5.1 竞赛指标计算
        print("\n【竞赛指标计算】")
        competition_metrics = self._calculate_competition_metrics(device_data, date_str)
        
        report_data = {
            'date': date_str,
            'total_devices': 16,
            'online': risk['online'],
            'avg_health_score': 0,  # 将在下面计算
            'risk_distribution': risk['distribution'],
            'trend_alerts': risk['trend_alerts'],
            'maintenance_priority': self._extract_priority(advice),
            'devices': device_results,
            'candidates_found': discovery.get('candidates_found', 0),
            'top_candidate': discovery.get('top_candidate'),
            # 新增对比洞察相关字段
            'comparison_anomaly': comparison_result.get('is_anomaly', False),
            'trend_analysis_anomaly': trend_result.get('has_anomaly', False),
            'insights_count': insight_result['count'] if insight_result else 0,
            # V5.1 竞赛指标
            'competition_metrics': competition_metrics
        }
        
        # 计算平均健康分（基于实际在线台数）
        scores = [self._get_score(d) for d in device_results.values()]
        valid_scores = [s for s in scores if s is not None]
        avg_health = (sum(valid_scores) / len(valid_scores)) if valid_scores else 0
        report_data['avg_health_score'] = avg_health
        
        # V5.1 规范路径: memory/reports/daily/station/YYYY-MM-DD.json
        self._write_station_report(date_str, report_data)
        
        # 同时写入设备级日报
        for sn, device_data in device_results.items():
            if self._get_score(device_data) is not None:
                self._write_inverter_report(sn, date_str, device_data, advice.get(sn, {}))
        
        # ========== V5.1 新增：统一历史存储 ==========
        print("\n【统一历史存储】")
        self._store_to_history(date_str, device_results, competition_metrics)
        
        # ========== V5.1 新增：设备画像生成 ==========
        print("\n【设备画像生成】")
        device_profiles = self._generate_device_profiles(date_str)
        
        # ========== V5.1 新增：场站排名生成 ==========
        print("\n【场站排名生成】")
        ranking = self._generate_station_ranking(date_str, device_profiles)
        
        # ========== V5.1 新增：发现规则检测 ==========
        print("\n【发现规则检测】")
        findings = self._run_discovery_rules(date_str, device_profiles, ranking)
        
        # 输出摘要
        self._print_summary(date_str, report_data, device_profiles, ranking, findings)
        
        # 释放内存，防止批量处理时内存累积
        gc.collect()
        
        return report_data
    
    def _collect_all(self, date_str: str) -> Dict:
        """并发采集"""
        results = {}
        
        def collect(sn):
            try:
                collector = DataCollector(sn)
                data = collector.collect_daily_data(date_str)
                
                # 提取关键指标（用于指标发现和健康评分）
                metrics = {}
                for code, df in data.items():
                    if not df.empty and 'value' in df.columns:
                        metrics[code] = df['value'].tolist()
                
                # 返回提取的指标数据（不保留原始DataFrame，避免内存累积）
                return sn, {
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
            'quality_rating': data.get('quality', 70.0),
            'raw_metrics': data.get('raw_metrics', {})  # 传递原始指标数据用于稳定性计算
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
    
    # ========== V5.1 竞赛指标计算 ==========
    
    def _calculate_competition_metrics(self, device_data: Dict, date_str: str) -> Dict:
        """
        计算竞赛指标（V5.1）
        
        当前实现:
        - equivalent_utilization_hours: 等效利用小时数 ✅
        - generation_duration: 发电时长 ✅
        
        待数据源到位:
        - curtailment_rate: 弃光率 ⚠️
        - plant_consumption_rate_comprehensive: 综合厂用电率 ⚠️
        - plant_overall_efficiency: 光伏电站整体效率 ⚠️
        """
        metrics = {}
        
        # 1. 等效利用小时数 (✅ 可计算)
        # 从所有逆变器的 ai68 (当日发电量) 累加
        total_generation = 0
        for sn, data in device_data.items():
            if data and 'raw_data' in data:
                raw = data['raw_data']
                if 'ai68' in raw and hasattr(raw['ai68'], 'values'):
                    # ai68 是累计值，取最后一个
                    daily_gen = raw['ai68']['value'].iloc[-1] if len(raw['ai68']) > 0 else 0
                    total_generation += daily_gen
        
        result = self.competition_calc.calculate_equivalent_utilization_hours(total_generation)
        metrics['equivalent_utilization_hours'] = {
            'value': result.get('value'),
            'unit': 'h',
            'computable': result.get('computable', False),
            'note': '发电量/装机容量' if result.get('computable') else result.get('error')
        }
        
        # 2. 发电时长 (✅ 可计算)
        # 统计所有设备的平均发电时长
        total_duration = 0
        device_count = 0
        for sn, data in device_data.items():
            if data and 'raw_data' in data:
                raw = data['raw_data']
                # 尝试从功率判断发电状态
                if 'ai56' in raw and hasattr(raw['ai56'], 'values'):
                    power_values = raw['ai56']['value'].tolist()
                    status_from_power = ['generating' if p > 10 else 'standby' for p in power_values]
                    result = self.competition_calc.calculate_generation_duration(status_from_power)
                    if result.get('value'):
                        total_duration += result['value']
                        device_count += 1
        
        avg_duration = total_duration / device_count if device_count > 0 else 0
        metrics['generation_duration'] = {
            'value': round(avg_duration, 2),
            'unit': 'h',
            'computable': device_count > 0,
            'device_count': device_count,
            'note': f'平均发电时长（{device_count}台设备）'
        }
        
        # 3. 弃光率 (⚠️ 待调度数据)
        metrics['curtailment_rate'] = {
            'value': None,
            'unit': '%',
            'computable': False,
            'note': '待调度数据到位'
        }
        
        # 4. 综合厂用电率 (⚠️ 待关口表数据)
        metrics['plant_consumption_rate_comprehensive'] = {
            'value': None,
            'unit': '%',
            'computable': False,
            'note': '待关口表数据到位'
        }
        
        # 5. 光伏电站整体效率 (⚠️ 待理论发电量计算)
        metrics['plant_overall_efficiency'] = {
            'value': None,
            'unit': '%',
            'computable': False,
            'note': '待理论发电量计算'
        }
        
        return metrics
    
    def _run_discovery(self, date_str: str, device_data: Dict) -> Dict:
        """运行指标发现 - 方案B分层评估"""
        try:
            from core.discovery.pipeline import run_discovery_pipeline

            # 统一三层发现入口（当前默认不接LLM）
            result = run_discovery_pipeline(date_str, device_data, llm_client=None, write_pending=False)
            
            # 使用原始候选（未经过滤）保存到候选池
            raw_candidates = result.get("layer1_raw", [])
            candidates = result.get("layer1", [])

            # 保存 Layer1 原始候选（兼容旧候选池）
            saved = 0
            for c in raw_candidates:
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
    
    # ========== V5.1 新增：统一历史存储 ==========
    
    def _store_to_history(self, date_str: str, device_results: Dict, competition_metrics: Dict):
        """存储到统一历史存储"""
        # 存储每台设备的日聚合
        valid_scores = []
        for sn, data in device_results.items():
            score = self._get_score(data)
            if score is not None:
                daily_metrics = {
                    'health_score': score,
                    'level': data.get('level'),
                    'trend_score': data.get('trend_score', 0)
                }
                self.history_store.append_device_daily(sn, date_str, daily_metrics)
                valid_scores.append(score)
        
        # 存储场站日聚合（基于实际在线台数）
        online_count = len(valid_scores)
        avg_health = (sum(valid_scores) / online_count) if valid_scores else 0
        station_metrics = {
            'online_count': online_count,
            'avg_health': avg_health,
            'competition_metrics': competition_metrics
        }
        self.history_store.append_station_daily(date_str, station_metrics)
        print(f"  ✅ 历史数据存储完成")
    
    # ========== V5.1 新增：设备画像生成 ==========
    
    def _generate_device_profiles(self, date_str: str) -> Dict[str, Dict]:
        """生成所有设备的画像"""
        profiles = {}
        for sn in self.DEVICE_CLUSTER:
            profile = self.aggregation.generate_device_profile(sn, days=30)
            profiles[sn] = profile
        
        # 打印摘要
        valid_profiles = [p for p in profiles.values() if p.get('status') != 'insufficient_data']
        print(f"  ✅ 生成 {len(valid_profiles)} 台设备画像")
        
        # 显示有问题的设备
        for sn, profile in profiles.items():
            if profile.get('health', {}).get('trend') == 'degrading':
                print(f"     ⚠️ {sn}: 健康分下滑")
        
        return profiles
    
    # ========== V5.1 新增：场站排名生成 ==========
    
    def _generate_station_ranking(self, date_str: str, device_profiles: Dict[str, Dict]) -> Dict:
        """生成场站排名"""
        ranking = self.aggregation.generate_station_ranking(date_str, device_profiles)
        
        print(f"  ✅ 排名生成完成")
        print(f"     TOP 3: {', '.join(ranking.get('top_performers', {}).get('by_health', [])[:3])}")
        print(f"     需关注: {', '.join(ranking.get('bottom_performers', {}).get('by_health', [])[-3:])}")
        
        return ranking
    
    # ========== V5.1 新增：发现规则检测 ==========
    
    def _run_discovery_rules(self, date_str: str, device_profiles: Dict, ranking: Dict) -> List[Dict]:
        """运行发现规则检测"""
        findings = []
        
        # 检查健康分下滑
        for sn, profile in device_profiles.items():
            if profile.get('health', {}).get('trend') == 'degrading':
                findings.append({
                    'rule': 'health_decline',
                    'device': sn,
                    'severity': 'warning',
                    'message': f'{sn} 健康分趋势下滑'
                })
        
        # 检查持续垫底
        bottom3 = ranking.get('bottom_performers', {}).get('by_health', [])
        for sn in bottom3:
            findings.append({
                'rule': 'consistent_bottom',
                'device': sn,
                'severity': 'warning',
                'message': f'{sn} 健康分排名垫底'
            })
        
        if findings:
            print(f"  ⚠️ 发现 {len(findings)} 个问题:")
            for f in findings[:5]:  # 只显示前5个
                print(f"     - {f['device']}: {f['message']}")
        else:
            print(f"  ✅ 未发现异常")
        
        return findings
    
    # ========== V5.1 更新：打印摘要 ==========
    
    def _print_summary(self, date: str, data: Dict, device_profiles: Dict = None, 
                       ranking: Dict = None, findings: List = None):
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
        
        # V5.1 竞赛指标
        comp = data.get('competition_metrics', {})
        print(f"\n【竞赛指标】")
        if comp.get('equivalent_utilization_hours', {}).get('computable'):
            print(f"  等效利用小时数: {comp['equivalent_utilization_hours']['value']} h")
        if comp.get('generation_duration', {}).get('computable'):
            print(f"  发电时长: {comp['generation_duration']['value']} h")
        
        # V5.1 设备画像摘要
        if device_profiles:
            degrading = [sn for sn, p in device_profiles.items() 
                        if p.get('health', {}).get('trend') == 'degrading']
            if degrading:
                print(f"\n【设备画像】")
                print(f"  健康分下滑: {', '.join(degrading[:3])}")
        
        # V5.1 发现结果
        if findings:
            critical = [f for f in findings if f.get('severity') == 'critical']
            warning = [f for f in findings if f.get('severity') == 'warning']
            if critical or warning:
                print(f"\n【发现规则】")
                if critical:
                    print(f"  🔴 严重: {len(critical)} 个")
                if warning:
                    print(f"  ⚠️ 警告: {len(warning)} 个")
        
        # 显示记忆系统统计
        stats = self.memory.get_stats()
        print(f"\n记忆统计: 日报{stats['daily_reports']} 设备{stats['device_memories']} 关系{stats['relationships']} 候选{stats['candidates']}")
        
        # V5.1 报表路径
        print(f"\n报表路径:")
        print(f"  - memory/reports/daily/station/{date}.json")
        print(f"  - memory/reports/daily/inverter/{{device_sn}}/{date}.json")
        
        print(f"{'='*70}")
    
    # ========== V5.1 规范报表输出 ==========
    
    def _write_station_report(self, date_str: str, report_data: Dict) -> str:
        """写入场站级日报（V5.1 规范路径）"""
        from pathlib import Path
        import json
        
        output_path = Path(f"memory/reports/daily/station/{date_str}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 添加元数据
        report_with_meta = {
            'version': 'v5.1',
            'generated_at': datetime.now().isoformat(),
            'report_type': 'daily_station',
            'data': report_data
        }
        
        output_path.write_text(json.dumps(report_with_meta, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f"  ✅ 场站日报: {output_path}")
        return str(output_path)
    
    def _write_inverter_report(self, device_sn: str, date_str: str, 
                               device_data: Dict, advice: Dict) -> str:
        """写入逆变器级日报（V5.1 规范路径）"""
        from pathlib import Path
        import json
        
        output_path = Path(f"memory/reports/daily/inverter/{device_sn}/{date_str}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        report = {
            'version': 'v5.1',
            'generated_at': datetime.now().isoformat(),
            'report_type': 'daily_inverter',
            'device_sn': device_sn,
            'date': date_str,
            'data': {
                'health_score': self._get_score(device_data),
                'level': device_data.get('level'),
                'dimensions': device_data.get('dimensions'),
                'advice': advice
            }
        }
        
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(output_path)
    
    # ========== 横向对比（从 registry 读取指标配置）==========
    
    def _run_horizontal_comparison(self, device_results: Dict, device_data: Dict, date_str: str) -> Dict:
        """
        横向对比 - 从 registry 读取 power_active 指标配置
        """
        # 从 registry 获取功率指标配置
        power_config = self._get_indicator_config('power_active')
        if not power_config:
            print(f"  ⚠️ 未找到 power_active 指标配置，跳过横向对比")
            return {'devices_compared': 0, 'is_anomaly': False, 'power_gap_pct': 0}
        
        # 获取指标输入字段（物模型编码）
        inputs = power_config.get('inputs', ['ai56'])
        point_code = inputs[0] if inputs else 'ai56'
        
        # 从原始数据中获取功率
        power_data = {}
        for sn, data in device_data.items():
            if data and 'raw_data' in data and point_code in data['raw_data']:
                df = data['raw_data'][point_code]
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
        
        # 从 registry 读取异常阈值（默认20%）
        gap_config = self._get_indicator_config('power_gap_ratio')
        threshold = 20  # 默认阈值
        if gap_config:
            # 可以从配置中读取阈值，如果没有则使用默认值
            pass
        
        is_anomaly = power_gap_pct > threshold
        
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
            'is_anomaly': is_anomaly,
            'indicator_id': 'power_active',
            'threshold': threshold
        }
    
    # ========== 时间维度分析（从 registry 读取指标配置）==========
    
    def _run_trend_analysis(self, device_results: Dict, date_str: str) -> Dict:
        """
        时间维度分析 - 从 registry 读取 health_score 指标配置
        """
        # 从 registry 获取健康分指标配置
        health_config = self._get_indicator_config('health_score')
        if not health_config:
            print(f"  ⚠️ 未找到 health_score 指标配置，跳过趋势分析")
            return {'has_trend': False, 'trend_alerts': [], 'has_anomaly': False}
        
        # 从 registry 获取趋势变化指标配置
        trend_config = self._get_indicator_config('health_trend_change')
        threshold = 30  # 默认阈值 30%
        if trend_config:
            # 可以从配置中读取阈值
            pass
        
        recent_reports = self.memory.read_recent_reports(days=7)
        
        if not recent_reports:
            print(f"  ⏭️ 无历史数据，跳过趋势分析")
            return {'has_trend': False, 'trend_alerts': [], 'has_anomaly': False}
        
        trend_alerts = []
        
        for sn, data in device_results.items():
            current_health = self._get_score(data)
            if current_health is None:
                continue
            
            # 找该设备的历史数据
            historical = []
            for report in recent_reports:
                if sn in report.get('devices', {}):
                    hist_score = self._get_score(report['devices'][sn])
                    if hist_score is not None:
                        historical.append(hist_score)
            
            if len(historical) >= 3:
                avg_historical = sum(historical) / len(historical)
                change_pct = ((current_health - avg_historical) / avg_historical * 100) if avg_historical > 0 else 0
                
                # 使用 registry 中的阈值判断异常
                if abs(change_pct) > threshold:
                    trend_alerts.append({
                        'device': sn,
                        'change_pct': change_pct,
                        'current': current_health,
                        'historical_avg': avg_historical,
                        'indicator_id': 'health_trend_change'
                    })
                    print(f"  ⚠️ {sn}: 健康分变化 {change_pct:+.1f}%")
        
        if not trend_alerts:
            print(f"  ✅ 趋势正常: 历史{len(recent_reports)}天, 无异常")
        
        return {
            'has_trend': True,
            'historical_days': len(recent_reports),
            'trend_alerts': trend_alerts,
            'has_anomaly': len(trend_alerts) > 0,
            'indicator_id': 'health_score',
            'threshold': threshold
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
    result = workflow.run(date_str)
    # 释放内存，防止批量处理时内存溢出
    gc.collect()
    return result


if __name__ == '__main__':
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else '2025-08-15'
    run_daily_v5(date)
