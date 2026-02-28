"""
每日巡检工作流（进化版）
三段式：探测阶段 → 评价阶段 → 分析阶段
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from skills.skill_1_data_collector import DataCollector
from core.time_series_analyzer import TimeSeriesAnalyzer
from core.indicator_evaluator import IndicatorEvaluator
from core.evolution_manager import IndicatorEvolutionManager
from skills.skill_6_deep_analyzer import RootCauseDiagnostician
from skills.skill_10_daily_reporter import DailyReporter
from utils.memory_manager import MemoryManager
from config.device_config import DEVICES, CORE_BENCHMARKS

# V3.0 新增
from core.composite_engine import CompositeIndicatorEngine
from core.deep_analyzer_v3 import DeepAnalyzerV3
from core.knowledge_distiller import KnowledgeDistiller
import pandas as pd


class DailyInspectionWorkflow:
    """
    每日巡检工作流（进化版）
    
    三段式进化流：
    1. 探测阶段 (Discovery):
       - 检查是否有从未见过的新指标（抽样探测）
       - 新指标加入候选池 (L0)
    
    2. 评价阶段 (Refining):
       - 对所有指标运行 IndicatorEvaluator（四维度评分）
       - 根据评分执行进化（升级/降级/静默/淘汰）
       - 更新 indicator_catalog.json
    
    3. 分析阶段 (Deep Analysis):
       - L2 核心指标：深度分析 + LLM 根因诊断
       - L1 活跃指标：趋势跟踪
       - L0 候选指标：基础统计
    
    进化规则：
    - 升级：连续 3 天评分 > 0.6 → 升级
    - 降级：连续 7 天数据为 0/空 → 进入静默池
    - 静默：7 天无数据 → 静默池（不再深度分析）
    - 淘汰：30 天无数据 → 移除（哨兵指标除外）
    """
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.device_config = DEVICES.get(device_sn, {})
        
        # 初始化各组件
        self.data_collector = DataCollector(device_sn)
        self.daily_reporter = DailyReporter(device_sn)
        self.memory_manager = MemoryManager(device_sn)
        self.diagnostician = RootCauseDiagnostician(device_sn)
        self.evaluator = IndicatorEvaluator()
        self.evo_manager = IndicatorEvolutionManager(device_sn)
        
        # 分析器缓存
        self.analyzers = {}
    
    def run_daily_inspection(self, date_str: str) -> dict:
        """
        执行每日巡检（三段式进化流）
        
        Args:
            date_str: 日期，如 "2025-02-22"
        
        Returns:
            巡检结果
        """
        print(f"\n{'='*70}")
        print(f"开始每日巡检（进化版）: {self.device_sn} @ {date_str}")
        print(f"{'='*70}\n")
        
        # ========== Stage 1: 探测阶段 (Discovery) ==========
        print("[Stage 1] 探测阶段 (Discovery)")
        print("-" * 50)
        
        # 1.1 获取当前监控的指标列表
        current_targets = self.evo_manager.get_analysis_targets()
        all_monitored = (
            current_targets["deep_analysis"] +
            current_targets["trend_tracking"] +
            current_targets["basic_stats"]
        )
        
        print(f"  当前监控指标: {len(all_monitored)} 个")
        print(f"    - L2 核心: {len(current_targets['deep_analysis'])} 个")
        print(f"    - L1 活跃: {len(current_targets['trend_tracking'])} 个")
        print(f"    - L0 候选: {len(current_targets['basic_stats'])} 个")
        
        # 1.2 抽样探测未知指标（每周一次全量，日常抽样）
        discovery_config = self.device_config.get("discovery", {})
        sample_size = discovery_config.get("daily_probe_sample", 10)
        
        print(f"\n  抽样探测 {sample_size} 个未知指标...")
        new_indicators = self.data_collector.probe_unknown_indicators(
            known_indicators=all_monitored,
            sample_size=sample_size
        )
        
        if new_indicators:
            print(f"  发现 {len(new_indicators)} 个新指标，注册到候选池")
            for code in new_indicators:
                name = self.data_collector.get_property_name(code)
                self.evo_manager.register_indicator(
                    indicator_code=code,
                    indicator_name=name,
                    level="L0"
                )
        else:
            print("  未发现新指标")
        
        # 1.3 确定今日分析目标
        analysis_targets = self.evo_manager.get_analysis_targets()
        indicators_to_analyze = (
            analysis_targets["deep_analysis"] +
            analysis_targets["trend_tracking"] +
            analysis_targets["basic_stats"]
        )
        
        print(f"\n  今日分析目标: {len(indicators_to_analyze)} 个指标")
        
        # ========== Stage 2: 数据获取与评价阶段 (Refining) ==========
        print(f"\n[Stage 2] 数据获取与评价阶段 (Refining)")
        print("-" * 50)
        
        # 2.1 获取数据
        print(f"  获取 {len(indicators_to_analyze)} 个指标的历史数据...")
        daily_data = self.data_collector.collect_daily_data(
            date_str=date_str,
            indicators=indicators_to_analyze
        )
        
        if not daily_data:
            print("  ⚠️ 未获取到任何数据")
            return self._generate_error_report(date_str, {"quality_score": 0})
        
        print(f"  ✅ 成功获取 {len(daily_data)} 个指标数据")
        
        # 2.2 确定核心基准指标
        core_benchmark = self.evaluator.get_core_benchmark(daily_data)
        if core_benchmark is not None:
            # 找到核心指标的代码
            core_code = None
            for code, df in daily_data.items():
                if df is core_benchmark:
                    core_code = code
                    break
            print(f"  核心基准指标: {core_code or '未知'}")
        
        # 2.3 评价所有指标
        print(f"\n  执行四维度评价...")
        evaluation_results = {}
        
        for indicator_code, df in daily_data.items():
            if df.empty:
                continue
            
            # 执行评价
            result = self.evaluator.evaluate(
                df=df,
                core_df=core_benchmark,
                indicator_code=indicator_code
            )
            
            evaluation_results[indicator_code] = result
            
            # 执行进化
            self.evo_manager.evaluate_and_evolve(
                indicator_code=indicator_code,
                evaluation_result=result,
                date_str=date_str
            )
        
        print(f"  ✅ 完成 {len(evaluation_results)} 个指标的评价与进化")
        
        # 2.4 数据质量检查
        quality_report = self.data_collector.get_data_quality_report()
        print(f"  数据质量评分: {quality_report['quality_score']:.1f}/100")
        
        if not quality_report['can_proceed']:
            print("  ⚠️ 数据质量不足，阻断分析流程")
            return self._generate_error_report(date_str, quality_report)
        
        # ========== Stage 3: 分析阶段 (Deep Analysis) ==========
        print(f"\n[Stage 3] 分析阶段 (Deep Analysis)")
        print("-" * 50)
        
        # 重新获取最新的分析目标（可能已进化）
        analysis_targets = self.evo_manager.get_analysis_targets()
        
        analysis_results = {}
        has_significant_anomaly = False
        
        # 3.1 L2 核心指标：深度分析
        if analysis_targets["deep_analysis"]:
            print(f"\n  [L2 核心指标] 深度分析 ({len(analysis_targets['deep_analysis'])} 个)...")
            for indicator_code in analysis_targets["deep_analysis"]:
                if indicator_code not in daily_data:
                    continue
                
                df = daily_data[indicator_code]
                if df.empty:
                    continue
                
                result = self._analyze_indicator(indicator_code, df, deep=True)
                analysis_results[indicator_code] = result
                
                if result.get("anomalies"):
                    has_significant_anomaly = True
                    print(f"    ⚠️ {indicator_code}: {len(result['anomalies'])} 个异常")
        
        # 3.2 L1 活跃指标：趋势跟踪
        if analysis_targets["trend_tracking"]:
            print(f"\n  [L1 活跃指标] 趋势跟踪 ({len(analysis_targets['trend_tracking'])} 个)...")
            for indicator_code in analysis_targets["trend_tracking"]:
                if indicator_code not in daily_data:
                    continue
                
                df = daily_data[indicator_code]
                if df.empty:
                    continue
                
                result = self._analyze_indicator(indicator_code, df, deep=False)
                analysis_results[indicator_code] = result
                
                if result.get("anomalies"):
                    has_significant_anomaly = True
                    print(f"    ⚠️ {indicator_code}: {len(result['anomalies'])} 个异常")
        
        # 3.3 L0 候选指标：基础统计
        if analysis_targets["basic_stats"]:
            print(f"\n  [L0 候选指标] 基础统计 ({len(analysis_targets['basic_stats'])} 个)...")
            # 仅记录基础统计，不输出详细信息
            for indicator_code in analysis_targets["basic_stats"]:
                if indicator_code not in daily_data:
                    continue
                
                df = daily_data[indicator_code]
                if df.empty:
                    continue
                
                # 简单统计，不存储详细结果
                values = df['value'].dropna()
                if len(values) > 0:
                    print(f"    ✅ {indicator_code}: 均值 {values.mean():.2f}")
        
        # ========== Stage 4: 生成报告与记忆更新 ==========
        print(f"\n[Stage 4] 生成报告与记忆更新")
        print("-" * 50)
        
        # 4.1 生成日报
        report = self.daily_reporter.generate_report(
            date_str=date_str,
            analysis_results=analysis_results,
            data_quality_report=quality_report
        )
        
        print(f"\n{'='*70}")
        print("日报摘要:")
        print(f"{'='*70}")
        print(report['summary'])
        
        # 4.2 更新记忆
        self._update_memory(date_str, quality_report, analysis_results, has_significant_anomaly)
        
        # 4.3 打印进化报告
        self.evo_manager.print_evolution_report()
        
        # ========== 完成 ==========
        print(f"\n{'='*70}")
        print(f"巡检完成: {date_str}")
        print(f"{'='*70}\n")
        
        return {
            "date": date_str,
            "device_sn": self.device_sn,
            "status": report['status'],
            "quality_score": quality_report['quality_score'],
            "has_anomaly": has_significant_anomaly,
            "report": report,
            "memory_file": str(self.memory_manager.analysis_file),
            "catalog_file": str(self.evo_manager.catalog_file)
        }
    
    def _analyze_indicator(self, indicator_code: str, df, deep: bool = False) -> dict:
        """分析单个指标"""
        # 获取或创建分析器
        if indicator_code not in self.analyzers:
            name = self.data_collector.get_property_name(indicator_code)
            self.analyzers[indicator_code] = TimeSeriesAnalyzer(
                indicator_code=indicator_code,
                indicator_name=name
            )
        
        analyzer = self.analyzers[indicator_code]
        
        # 执行分析
        result = analyzer.analyze(df)
        
        # 添加元数据
        result['code'] = indicator_code
        result['name'] = df.attrs.get('name', indicator_code)
        result['unit'] = df.attrs.get('unit', '')
        
        # 更新分析器模型
        analyzer.update_model(df)
        
        return result
    
    def _update_memory(self, date_str: str, quality_report: dict, analysis_results: dict, has_anomaly: bool):
        """更新记忆"""
        # 保存 L2 动态履历
        self.memory_manager.save_daily_log(
            date_str=date_str,
            data={
                "quality_score": quality_report['quality_score'],
                "indicators_analyzed": list(analysis_results.keys()),
                "has_anomaly": has_anomaly,
                "analysis_summary": {
                    k: {
                        "status": v.get("status"),
                        "anomaly_count": len(v.get("anomalies", [])),
                        "code": v.get("code", k)
                    }
                    for k, v in analysis_results.items()
                }
            }
        )
        
        # 更新 L3 认知知识
        for indicator_code, result in analysis_results.items():
            insights = result.get("insights", [])
            if insights:
                content = "\n".join([f"- {i}" for i in insights])
                self.memory_manager.update_analysis_memory(
                    section=f"{indicator_code}_insights",
                    content=content
                )
        
        print(f"  ✅ 记忆已更新")
    
    def _generate_error_report(self, date_str: str, quality_report: dict) -> dict:
        """生成错误报告"""
        report = self.daily_reporter.generate_report(
            date_str=date_str,
            analysis_results={},
            data_quality_report=quality_report
        )
        
        print(f"\n{'='*70}")
        print("日报摘要:")
        print(f"{'='*70}")
        print(report['summary'])
        
        return {
            "date": date_str,
            "device_sn": self.device_sn,
            "status": "error",
            "quality_score": quality_report['quality_score'],
            "has_anomaly": False,
            "report": report,
            "error": "数据质量不足"
        }
    
    def run_rolling_analysis(self, start_date: str, end_date: str = None):
        """
        执行滚动迭代分析
        
        从 start_date 到 end_date（或今天），逐日执行分析
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        current_dt = start_dt
        results = []
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            result = self.run_daily_inspection(date_str)
            results.append(result)
            
            current_dt += timedelta(days=1)
        
        # 输出汇总
        print(f"\n{'='*70}")
        print(f"滚动分析完成: {start_date} ~ {end_date}")
        print(f"{'='*70}")
        print(f"总天数: {len(results)}")
        print(f"正常天数: {sum(1 for r in results if r['status'] == 'normal')}")
        print(f"异常天数: {sum(1 for r in results if r['status'] == 'abnormal')}")
        print(f"错误天数: {sum(1 for r in results if r['status'] == 'error')}")
        
        return results
    
    def run_rolling_analysis_with_evolution(
        self, 
        start_date: str, 
        end_date: str = None,
        evo_manager: IndicatorEvolutionManager = None
    ):
        """
        执行带进化的滚动迭代分析
        
        从 start_date 到 end_date，逐日执行：
        1. 数据收集
        2. 指标评价（差异化评分）
        3. 进化管理（升级/降级）
        
        Args:
            start_date: 起始日期
            end_date: 结束日期（默认今天）
            evo_manager: 进化管理器实例（可选）
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        # 使用传入的 evo_manager 或创建新的
        if evo_manager:
            self.evo_manager = evo_manager
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        current_dt = start_dt
        day_count = 0
        
        print(f"开始进化分析: {start_date} ~ {end_date}")
        print(f"总天数: {(end_dt - start_dt).days + 1}")
        print()
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            day_count += 1
            
            print(f"\n{'='*70}")
            print(f"[进化分析] Day {day_count}: {date_str}")
            print(f"{'='*70}")
            
            # 1. 数据收集
            print(f"\n[1/3] 数据收集...")
            daily_data = self.data_collector.collect_daily_data(date_str)
            
            if not daily_data:
                print(f"  ⚠️ {date_str} 无数据，跳过")
                current_dt += timedelta(days=1)
                continue
            
            # 获取指标类型映射
            indicator_types = {
                code: df.attrs.get('type', 'other') 
                for code, df in daily_data.items()
            }
            
            # 2. 指标评价（差异化评分）
            print(f"\n[2/3] 指标评价（差异化评分）...")
            
            # 获取核心基准数据
            core_df = self.evaluator.get_core_benchmark(daily_data)
            
            # 批量评价
            evaluation_results = self.evaluator.evaluate_batch(
                data_dict=daily_data,
                core_indicator=self.evaluator.CORE_BENCHMARKS["daytime"],
                indicator_types=indicator_types
            )
            
            # 3. 进化管理
            print(f"\n[3/3] 进化管理...")
            for indicator_code, eval_result in evaluation_results.items():
                self.evo_manager.evaluate_and_evolve(
                    indicator_code=indicator_code,
                    evaluation_result=eval_result,
                    date_str=date_str
                )
            
            # 显示当日进化摘要
            summary = self.evo_manager.get_catalog_summary()
            print(f"\n  当日进化摘要:")
            print(f"    L0: {summary['L0_candidates']} | L1: {summary['L1_active']} | L2: {summary['L2_core']}")
            
            # 4. Claw Agent关联分析（当L2指标>=3时）
            if summary['L2_core'] >= 3 and summary['L3_synthesized'] == 0:
                print(f"\n  [4/4] Claw Agent关联分析...")
                try:
                    from core.claw_agent_correlation import ClawAgentCorrelationAnalyzer
                    analyzer = ClawAgentCorrelationAnalyzer(self.device_sn)
                    # 使用默认结果（实际Claw环境中会调用Agent）
                    result = analyzer.run_analysis()
                    if result:
                        print(f"    ✅ 生成复合指标建议")
                except Exception as e:
                    print(f"    ⚠️ 关联分析跳过: {e}")
            
            current_dt += timedelta(days=1)
        
        print(f"\n{'='*70}")
        print(f"进化分析完成！共处理 {day_count} 天数据")
        print(f"{'='*70}")
        
        return {
            "start_date": start_date,
            "end_date": end_date,
            "total_days": day_count,
            "final_summary": self.evo_manager.get_catalog_summary()
        }
    
    # ========== V3.0 新增：Stage 4 复合指标进化 ==========
    
    def run_stage4_composite_evolution(self, date_str: str, daily_data: Dict) -> Dict:
        """
        Stage 4: 复合指标进化（V3.0核心）
        
        工作流程：
        1. Python引擎：穷举变异、筛选异常
        2. LLM诊断：物理解释、生成规则
        3. 知识固化：保存到dynamic_rules.json
        
        Args:
            date_str: 日期
            daily_data: 当日数据
        
        Returns:
            诊断结果
        """
        print(f"\n{'='*70}")
        print("[Stage 4] V3.0 复合指标进化")
        print(f"{'='*70}")
        
        # 1. 合并所有指标数据为DataFrame
        df_merged = self._merge_daily_data(daily_data)
        
        if df_merged.empty:
            print("  ⚠️ 数据为空，跳过复合指标分析")
            return {}
        
        # 2. Python引擎：生成并筛选复合指标
        print("\n  [4.1] 启动复合指标变异引擎...")
        engine = CompositeIndicatorEngine(df_merged)
        survivors = engine.generate_and_select()
        
        if not survivors:
            print("  ✅ 今日未发现复合指标异常突变")
            return {"status": "no_anomaly"}
        
        print(f"  🚨 抓取到 {len(survivors)} 个异常突变，移交LLM诊断...")
        
        # 3. LLM诊断：物理解释
        print("\n  [4.2] LLM深度诊断...")
        temp_max = df_merged['ai61'].max() if 'ai61' in df_merged.columns else 0
        
        analyzer = DeepAnalyzerV3()  # 可以传入llm_client
        diagnosis = analyzer.analyze_anomalies(
            surviving_composites=survivors,
            temp_max=temp_max,
            context={"date": date_str, "device": self.device_sn}
        )
        
        # 4. 打印诊断结果
        print(f"\n  [4.3] 诊断结果:")
        print(f"    异常数量: {len(diagnosis.get('diagnosed_anomalies', []))}")
        print(f"    整体风险: {diagnosis.get('overall_risk_level', 'unknown')}")
        
        for anomaly in diagnosis.get('diagnosed_anomalies', []):
            print(f"\n    - {anomaly['composite_name']}")
            print(f"      原因: {anomaly['physical_diagnosis'][:50]}...")
            print(f"      级别: {anomaly['severity']}")
            print(f"      建议: {anomaly['remediation']['immediate_action']}")
        
        # 5. 知识固化：保存规则
        print("\n  [4.4] 知识固化...")
        distiller = KnowledgeDistiller(self.device_sn)
        rules_added = self._save_diagnosis_rules(diagnosis, distiller)
        
        if rules_added:
            print(f"    ✅ 已固化 {rules_added} 条新规则到 dynamic_rules.json")
        
        return diagnosis
    
    def _merge_daily_data(self, daily_data: Dict) -> pd.DataFrame:
        """
        合并所有指标数据为DataFrame（内存优化版）
        
        优化点：
        1. 避免循环merge，一次性创建DataFrame
        2. 使用字典收集数据，减少内存碎片
        3. 复用timestamp，避免重复数据
        """
        # 1. 提取所有数据为字典，避免创建多个小DataFrame
        data_dict = {'timestamp': None}
        
        for code, df in daily_data.items():
            if df.empty or 'value' not in df.columns:
                continue
            # 只保存第一个指标的timestamp
            if data_dict['timestamp'] is None:
                data_dict['timestamp'] = df['timestamp'].values
            # 保存指标值
            data_dict[code] = df['value'].values
        
        if data_dict['timestamp'] is None:
            return pd.DataFrame()
        
        # 2. 一次性创建DataFrame，避免内存碎片
        merged = pd.DataFrame(data_dict)
        
        return merged
    
    def _save_diagnosis_rules(self, diagnosis: Dict, distiller) -> int:
        """保存诊断规则"""
        from core.deep_analyzer_v3 import save_diagnosis_to_rules
        return save_diagnosis_to_rules(diagnosis, distiller)


# 主入口
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="光伏设备每日巡检（进化版）")
    parser.add_argument("--device", default="XHDL_1NBQ", help="设备SN")
    parser.add_argument("--date", help="指定日期 (YYYY-MM-DD)")
    parser.add_argument("--start-date", help="滚动分析起始日期")
    parser.add_argument("--end-date", help="滚动分析结束日期")
    parser.add_argument("--v3", action="store_true", help="启用V3.0复合指标进化")
    
    args = parser.parse_args()
    
    workflow = DailyInspectionWorkflow(args.device)
    
    if args.date:
        result = workflow.run_daily_inspection(args.date)
        
        # V3.0模式：额外运行Stage 4
        if args.v3:
            from skills.skill_1_data_collector import DataCollector
            collector = DataCollector(args.device)
            daily_data = collector.collect_daily_data(args.date)
            workflow.run_stage4_composite_evolution(args.date, daily_data)
            
    elif args.start_date:
        results = workflow.run_rolling_analysis(args.start_date, args.end_date)
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = workflow.run_daily_inspection(yesterday)
