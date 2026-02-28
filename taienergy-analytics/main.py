#!/usr/bin/python3
"""
光伏设备滚动迭代深度分析系统 - 主入口
支持自主进化指标体系

用法:
    # 单日分析
    python main.py --date 2025-02-22
    
    # 滚动分析（从起始日期到今日）
    python main.py --start-date 2025-02-22
    
    # 指定日期范围
    python main.py --start-date 2025-02-22 --end-date 2025-02-24
    
    # 指定设备
    python main.py --device XHDL_1NBQ --start-date 2025-02-22
    
    # 进化模式：扫描全量点位，更新指标档案
    python main.py --evolve
    
    # 显示指标进化报告
    python main.py --evolution-report
"""

import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from workflows.daily_inspection import DailyInspectionWorkflow
from core.evolution_manager import IndicatorEvolutionManager
from utils.taienergy_api import get_api


def main():
    parser = argparse.ArgumentParser(
        description="光伏设备滚动迭代深度分析系统（支持自主进化指标体系）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --date 2026-02-24                    # 分析单日
  %(prog)s --start-date 2025-02-22              # 从该日开始滚动分析
  %(prog)s --start-date 2025-02-22 --end-date 2025-02-24  # 指定范围
  %(prog)s --device XHDL_1NBQ --start-date 2025-02-22     # 指定设备
  %(prog)s --evolve                             # 进化模式：全量扫描并更新指标档案
  %(prog)s --evolution-report                   # 显示指标进化报告
        """
    )
    
    parser.add_argument(
        "--device",
        default="XHDL_1NBQ",
        help="设备SN (默认: XHDL_1NBQ)"
    )
    
    parser.add_argument(
        "--date",
        help="分析指定日期 (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--start-date",
        help="滚动分析起始日期 (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        help="滚动分析结束日期 (YYYY-MM-DD，默认为今天)"
    )
    
    parser.add_argument(
        "--list-indicators",
        action="store_true",
        help="列出设备支持的分析指标"
    )
    
    parser.add_argument(
        "--evolve",
        action="store_true",
        help="进化模式：扫描全量点位，更新 indicator_catalog.json"
    )
    
    parser.add_argument(
        "--evolution-report",
        action="store_true",
        help="显示指标进化报告"
    )
    
    parser.add_argument(
        "--probe-range",
        type=int,
        default=200,
        help="探测范围（默认: 200，即 ai1-200, di1-200）"
    )
    
    args = parser.parse_args()
    
    # ========== 进化模式 ==========
    if args.evolve:
        print(f"\n{'='*60}")
        print("进化模式：全量点位扫描")
        print(f"{'='*60}\n")
        
        api = get_api()
        
        print(f"设备: {args.device}")
        print(f"探测范围: ai1-ai{args.probe_range}, di1-di{args.probe_range}")
        print()
        
        # 执行全量探测
        discovered = api.discover_available_points(
            device_sn=args.device,
            probe_range=args.probe_range,
            force_full_scan=True
        )
        
        # 注册到进化管理器
        evo_manager = IndicatorEvolutionManager(args.device)
        
        # 获取属性名称
        properties = api.get_device_properties(args.device)
        
        # 获取指标类型映射
        from skills.skill_1_data_collector import DataCollector
        collector = DataCollector(args.device)
        metadata = collector.get_indicator_metadata()
        
        print(f"\n注册 {len(discovered)} 个指标到进化体系...")
        for code in discovered:
            prop_info = properties.get(code, {})
            name = prop_info.get("ppn", code)
            unit = prop_info.get("unit", "")
            meta = metadata.get(code, {})
            ind_type = meta.get("type", "other")
            
            # 判断是否为哨兵指标
            from config.device_config import SENTINEL_INDICATORS
            is_sentinel = code in SENTINEL_INDICATORS or code.startswith("di")
            
            evo_manager.register_indicator(
                indicator_code=code,
                indicator_name=name,
                indicator_unit=unit,
                indicator_type=ind_type,
                level="L0",
                is_sentinel=is_sentinel
            )
        
        # 更新元数据映射表
        evo_manager.update_indicator_metadata(metadata)
        
        # 打印进化报告
        evo_manager.print_evolution_report()
        
        print(f"\n指标档案已保存至: memory/indicator_catalog.json")
        
        # 如果指定了起始日期，继续跑历史数据评价
        if args.start_date:
            print(f"\n{'='*60}")
            print("开始历史数据评价与进化")
            print(f"{'='*60}\n")
            
            end_date = args.end_date or datetime.now().strftime("%Y-%m-%d")
            print(f"评价范围: {args.start_date} ~ {end_date}")
            print()
            
            # 使用工作流跑滚动分析（带进化）
            workflow = DailyInspectionWorkflow(args.device)
            results = workflow.run_rolling_analysis_with_evolution(
                args.start_date, 
                end_date,
                evo_manager=evo_manager
            )
            
            # 最终报告
            print(f"\n{'='*60}")
            print("进化完成！最终指标分布：")
            print(f"{'='*60}\n")
            evo_manager.print_evolution_report()
        
        return
    
    # ========== 进化报告模式 ==========
    if args.evolution_report:
        print(f"\n{'='*60}")
        print("指标进化报告")
        print(f"{'='*60}\n")
        
        evo_manager = IndicatorEvolutionManager(args.device)
        evo_manager.print_evolution_report()
        return
    
    # ========== 列表模式 ==========
    if args.list_indicators:
        evo_manager = IndicatorEvolutionManager(args.device)
        targets = evo_manager.get_analysis_targets()
        summary = evo_manager.get_catalog_summary()
        
        print(f"\n设备 {args.device} 指标体系:")
        print("-" * 60)
        print(f"L2 核心指标 ({len(targets['deep_analysis'])} 个): 深度分析 + LLM 诊断")
        for code in targets['deep_analysis'][:10]:
            info = evo_manager.catalog["indicators"].get(code, {})
            print(f"  - {code} ({info.get('name', 'N/A')})")
        if len(targets['deep_analysis']) > 10:
            print(f"  ... 还有 {len(targets['deep_analysis']) - 10} 个")
        
        print(f"\nL1 活跃指标 ({len(targets['trend_tracking'])} 个): 趋势跟踪")
        for code in targets['trend_tracking'][:5]:
            info = evo_manager.catalog["indicators"].get(code, {})
            print(f"  - {code} ({info.get('name', 'N/A')})")
        if len(targets['trend_tracking']) > 5:
            print(f"  ... 还有 {len(targets['trend_tracking']) - 5} 个")
        
        print(f"\nL0 候选指标 ({len(targets['basic_stats'])} 个): 基础统计")
        print(f"静默池指标 ({len(targets['silent'])} 个)")
        print(f"哨兵指标 ({summary['sentinels']} 个): 永久保留")
        print()
        return
    
    # ========== 标准分析模式 ==========
    # 初始化工作流
    workflow = DailyInspectionWorkflow(args.device)
    
    # 执行分析
    if args.date:
        # 单日分析
        print(f"\n执行单日分析: {args.date}")
        result = workflow.run_daily_inspection(args.date)
        
    elif args.start_date:
        # 滚动分析 - 使用进化版流程（带Claw Agent）
        end_date = args.end_date or datetime.now().strftime("%Y-%m-%d")
        print(f"\n执行滚动分析: {args.start_date} ~ {end_date}")
        print("(使用进化版流程，包含Claw Agent关联分析)\n")
        
        # 使用进化版流程
        from core.evolution_manager import IndicatorEvolutionManager
        evo_manager = IndicatorEvolutionManager(args.device)
        
        results = workflow.run_rolling_analysis_with_evolution(
            args.start_date, 
            end_date,
            evo_manager=evo_manager
        )
        
        # 最终打印进化报告
        evo_manager.print_evolution_report()
        
    else:
        # 默认：分析昨天
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"\n未指定日期，默认分析昨天: {yesterday}")
        print("使用 --help 查看更多用法\n")
        result = workflow.run_daily_inspection(yesterday)


if __name__ == "__main__":
    main()
