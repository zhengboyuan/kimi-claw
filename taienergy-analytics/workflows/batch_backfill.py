"""
V5.1 批量历史数据回补工作流
基于 daily_v5.py 扩展，支持批量回补历史数据
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import gc  # 垃圾回收

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workflows.daily_v5 import DailyAssetManagementV5
from core.unified_history import UnifiedHistoryStore
from core.aggregation_engine import AggregationEngine


class BatchBackfillWorkflow:
    """
    批量历史数据回补工作流
    
    基于 daily_v5 扩展，支持：
    1. 批量回补指定日期范围的数据
    2. 自动跳过已存在的日期（增量回补）
    3. 生成回补报告
    """
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.daily_workflow = DailyAssetManagementV5()
        self.history = UnifiedHistoryStore()
        self.aggregation = AggregationEngine(self.history)
    
    def run_backfill(self, start_date: str, end_date: str, 
                     skip_existing: bool = True,
                     indicators: List[str] = None) -> Dict:
        """
        运行批量回补
        
        Args:
            start_date: 开始日期，如 "2025-08-01"
            end_date: 结束日期，如 "2025-08-30"
            skip_existing: 是否跳过已存在的日期
            indicators: 指定回补的指标，None表示全部
        
        Returns:
            回补结果报告
        """
        print("=" * 70)
        print("V5.1 批量历史数据回补")
        print("=" * 70)
        print(f"\n回补范围: {start_date} 至 {end_date}")
        print(f"设备数: {len(self.DEVICE_CLUSTER)} 台")
        print(f"跳过已存在: {skip_existing}")
        
        # 生成日期列表
        dates = self._generate_date_range(start_date, end_date)
        print(f"总天数: {len(dates)}")
        
        # 检查已存在的日期
        if skip_existing:
            existing_dates = self._get_existing_dates()
            dates_to_process = [d for d in dates if d not in existing_dates]
            print(f"已存在: {len(existing_dates)} 天，需回补: {len(dates_to_process)} 天")
        else:
            dates_to_process = dates
        
        if not dates_to_process:
            print("\n✅ 无需回补，所有日期已存在")
            return {"status": "skipped", "reason": "all_dates_exist"}
        
        # 开始回补
        results = {
            "start_date": start_date,
            "end_date": end_date,
            "total_dates": len(dates),
            "processed_dates": 0,
            "success_dates": [],
            "failed_dates": [],
            "errors": []
        }
        
        print(f"\n开始回补...")
        for i, date_str in enumerate(dates_to_process, 1):
            print(f"\n[{i}/{len(dates_to_process)}] 处理: {date_str}")
            
            try:
                # 调用 daily_v5 工作流
                result = self.daily_workflow.run(date_str)
                
                if result:
                    results["success_dates"].append(date_str)
                    print(f"  ✅ 成功")
                else:
                    results["failed_dates"].append(date_str)
                    print(f"  ⚠️ 无结果")
                    
            except Exception as e:
                results["failed_dates"].append(date_str)
                results["errors"].append({"date": date_str, "error": str(e)})
                print(f"  ❌ 失败: {e}")
            
            results["processed_dates"] += 1
            
            # 显式释放内存（关键修复）
            gc.collect()
            
            # 每5天额外清理一次
            if i % 5 == 0:
                print(f"  🧹 内存清理 ({i}/{len(dates_to_process)})")
                gc.collect()
        
        # 生成回补后报告
        print(f"\n{'=' * 70}")
        print("回补完成，生成报告...")
        print(f"{'=' * 70}")
        
        self._generate_backfill_report(results)
        
        return results
    
    def _generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """生成日期列表"""
        dates = []
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
    
    def _get_existing_dates(self) -> set:
        """获取已存在的日期（基于第一个设备的历史数据）"""
        # 检查第一个设备的 daily 目录
        daily_dir = Path("memory/devices/XHDL_1NBQ/daily")
        if not daily_dir.exists():
            return set()
        
        return set(f.stem for f in daily_dir.glob('*.json'))
    
    def _generate_backfill_report(self, results: Dict):
        """生成回补报告"""
        success = len(results["success_dates"])
        failed = len(results["failed_dates"])
        total = results["processed_dates"]
        
        print(f"\n【回补结果】")
        print(f"  成功: {success} 天")
        print(f"  失败: {failed} 天")
        print(f"  成功率: {success/total*100:.1f}%" if total > 0 else "  成功率: N/A")
        
        if results["failed_dates"]:
            print(f"\n  失败日期:")
            for d in results["failed_dates"][:5]:
                print(f"    - {d}")
        
        # 生成设备画像（基于回补后的数据）
        print(f"\n【生成设备画像】")
        profiles = {}
        for sn in self.DEVICE_CLUSTER:
            profile = self.aggregation.generate_device_profile(sn, days=30)
            if profile.get('status') != 'insufficient_data':
                profiles[sn] = profile
        
        print(f"  ✅ 生成 {len(profiles)} 台设备画像")
        
        # 生成排名
        if profiles:
            latest_date = max(results["success_dates"]) if results["success_dates"] else None
            if latest_date:
                ranking = self.aggregation.generate_station_ranking(latest_date, profiles)
                print(f"\n【场站排名】({latest_date})")
                print(f"  TOP 3: {', '.join(ranking.get('top_performers', {}).get('by_health', [])[:3])}")
        
        print(f"\n{'=' * 70}")
        print("批量回补完成")
        print(f"{'=' * 70}")


# 便捷函数
def run_backfill(start_date: str, end_date: str, skip_existing: bool = True) -> Dict:
    """便捷函数：运行批量回补"""
    workflow = BatchBackfillWorkflow()
    return workflow.run_backfill(start_date, end_date, skip_existing)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) >= 3:
        start = sys.argv[1]
        end = sys.argv[2]
    else:
        # 默认回补最近7天
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    result = run_backfill(start, end)
    print(f"\n结果: {result['processed_dates']} 天已处理")