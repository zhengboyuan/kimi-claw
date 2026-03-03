#!/usr/bin/env python3
"""
P0 最终验收：30天稳定回归测试
跑 2025-09-01 ~ 2025-09-30 连续30天离线流程
生成汇总报告
"""

import sys
import os
import time
from pathlib import Path
from datetime import datetime, timedelta

# 设置路径
TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from workflows.daily_v5 import DailyAssetManagementV5


def run_30day_test():
    """运行30天回归测试"""
    
    print("=" * 70)
    print("P0 最终验收：30天稳定回归测试")
    print("=" * 70)
    
    # 日期范围
    start_date = datetime(2025, 9, 1)
    end_date = datetime(2025, 9, 30)
    
    # 结果收集
    results = []
    total_time = 0
    
    workflow = DailyAssetManagementV5()
    
    current = start_date
    day_count = 0
    
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        day_count += 1
        
        print(f"\n[{day_count}/30] {date_str} ...", end=" ", flush=True)
        
        start_time = time.time()
        try:
            result = workflow.run(date_str)
            elapsed = time.time() - start_time
            total_time += elapsed
            
            # 收集关键指标
            summary = {
                'date': date_str,
                'success': True,
                'online': result.get('online', 0),
                'avg_health_score': result.get('avg_health_score', 0),
                'anomaly_count': (
                    (1 if result.get('comparison_anomaly') else 0) +
                    (1 if result.get('trend_analysis_anomaly') else 0)
                ),
                'candidates_found': result.get('candidates_found', 0),
                'elapsed_time': elapsed,
                'error': None
            }
            results.append(summary)
            print(f"✅ 成功 ({elapsed:.1f}s)", flush=True)
            
        except Exception as e:
            elapsed = time.time() - start_time
            total_time += elapsed
            
            summary = {
                'date': date_str,
                'success': False,
                'online': 0,
                'avg_health_score': 0,
                'anomaly_count': 0,
                'candidates_found': 0,
                'elapsed_time': elapsed,
                'error': str(e)
            }
            results.append(summary)
            print(f"❌ 失败: {e}", flush=True)
        
        current += timedelta(days=1)
    
    # 生成汇总报告
    generate_report(results, total_time)
    
    return results


def generate_report(results, total_time):
    """生成汇总报告"""
    
    # 统计
    total_days = len(results)
    success_days = sum(1 for r in results if r['success'])
    failed_days = total_days - success_days
    
    success_rate = (success_days / total_days * 100) if total_days > 0 else 0
    avg_time = total_time / total_days if total_days > 0 else 0
    
    # 健康分统计（成功天数）
    health_scores = [r['avg_health_score'] for r in results if r['success']]
    avg_health = sum(health_scores) / len(health_scores) if health_scores else 0
    
    # 失败详情
    failures = [(r['date'], r['error']) for r in results if not r['success']]
    
    # 创建报告目录
    report_dir = Path(__file__).parent.parent / 'reports'
    report_dir.mkdir(exist_ok=True)
    
    # 生成 Markdown 报告
    report_path = report_dir / 'p0_30day_summary.md'
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# P0 最终验收：30天稳定回归测试报告\n\n")
        f.write(f"**测试时间**: {datetime.now().isoformat()}\n\n")
        
        f.write("## 汇总统计\n\n")
        f.write(f"| 指标 | 数值 |\n")
        f.write(f"|------|------|\n")
        f.write(f"| 总天数 | {total_days} |\n")
        f.write(f"| 成功天数 | {success_days} |\n")
        f.write(f"| 失败天数 | {failed_days} |\n")
        f.write(f"| 成功率 | {success_rate:.1f}% |\n")
        f.write(f"| 平均健康分 | {avg_health:.1f} |\n")
        f.write(f"| 总耗时 | {total_time:.1f}s |\n")
        f.write(f"| 平均每日耗时 | {avg_time:.1f}s |\n")
        
        f.write("\n## 每日详细结果\n\n")
        f.write("| 日期 | 状态 | 在线设备 | 健康分 | 异常数 | 候选数 | 耗时 |\n")
        f.write("|------|------|----------|--------|--------|--------|------|\n")
        
        for r in results:
            status = "✅" if r['success'] else "❌"
            f.write(f"| {r['date']} | {status} | {r['online']} | {r['avg_health_score']:.1f} | {r['anomaly_count']} | {r['candidates_found']} | {r['elapsed_time']:.1f}s |\n")
        
        if failures:
            f.write("\n## 失败详情\n\n")
            for date, error in failures:
                f.write(f"- **{date}**: {error}\n")
        else:
            f.write("\n## 失败详情\n\n无失败记录\n")
        
        f.write("\n## 验收结论\n\n")
        if success_rate >= 99:
            f.write("✅ **验收通过**：成功率 >= 99%\n")
        elif success_rate >= 95:
            f.write("⚠️ **基本通过**：成功率 >= 95%，建议排查失败原因\n")
        else:
            f.write("❌ **验收失败**：成功率 < 95%，需要修复\n")
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)
    print(f"总天数: {total_days}")
    print(f"成功: {success_days} 天")
    print(f"失败: {failed_days} 天")
    print(f"成功率: {success_rate:.1f}%")
    print(f"平均健康分: {avg_health:.1f}")
    print(f"总耗时: {total_time:.1f}s")
    print(f"报告路径: {report_path}")
    print("=" * 70)
    
    return success_rate >= 99


if __name__ == '__main__':
    success = run_30day_test()
    sys.exit(0 if success else 1)
