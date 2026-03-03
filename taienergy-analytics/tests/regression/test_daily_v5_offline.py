#!/usr/bin/env python3
"""
回归测试：验证 daily_v5 能离线跑完一日数据不崩溃
覆盖异常值场景
"""

import sys
import os
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_v5 import DailyAssetManagementV5, clean_numeric_values
import pandas as pd


def test_clean_numeric_values_with_real_data():
    """测试真实数据场景下的清洗功能"""
    print("\n[回归测试] 真实数据场景清洗")
    
    # 模拟真实场景：包含各种异常的数据
    raw_data = [
        100.5,  # 正常值
        None,   # None
        pd.NA,  # pd.NA
        200.0,  # 正常值
        "abc",  # 无效字符串
        float('nan'),  # NaN
        "150.5",  # 数字字符串
    ]
    
    result = clean_numeric_values(raw_data)
    
    # 验证：应该保留 100.5, 200.0, 150.5
    assert len(result) == 3, f"期望3个值，实际{len(result)}个"
    assert 100.5 in result
    assert 200.0 in result
    assert 150.5 in result
    
    print("✅ 真实数据场景清洗通过")
    return True


def test_daily_v5_offline_single_day():
    """离线测试：跑单日数据不崩溃"""
    print("\n[回归测试] daily_v5 离线单日测试")
    
    try:
        workflow = DailyAssetManagementV5()
        
        # 使用已有数据日期测试
        result = workflow.run('2025-09-01')
        
        # 验证基本输出结构
        assert 'date' in result
        assert 'online' in result
        assert 'avg_health_score' in result
        assert result['online'] == 16, f"期望16台在线，实际{result['online']}台"
        
        print(f"✅ 单日测试通过: {result['date']}, 在线{result['online']}台, 健康分{result['avg_health_score']:.1f}")
        return True
        
    except Exception as e:
        print(f"❌ 单日测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_daily_v5_two_days_trend():
    """离线测试：连续两天，验证趋势分析能读取前一天数据"""
    print("\n[回归测试] daily_v5 两日趋势测试")
    
    try:
        workflow = DailyAssetManagementV5()
        
        # 第一天
        result1 = workflow.run('2025-09-02')
        print(f"  第一天: {result1['date']}, 健康分{result1['avg_health_score']:.1f}")
        
        # 第二天
        result2 = workflow.run('2025-09-03')
        print(f"  第二天: {result2['date']}, 健康分{result2['avg_health_score']:.1f}")
        
        # 验证两天都有数据
        assert result1['online'] == 16
        assert result2['online'] == 16
        
        print("✅ 两日趋势测试通过")
        return True
        
    except Exception as e:
        print(f"❌ 两日测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("=" * 60)
    print("P2-7 回归测试: daily_v5 异常值场景")
    print("=" * 60)
    
    tests = [
        test_clean_numeric_values_with_real_data,
        test_daily_v5_offline_single_day,
        test_daily_v5_two_days_trend,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test.__name__} 异常: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"回归测试完成: {passed} 通过, {failed} 失败")
    print("=" * 60)
    
    sys.exit(0 if failed == 0 else 1)
