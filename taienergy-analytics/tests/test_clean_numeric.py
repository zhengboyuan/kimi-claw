#!/usr/bin/env python3
"""
P0-1 验收测试：clean_numeric_values 数据清洗函数
验证 pd.NA/None/字符串异常值能正确处理
"""

import sys
import math
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from workflows.daily_v5 import clean_numeric_values
import pandas as pd
import numpy as np

def test_clean_numeric_values():
    """测试各种异常值场景"""
    
    print("="*60)
    print("P0-1 数据清洗测试")
    print("="*60)
    
    tests_passed = 0
    tests_failed = 0
    
    # Test 1: 正常数值
    print("\n[Test 1] 正常数值 [1, 2, 3.5]")
    result = clean_numeric_values([1, 2, 3.5])
    expected = [1.0, 2.0, 3.5]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 2: None 值
    print("\n[Test 2] None 值 [1, None, 3]")
    result = clean_numeric_values([1, None, 3])
    expected = [1.0, 3.0]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 3: pd.NA (pandas nullable NA)
    print("\n[Test 3] pd.NA 值 [1, pd.NA, 3]")
    result = clean_numeric_values([1, pd.NA, 3])
    expected = [1.0, 3.0]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 4: float NaN
    print("\n[Test 4] float NaN [1, float('nan'), 3]")
    result = clean_numeric_values([1, float('nan'), 3])
    expected = [1.0, 3.0]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 5: 字符串 "abc" (应该被过滤)
    print("\n[Test 5] 字符串 'abc' [1, 'abc', 3]")
    result = clean_numeric_values([1, 'abc', 3])
    expected = [1.0, 3.0]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 6: 数字字符串 "123" (应该被转换)
    print("\n[Test 6] 数字字符串 '123' [1, '123', 3.5]")
    result = clean_numeric_values([1, '123', 3.5])
    expected = [1.0, 123.0, 3.5]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 7: 混合异常值
    print("\n[Test 7] 混合异常值 [1, None, pd.NA, 'abc', 5, float('nan'), '10']")
    result = clean_numeric_values([1, None, pd.NA, 'abc', 5, float('nan'), '10'])
    expected = [1.0, 5.0, 10.0]
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 8: 空列表
    print("\n[Test 8] 空列表 []")
    result = clean_numeric_values([])
    expected = []
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Test 9: None 输入
    print("\n[Test 9] None 输入")
    result = clean_numeric_values(None)
    expected = []
    if result == expected:
        print(f"  ✅ PASS: {result}")
        tests_passed += 1
    else:
        print(f"  ❌ FAIL: got {result}, expected {expected}")
        tests_failed += 1
    
    # Summary
    print("\n" + "="*60)
    print(f"测试结果: {tests_passed} 通过, {tests_failed} 失败")
    print("="*60)
    
    return tests_failed == 0

if __name__ == '__main__':
    success = test_clean_numeric_values()
    sys.exit(0 if success else 1)
