#!/usr/bin/env python3
"""
P0-1 数据清洗测试
验证 clean_numeric_values 能正确处理各种异常值
"""

import sys
import math
from pathlib import Path

# 基于当前文件位置动态计算项目根目录
TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]   # taienergy-analytics
REPO_ROOT = TAIENERGY_DIR.parent
sys.path.insert(0, str(TAIENERGY_DIR))
sys.path.insert(0, str(REPO_ROOT))

from workflows.daily_v5 import clean_numeric_values
import pandas as pd
import numpy as np


def test_clean_numeric_values():
    """测试统一数据清洗函数 - pytest 风格（无返回值）"""
    
    test_cases = [
        # (输入, 期望输出, 描述)
        ([1, 2, 3], [1.0, 2.0, 3.0], "正常整数"),
        ([1.5, 2.5, 3.5], [1.5, 2.5, 3.5], "正常浮点数"),
        ([None, 1, None, 2], [1.0, 2.0], "包含 None"),
        ([1, float('nan'), 2], [1.0, 2.0], "包含 float NaN"),
        ([1, pd.NA, 2], [1.0, 2.0], "包含 pd.NA"),
        ([1, np.nan, 2], [1.0, 2.0], "包含 np.nan"),
        (["abc", 1, "def", 2], [1.0, 2.0], "包含字符串"),
        (["123", "45.6", "abc"], [123.0, 45.6], "数字字符串转数值"),
        ([None, pd.NA, float('nan'), "abc", 42], [42.0], "混合异常值"),
        ([], [], "空列表"),
        (None, [], "None 输入"),
    ]
    
    failed = 0
    
    for input_vals, expected, desc in test_cases:
        result = clean_numeric_values(input_vals)
        
        # 比较结果（处理浮点数精度）
        if len(result) != len(expected):
            match = False
        else:
            match = all(math.isclose(r, e, rel_tol=1e-9) for r, e in zip(result, expected))
        
        if not match:
            print(f"❌ FAIL: {desc}")
            print(f"   输入: {input_vals}")
            print(f"   期望: {expected}")
            print(f"   实际: {result}")
            failed += 1
    
    assert failed == 0, f"{failed} 个测试用例失败"


if __name__ == '__main__':
    # 本地运行入口
    test_clean_numeric_values()
    print("✅ 所有测试通过")
