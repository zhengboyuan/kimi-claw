"""
指标实现: ai56_rolling_std
Ralph自动生成（修复版）
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List

def calculate_ai56_rolling_std(data: dict) -> float:
    """
    计算ai56的24小时滚动标准差
    
    Args:
        data: 包含'ai56'键的字典，值为列表
    
    Returns:
        滚动标准差值
    
    Example:
        >>> calculate_ai56_rolling_std({'ai56': [10, 11, 12, 11, 10]})
        0.8366600265340756
    """
    # 边界检查1: 空数据
    if not isinstance(data, dict):
        return 0.0
    
    # 边界检查2: 缺少键
    if 'ai56' not in data:
        return 0.0
    
    values = data['ai56']
    
    # 边界检查3: 空列表
    if not isinstance(values, (list, tuple)) or len(values) == 0:
        return 0.0
    
    # 边界检查4: 单值
    if len(values) < 2:
        return 0.0
    
    # 处理None和异常值
    clean_values = []
    for v in values:
        if v is None:
            continue
        try:
            fv = float(v)
            if not np.isnan(fv) and not np.isinf(fv):
                clean_values.append(fv)
        except:
            continue
    
    if len(clean_values) < 2:
        return 0.0
    
    # 计算滚动标准差（最后24个值）
    window = min(24, len(clean_values))
    recent_values = clean_values[-window:]
    
    return float(np.std(recent_values, ddof=1))


# 单元测试
def test_calculate():
    """测试函数"""
    # 测试1: 正常数据
    result = calculate_ai56_rolling_std({'ai56': [10, 11, 12, 11, 10]})
    assert result > 0, "正常数据应该返回正数"
    
    # 测试2: 空数据
    result = calculate_ai56_rolling_std({})
    assert result == 0.0, "空数据应该返回0"
    
    # 测试3: 单值
    result = calculate_ai56_rolling_std({'ai56': [10]})
    assert result == 0.0, "单值标准差应为0"
    
    # 测试4: None值
    result = calculate_ai56_rolling_std({'ai56': [1, None, 3]})
    assert result >= 0, "包含None应该正常处理"
    
    print("✅ 所有测试通过")

if __name__ == "__main__":
    test_calculate()
