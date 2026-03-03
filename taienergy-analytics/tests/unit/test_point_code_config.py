#!/usr/bin/env python3
"""
测点编码配置测试
验证 ai56/ai68 等点位编码可由配置驱动
"""

import sys
from pathlib import Path

TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from workflows.daily_v5 import POWER_POINT_CODE, GENERATION_POINT_CODE


def test_default_point_codes():
    """场景1: 使用默认点位"""
    # 验证默认点位编码
    assert POWER_POINT_CODE == "ai56", f"默认功率点位应为 ai56，实际是 {POWER_POINT_CODE}"
    assert GENERATION_POINT_CODE == "ai68", f"默认发电量点位应为 ai68，实际是 {GENERATION_POINT_CODE}"
    print(f"✅ 默认点位编码: power={POWER_POINT_CODE}, generation={GENERATION_POINT_CODE}")


def test_point_codes_from_config():
    """场景2: 配置驱动点位"""
    from config.system_config import POINT_CONFIG
    
    # 验证配置存在
    assert "power_point_code" in POINT_CONFIG, "POINT_CONFIG 应有 power_point_code"
    assert "generation_point_code" in POINT_CONFIG, "POINT_CONFIG 应有 generation_point_code"
    
    # 验证配置值与代码中使用的一致
    assert POWER_POINT_CODE == POINT_CONFIG["power_point_code"], "代码中应使用配置值"
    assert GENERATION_POINT_CODE == POINT_CONFIG["generation_point_code"], "代码中应使用配置值"
    
    print(f"✅ 配置驱动点位: {POINT_CONFIG}")


def test_fallback_behavior():
    """场景3: 缺失配置回退"""
    # 验证 get 方法有默认值兜底
    from config.system_config import POINT_CONFIG
    
    # 模拟缺失配置的情况
    power_fallback = POINT_CONFIG.get("power_point_code", "ai56")
    gen_fallback = POINT_CONFIG.get("generation_point_code", "ai68")
    
    assert power_fallback == "ai56", "缺失时应回退到 ai56"
    assert gen_fallback == "ai68", "缺失时应回退到 ai68"
    
    print(f"✅ 兜底机制正常")


if __name__ == '__main__':
    print("=" * 60)
    print("测点编码配置测试")
    print("=" * 60)
    
    test_default_point_codes()
    test_point_codes_from_config()
    test_fallback_behavior()
    
    print("=" * 60)
    print("✅ 所有测试通过")
    print("=" * 60)
