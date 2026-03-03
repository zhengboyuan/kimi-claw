#!/usr/bin/env python3
"""
Registry Schema 校验测试
覆盖：缺字段、类型错误、合法样例通过
"""

import sys
from pathlib import Path

TEST_FILE = Path(__file__).resolve()
TAIENERGY_DIR = TEST_FILE.parents[2]
sys.path.insert(0, str(TAIENERGY_DIR))

from core.registry_validator import validate_indicator, validate_registry


def test_missing_required_fields():
    """测试缺少必需字段"""
    # 缺少 id
    indicator = {"name": "测试", "source": "user", "scope": "station", "level": "L1", "lifecycle_status": "pending", "computable": True}
    errors = validate_indicator("test_id", indicator)
    assert any("缺少必需字段" in e and "id" in e for e in errors), "应报告缺少 id 字段"
    
    # 缺少 computable
    indicator = {"id": "test", "name": "测试", "source": "user", "scope": "station", "level": "L1", "lifecycle_status": "pending"}
    errors = validate_indicator("test_id", indicator)
    assert any("缺少必需字段" in e and "computable" in e for e in errors), "应报告缺少 computable 字段"


def test_type_errors():
    """测试类型错误"""
    # computable 类型错误（应为 bool）
    indicator = {
        "id": "test",
        "name": "测试",
        "source": "user",
        "scope": "station",
        "level": "L1",
        "lifecycle_status": "pending",
        "computable": "yes"  # 错误：字符串而非布尔值
    }
    errors = validate_indicator("test_id", indicator)
    assert any("必须是布尔值" in e for e in errors), "应报告 computable 类型错误"
    
    # name 类型错误（应为 string）
    indicator = {
        "id": "test",
        "name": 123,  # 错误：数字而非字符串
        "source": "user",
        "scope": "station",
        "level": "L1",
        "lifecycle_status": "pending",
        "computable": True
    }
    errors = validate_indicator("test_id", indicator)
    assert any("必须是字符串" in e for e in errors), "应报告 name 类型错误"


def test_valid_indicator():
    """测试合法样例通过"""
    indicator = {
        "id": "power_active",
        "name": "有功功率",
        "source": "user",
        "scope": "inverter",
        "level": "L1",
        "lifecycle_status": "approved",
        "computable": True,
        "formula": "ai56",
        "unit": "kW"
    }
    errors = validate_indicator("power_active", indicator)
    assert len(errors) == 0, f"合法样例应无错误，但有: {errors}"


def test_enum_validation():
    """测试枚举值校验"""
    # 非法 source
    indicator = {
        "id": "test",
        "name": "测试",
        "source": "invalid_source",  # 错误
        "scope": "station",
        "level": "L1",
        "lifecycle_status": "pending",
        "computable": True
    }
    errors = validate_indicator("test_id", indicator)
    assert any("source" in e and "非法" in e for e in errors), "应报告 source 枚举错误"
    
    # 非法 level
    indicator = {
        "id": "test",
        "name": "测试",
        "source": "user",
        "scope": "station",
        "level": "L4",  # 错误
        "lifecycle_status": "pending",
        "computable": True
    }
    errors = validate_indicator("test_id", indicator)
    assert any("level" in e and "非法" in e for e in errors), "应报告 level 枚举错误"


def test_id_mismatch():
    """测试 id 与键名不匹配"""
    indicator = {
        "id": "wrong_id",  # 与键名不同
        "name": "测试",
        "source": "user",
        "scope": "station",
        "level": "L1",
        "lifecycle_status": "pending",
        "computable": True
    }
    errors = validate_indicator("correct_id", indicator)
    assert any("id" in e and "不匹配" in e for e in errors), "应报告 id 不匹配"


if __name__ == '__main__':
    # 本地运行入口
    test_missing_required_fields()
    test_type_errors()
    test_valid_indicator()
    test_enum_validation()
    test_id_mismatch()
    print("✅ 所有测试通过")
