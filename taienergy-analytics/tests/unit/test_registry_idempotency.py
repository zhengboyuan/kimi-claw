#!/usr/bin/env python3
"""
P1-5 幂等性测试
验证 update_registry 的幂等性和唯一性
"""

import sys
import os
sys.path.insert(0, '/root/.openclaw/workspace/taienergy-analytics')

from core.memory_system import MemorySystem


def test_update_registry_idempotency():
    """测试 update_registry 幂等性"""
    print("\n" + "=" * 60)
    print("测试 update_registry 幂等性")
    print("=" * 60)
    
    memory = MemorySystem()
    
    # 测试指标
    test_indicator = {
        "id": "test_power_active",
        "name": "测试有功功率",
        "source": "constructed",
        "scope": "inverter",
        "level": "L1",
        "lifecycle_status": "pending",
        "computable": True,
        "unit": "kW"
    }
    
    # 第一次提交
    print("\n[测试1] 第一次提交新指标...")
    result1 = memory.update_registry(test_indicator)
    registry1 = memory.read_registry()
    indicator1 = registry1["indicators"].get("test_power_active")
    created_at_1 = indicator1.get("created_at")
    print(f"  结果: {'✅ 成功' if result1 else '❌ 失败'}")
    print(f"  created_at: {created_at_1}")
    
    # 第二次提交（相同 id）
    print("\n[测试2] 第二次提交相同 id（应更新而非新增）...")
    test_indicator["name"] = "测试有功功率（已更新）"
    result2 = memory.update_registry(test_indicator)
    registry2 = memory.read_registry()
    indicator2 = registry2["indicators"].get("test_power_active")
    created_at_2 = indicator2.get("created_at")
    updated_at_2 = indicator2.get("updated_at")
    print(f"  结果: {'✅ 成功' if result2 else '❌ 失败'}")
    print(f"  name: {indicator2.get('name')}")
    print(f"  created_at: {created_at_2}（应与第一次相同）")
    print(f"  updated_at: {updated_at_2}")
    
    # 验证幂等性
    assert created_at_1 == created_at_2, "created_at 不应被覆盖"
    assert indicator2.get("name") == "测试有功功率（已更新）", "name 应被更新"
    print("  ✅ 幂等性验证通过：created_at 保留，字段更新")
    
    # 第三次提交（验证不新增重复项）
    print("\n[测试3] 第三次提交，验证指标数量...")
    registry3 = memory.read_registry()
    count_before = len(registry3["indicators"])
    memory.update_registry(test_indicator)
    registry4 = memory.read_registry()
    count_after = len(registry4["indicators"])
    print(f"  指标数量: {count_before} -> {count_after}")
    assert count_before == count_after, "重复提交不应新增指标"
    print("  ✅ 唯一性验证通过：指标数量未增加")
    
    # 清理测试数据
    print("\n[清理] 删除测试指标...")
    registry = memory.read_registry()
    if "test_power_active" in registry["indicators"]:
        del registry["indicators"]["test_power_active"]
        from core.indicator_registry import write_registry
        write_registry(registry)
        print("  ✅ 测试数据已清理")
    
    return True


def test_update_registry_validation():
    """测试 update_registry 参数校验"""
    print("\n" + "=" * 60)
    print("测试 update_registry 参数校验")
    print("=" * 60)
    
    memory = MemorySystem()
    
    # 测试缺少 id
    print("\n[测试4] 提交缺少 id 的指标...")
    invalid_indicator = {"name": "无效指标"}
    result = memory.update_registry(invalid_indicator)
    print(f"  结果: {'❌ 被拒绝（预期）' if not result else '✅ 意外通过'}")
    assert not result, "缺少 id 应被拒绝"
    
    # 测试无效 id 格式
    print("\n[测试5] 提交无效 id 格式的指标...")
    invalid_id_indicator = {
        "id": "test-invalid@id",
        "name": "无效id格式",
        "source": "constructed",
        "scope": "inverter",
        "level": "L1",
        "lifecycle_status": "pending"
    }
    result = memory.update_registry(invalid_id_indicator)
    print(f"  结果: {'❌ 被拒绝（预期）' if not result else '✅ 意外通过'}")
    assert not result, "无效 id 格式应被拒绝"
    
    print("\n  ✅ 参数校验测试通过")
    return True


if __name__ == '__main__':
    print("=" * 60)
    print("P1-5 测试: update_registry 幂等性与唯一性")
    print("=" * 60)
    
    tests = [
        test_update_registry_idempotency,
        test_update_registry_validation,
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
            print(f"\n❌ {test.__name__} 失败: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"测试结果: {passed} 通过, {failed} 失败")
    print("=" * 60)
    
    sys.exit(0 if failed == 0 else 1)
