"""
Registry Schema Validator
校验 indicator registry 的完整性和合法性
"""

import json
import os
from typing import Dict, List, Tuple


def load_schema() -> Dict:
    """加载 registry schema"""
    schema_path = os.path.join(
        os.path.dirname(__file__), 
        '..', 'config', 'indicators', 'registry.schema.json'
    )
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_registry() -> Dict:
    """加载 registry 数据"""
    registry_path = os.path.join(
        os.path.dirname(__file__),
        '..', 'config', 'indicators', 'registry.json'
    )
    with open(registry_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def validate_indicator(indicator_id: str, indicator: Dict) -> List[str]:
    """
    校验单个 indicator
    返回错误列表，空列表表示校验通过
    """
    errors = []
    
    # 必需字段检查
    required_fields = ['id', 'name', 'source', 'scope', 'level', 'lifecycle_status']
    for field in required_fields:
        if field not in indicator:
            errors.append(f"[{indicator_id}] 缺少必需字段: {field}")
    
    # id 必须匹配键名
    if indicator.get('id') != indicator_id:
        errors.append(f"[{indicator_id}] id 字段与键名不匹配: {indicator.get('id')}")
    
    # source 枚举值检查
    valid_sources = ['user', 'constructed', 'llm']
    if indicator.get('source') not in valid_sources:
        errors.append(f"[{indicator_id}] 非法 source: {indicator.get('source')}, 必须是 {valid_sources}")
    
    # scope 枚举值检查
    valid_scopes = ['station', 'inverter', 'string']
    if indicator.get('scope') not in valid_scopes:
        errors.append(f"[{indicator_id}] 非法 scope: {indicator.get('scope')}, 必须是 {valid_scopes}")
    
    # level 枚举值检查
    valid_levels = ['L1', 'L2', 'L3']
    if indicator.get('level') not in valid_levels:
        errors.append(f"[{indicator_id}] 非法 level: {indicator.get('level')}, 必须是 {valid_levels}")
    
    # lifecycle_status 枚举值检查
    valid_statuses = ['pending', 'approved', 'retired']
    if indicator.get('lifecycle_status') not in valid_statuses:
        errors.append(f"[{indicator_id}] 非法 lifecycle_status: {indicator.get('lifecycle_status')}, 必须是 {valid_statuses}")
    
    # computable 类型检查
    if 'computable' in indicator and not isinstance(indicator['computable'], bool):
        errors.append(f"[{indicator_id}] computable 必须是布尔值")
    
    return errors


def validate_registry() -> Tuple[bool, List[str]]:
    """
    校验整个 registry
    返回: (是否通过, 错误列表)
    """
    try:
        registry = load_registry()
    except Exception as e:
        return False, [f"无法加载 registry: {e}"]
    
    errors = []
    
    # 检查顶层结构
    if 'version' not in registry:
        errors.append("缺少 version 字段")
    
    if 'updated_at' not in registry:
        errors.append("缺少 updated_at 字段")
    
    if 'indicators' not in registry:
        errors.append("缺少 indicators 字段")
        return False, errors
    
    indicators = registry.get('indicators', {})
    
    if not indicators:
        errors.append("indicators 为空")
    
    # 校验每个 indicator
    for indicator_id, indicator in indicators.items():
        indicator_errors = validate_indicator(indicator_id, indicator)
        errors.extend(indicator_errors)
    
    return len(errors) == 0, errors


def main():
    """主函数：运行校验并输出结果"""
    print("=" * 60)
    print("Registry Schema Validation")
    print("=" * 60)
    
    passed, errors = validate_registry()
    
    if passed:
        print("✅ Registry 校验通过")
        registry = load_registry()
        print(f"   版本: {registry.get('version')}")
        print(f"   更新时间: {registry.get('updated_at')}")
        print(f"   指标数量: {len(registry.get('indicators', {}))}")
    else:
        print("❌ Registry 校验失败")
        print(f"   发现 {len(errors)} 个错误:")
        for i, error in enumerate(errors[:10], 1):  # 最多显示10个错误
            print(f"   {i}. {error}")
        if len(errors) > 10:
            print(f"   ... 还有 {len(errors) - 10} 个错误")
    
    print("=" * 60)
    return 0 if passed else 1


if __name__ == '__main__':
    exit(main())
