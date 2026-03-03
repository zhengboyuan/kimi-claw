"""
Registry Schema Validator
校验 indicator registry 的完整性和合法性
"""

import json
import os
from typing import Dict, List, Tuple


def get_schema_path() -> str:
    """获取 schema 文件路径"""
    return os.path.join(
        os.path.dirname(__file__),
        '..', 'config', 'indicators', 'registry.schema.json'
    )


def get_registry_path() -> str:
    """获取 registry 文件路径"""
    return os.path.join(
        os.path.dirname(__file__),
        '..', 'config', 'indicators', 'registry.json'
    )


def load_schema() -> Dict:
    """加载 registry schema"""
    schema_path = get_schema_path()
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def validate_type(value, expected_type: str, field_name: str) -> List[str]:
    """验证单个字段类型"""
    errors = []
    
    if expected_type == 'string':
        if not isinstance(value, str):
            errors.append(f"字段 '{field_name}' 必须是字符串，实际是 {type(value).__name__}")
    elif expected_type == 'boolean':
        if not isinstance(value, bool):
            errors.append(f"字段 '{field_name}' 必须是布尔值，实际是 {type(value).__name__}")
    elif expected_type == 'array':
        if not isinstance(value, list):
            errors.append(f"字段 '{field_name}' 必须是数组，实际是 {type(value).__name__}")
    elif expected_type == 'object':
        if not isinstance(value, dict):
            errors.append(f"字段 '{field_name}' 必须是对象，实际是 {type(value).__name__}")
    
    return errors


def validate_indicator(indicator_id: str, indicator: Dict) -> List[str]:
    """
    校验单个 indicator
    返回错误列表，空列表表示校验通过
    """
    errors = []
    
    # 必需字段检查
    required_fields = {
        'id': 'string',
        'name': 'string',
        'source': 'string',
        'scope': 'string',
        'level': 'string',
        'lifecycle_status': 'string',
        'computable': 'boolean'
    }
    
    for field, field_type in required_fields.items():
        if field not in indicator:
            errors.append(f"[{indicator_id}] 缺少必需字段: '{field}'")
        else:
            # 类型校验
            type_errors = validate_type(indicator[field], field_type, f"{indicator_id}.{field}")
            errors.extend(type_errors)
    
    # id 必须匹配键名
    if 'id' in indicator and indicator['id'] != indicator_id:
        errors.append(f"[{indicator_id}] 字段 'id' 值与键名不匹配: '{indicator['id']}' != '{indicator_id}'")
    
    # source 枚举值检查
    if 'source' in indicator and isinstance(indicator['source'], str):
        valid_sources = ['user', 'constructed', 'llm']
        if indicator['source'] not in valid_sources:
            errors.append(f"[{indicator_id}] 字段 'source' 值非法: '{indicator['source']}'，必须是 {valid_sources}")
    
    # scope 枚举值检查
    if 'scope' in indicator and isinstance(indicator['scope'], str):
        valid_scopes = ['station', 'inverter', 'string']
        if indicator['scope'] not in valid_scopes:
            errors.append(f"[{indicator_id}] 字段 'scope' 值非法: '{indicator['scope']}'，必须是 {valid_scopes}")
    
    # level 枚举值检查
    if 'level' in indicator and isinstance(indicator['level'], str):
        valid_levels = ['L1', 'L2', 'L3']
        if indicator['level'] not in valid_levels:
            errors.append(f"[{indicator_id}] 字段 'level' 值非法: '{indicator['level']}'，必须是 {valid_levels}")
    
    # lifecycle_status 枚举值检查
    if 'lifecycle_status' in indicator and isinstance(indicator['lifecycle_status'], str):
        valid_statuses = ['pending', 'approved', 'retired']
        if indicator['lifecycle_status'] not in valid_statuses:
            errors.append(f"[{indicator_id}] 字段 'lifecycle_status' 值非法: '{indicator['lifecycle_status']}'，必须是 {valid_statuses}")
    
    return errors


def validate_registry(registry_data: Dict = None) -> Tuple[bool, List[str]]:
    """
    校验整个 registry
    返回: (是否通过, 错误列表)
    """
    if registry_data is None:
        try:
            registry_path = get_registry_path()
            with open(registry_path, 'r', encoding='utf-8') as f:
                registry_data = json.load(f)
        except Exception as e:
            return False, [f"无法加载 registry: {e}"]
    
    errors = []
    
    # 检查顶层结构
    if 'version' not in registry_data:
        errors.append("缺少必需字段: 'version'")
    elif not isinstance(registry_data['version'], str):
        errors.append("字段 'version' 必须是字符串")
    
    if 'updated_at' not in registry_data:
        errors.append("缺少必需字段: 'updated_at'")
    
    if 'indicators' not in registry_data:
        errors.append("缺少必需字段: 'indicators'")
        return False, errors
    
    if not isinstance(registry_data['indicators'], dict):
        errors.append("字段 'indicators' 必须是对象")
        return False, errors
    
    indicators = registry_data['indicators']
    
    if not indicators:
        errors.append("字段 'indicators' 为空")
    
    # 校验每个 indicator
    for indicator_id, indicator in indicators.items():
        if not isinstance(indicator, dict):
            errors.append(f"[{indicator_id}] 必须是对象")
            continue
        
        indicator_errors = validate_indicator(indicator_id, indicator)
        errors.extend(indicator_errors)
    
    return len(errors) == 0, errors


class RegistryValidationError(Exception):
    """Registry 校验错误"""
    pass


def validate_registry_strict(registry_data: Dict = None):
    """
    严格校验 registry，失败时抛出带字段名的明确错误
    """
    passed, errors = validate_registry(registry_data)
    
    if not passed:
        error_msg = "Registry 校验失败:\n"
        for i, error in enumerate(errors[:10], 1):
            error_msg += f"  {i}. {error}\n"
        if len(errors) > 10:
            error_msg += f"  ... 还有 {len(errors) - 10} 个错误\n"
        raise RegistryValidationError(error_msg)
    
    return True


if __name__ == '__main__':
    print("=" * 60)
    print("Registry Schema Validation")
    print("=" * 60)
    
    try:
        validate_registry_strict()
        print("✅ Registry 校验通过")
    except RegistryValidationError as e:
        print(e)
        exit(1)
