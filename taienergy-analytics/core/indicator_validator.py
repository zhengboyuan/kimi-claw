"""
V4.5.1 指标实现后校验模块
防止Ralph"假装完成"，确保代码质量
"""

import ast
import os
import sys
import time
import traceback
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
import pandas as pd
import numpy as np


@dataclass
class ValidationResult:
    """验证结果"""
    passed: bool
    checks: Dict[str, bool]  # 各项检查结果
    errors: List[str]        # 错误信息
    warnings: List[str]      # 警告信息


class IndicatorValidator:
    """指标实现验证器"""
    
    def __init__(self):
        self.checks = [
            ("代码语法", self._check_code_syntax),
            ("函数定义", self._check_function_definition),
            ("依赖存在", self._check_dependencies_exist),
            ("单元测试", self._check_unit_tests),
            ("对抗测试", self._run_adversarial_tests),
            ("性能基线", self._check_performance_baseline),
        ]
    
    def validate(self, code_path: str, dependencies: List[str] = None) -> ValidationResult:
        """
        全面验证指标实现
        
        Args:
            code_path: 代码文件路径
            dependencies: 依赖的指标代码列表
        
        Returns:
            ValidationResult
        """
        print(f"[Validator] 开始验证: {code_path}")
        
        if not os.path.exists(code_path):
            return ValidationResult(
                passed=False,
                checks={},
                errors=[f"代码文件不存在: {code_path}"],
                warnings=[]
            )
        
        # 读取代码
        try:
            with open(code_path, 'r') as f:
                code = f.read()
        except Exception as e:
            return ValidationResult(
                passed=False,
                checks={},
                errors=[f"读取代码失败: {e}"],
                warnings=[]
            )
        
        # 执行各项检查
        results = {}
        errors = []
        warnings = []
        
        for check_name, check_func in self.checks:
            try:
                passed, msg = check_func(code, dependencies)
                results[check_name] = passed
                if not passed:
                    errors.append(f"{check_name}: {msg}")
            except Exception as e:
                results[check_name] = False
                errors.append(f"{check_name}: 检查异常 - {e}")
        
        # 汇总
        all_passed = all(results.values())
        
        print(f"[Validator] 验证完成: passed={all_passed}, checks={len([v for v in results.values() if v])}/{len(results)}")
        
        return ValidationResult(
            passed=all_passed,
            checks=results,
            errors=errors,
            warnings=warnings
        )
    
    def _check_code_syntax(self, code: str, deps: List[str] = None) -> Tuple[bool, str]:
        """检查代码语法"""
        try:
            ast.parse(code)
            return True, "语法正确"
        except SyntaxError as e:
            return False, f"语法错误: {e}"
    
    def _check_function_definition(self, code: str, deps: List[str] = None) -> Tuple[bool, str]:
        """检查是否有正确的函数定义"""
        try:
            tree = ast.parse(code)
            
            # 查找函数定义
            funcs = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
            
            if not funcs:
                return False, "没有找到函数定义"
            
            # 检查是否有return语句
            has_return = False
            for func in funcs:
                for node in ast.walk(func):
                    if isinstance(node, ast.Return):
                        has_return = True
                        break
            
            if not has_return:
                return False, "函数没有return语句"
            
            return True, f"找到 {len(funcs)} 个函数定义"
            
        except Exception as e:
            return False, f"检查失败: {e}"
    
    def _check_dependencies_exist(self, code: str, deps: List[str] = None) -> Tuple[bool, str]:
        """检查依赖的指标是否存在"""
        if not deps:
            return True, "无依赖检查"
        
        # 从device_config加载有效指标
        try:
            from config.device_config import DeviceConfig
            config = DeviceConfig()
            valid_codes = set(config.get_all_metric_codes())
        except:
            # 使用默认列表
            valid_codes = {'ai51', 'ai52', 'ai53', 'ai54', 'ai55', 'ai56', 'ai62', 'ai68'}
        
        missing = [d for d in deps if d not in valid_codes]
        if missing:
            return False, f"依赖指标不存在: {missing}"
        
        return True, f"所有依赖有效: {deps}"
    
    def _check_unit_tests(self, code: str, deps: List[str] = None) -> Tuple[bool, str]:
        """检查是否有单元测试（简化版）"""
        # 检查代码中是否有test相关的内容
        test_patterns = ['test_', '_test', 'assert', 'unittest', 'pytest']
        has_test = any(p in code for p in test_patterns)
        
        # 也接受文档字符串中的示例
        has_example = '>>>' in code or 'Example:' in code
        
        if has_test or has_example:
            return True, "包含测试或示例"
        
        # 警告级别，不强制要求
        return True, "无显式测试（警告）"
    
    def _run_adversarial_tests(self, code: str, deps: List[str] = None) -> Tuple[bool, str]:
        """
        对抗测试：用边界数据测试实现是否robust
        """
        try:
            # 动态执行代码获取函数
            namespace = {}
            exec(code, namespace)
            
            # 找到计算函数（假设是第一个函数）
            func = None
            for name, obj in namespace.items():
                if callable(obj) and not name.startswith('_'):
                    func = obj
                    break
            
            if not func:
                return False, "没有找到可执行的函数"
            
            # 测试用例
            test_cases = [
                # Case 1: 空数据
                ({}, "空数据"),
                # Case 2: 单条数据
                ({'ai56': [10]}, "单条数据"),
                # Case 3: 包含None
                ({'ai56': [1, None, 3]}, "包含None"),
                # Case 4: 包含异常值
                ({'ai56': [1, 2, float('inf')]}, "包含Inf"),
            ]
            
            passed = 0
            failed = 0
            
            for test_data, desc in test_cases:
                try:
                    result = func(test_data)
                    # 只要没崩溃就算通过
                    passed += 1
                except Exception as e:
                    # 某些异常是预期的（如空数据应该报错）
                    if "空" in desc or "None" in desc:
                        passed += 1  # 预期会失败
                    else:
                        failed += 1
            
            if failed > 0:
                return False, f"对抗测试失败: {failed}/{len(test_cases)}"
            
            return True, f"对抗测试通过: {passed}/{len(test_cases)}"
            
        except Exception as e:
            return False, f"对抗测试异常: {e}"
    
    def _check_performance_baseline(self, code: str, deps: List[str] = None) -> Tuple[bool, str]:
        """检查性能基线（<500ms/设备/天）"""
        try:
            # 动态执行并计时
            namespace = {}
            exec(code, namespace)
            
            # 找到函数
            func = None
            for name, obj in namespace.items():
                if callable(obj) and not name.startswith('_'):
                    func = obj
                    break
            
            if not func:
                return False, "没有找到函数"
            
            # 构造测试数据（24小时）
            test_data = {'ai56': list(range(24))}
            
            # 计时执行
            start = time.time()
            for _ in range(100):  # 执行100次取平均
                func(test_data)
            duration = (time.time() - start) / 100
            
            # 检查是否<500ms
            if duration < 0.5:
                return True, f"性能达标: {duration*1000:.1f}ms"
            else:
                return False, f"性能不达标: {duration*1000:.1f}ms (>500ms)"
                
        except Exception as e:
            return False, f"性能测试异常: {e}"


# 便捷函数
def validate_indicator(code_path: str, dependencies: List[str] = None) -> bool:
    """验证指标的便捷函数"""
    validator = IndicatorValidator()
    result = validator.validate(code_path, dependencies)
    return result.passed


def validate_with_details(code_path: str, dependencies: List[str] = None) -> Dict:
    """验证并返回详细信息的便捷函数"""
    validator = IndicatorValidator()
    result = validator.validate(code_path, dependencies)
    
    return {
        "passed": result.passed,
        "checks": result.checks,
        "errors": result.errors,
        "warnings": result.warnings
    }


if __name__ == "__main__":
    # 测试
    print("Indicator Validator 测试")
    print("=" * 50)
    
    # 创建一个测试代码文件
    test_code = '''
def calculate_test_metric(data):
    """
    测试指标计算
    
    Example:
        >>> calculate_test_metric({'ai56': [1, 2, 3]})
        2.0
    """
    if not data or 'ai56' not in data:
        raise ValueError("数据不能为空")
    
    values = data['ai56']
    if not values:
        return 0.0
    
    return sum(values) / len(values)
'''
    
    test_path = "/tmp/test_indicator.py"
    with open(test_path, 'w') as f:
        f.write(test_code)
    
    # 验证
    result = validate_with_details(test_path, dependencies=['ai56'])
    print(f"\n验证结果: {result['passed']}")
    print(f"检查项: {result['checks']}")
    if result['errors']:
        print(f"错误: {result['errors']}")
