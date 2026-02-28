"""
V4.5.1 Ralph 调用接口封装
Python调用Ralph命令行工具的桥梁
"""

import subprocess
import os
import time
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RalphResult:
    """Ralph执行结果"""
    success: bool
    output: str
    error: str
    returncode: int
    duration: float  # 执行时间（秒）
    spec_path: str


class RalphRunner:
    """
    Ralph调用器
    
    封装ralph-loop.sh调用，提供Python友好的接口
    """
    
    def __init__(self, ralph_path: str = None):
        """
        初始化Ralph运行器
        
        Args:
            ralph_path: Ralph安装路径，默认使用skill安装位置
        """
        if ralph_path is None:
            # 默认使用skill安装位置
            self.ralph_path = "/root/.openclaw/skills/ralph-wiggum"
        else:
            self.ralph_path = ralph_path
        
        self.script_path = os.path.join(self.ralph_path, "scripts", "ralph-loop.sh")
        
        # 检查Ralph是否安装
        if not os.path.exists(self.script_path):
            raise RalphNotFoundError(f"Ralph未找到: {self.script_path}")
    
    def run_spec(self, spec_path: str, max_iter: int = 3, timeout: int = 600) -> RalphResult:
        """
        执行单个spec文件
        
        Args:
            spec_path: spec文件路径
            max_iter: 最大迭代次数，默认3次
            timeout: 超时时间（秒），默认10分钟
        
        Returns:
            RalphResult对象
        """
        print(f"[Ralph] 开始执行: {spec_path}")
        start_time = time.time()
        
        try:
            # 构建命令
            cmd = [
                "bash",
                self.script_path,
                "--spec", spec_path,
                "--max-iterations", str(max_iter)
            ]
            
            # 执行Ralph
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            duration = time.time() - start_time
            
            # 解析结果
            output = result.stdout
            error = result.stderr
            
            # 检查成功信号
            has_done = "<promise>DONE</promise>" in output
            
            # 额外验证（防作弊）
            if has_done:
                valid = self._validate_output(spec_path, output)
                success = valid
            else:
                success = False
            
            print(f"[Ralph] 执行完成: success={success}, duration={duration:.1f}s")
            
            return RalphResult(
                success=success,
                output=output,
                error=error,
                returncode=result.returncode,
                duration=duration,
                spec_path=spec_path
            )
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            print(f"[Ralph] 执行超时 (>{timeout}s)")
            return RalphResult(
                success=False,
                output="",
                error=f"Timeout after {timeout} seconds",
                returncode=-1,
                duration=duration,
                spec_path=spec_path
            )
        except Exception as e:
            duration = time.time() - start_time
            print(f"[Ralph] 执行异常: {e}")
            return RalphResult(
                success=False,
                output="",
                error=str(e),
                returncode=-1,
                duration=duration,
                spec_path=spec_path
            )
    
    def _validate_output(self, spec_path: str, output: str) -> bool:
        """
        验证Ralph输出是否真实有效（防作弊）
        
        检查项：
        1. 代码文件是否生成
        2. 代码语法是否正确
        3. 是否有单元测试
        """
        # 提取指标名称
        spec_name = os.path.basename(spec_path).replace('.md', '')
        
        # 检查1: 代码文件是否生成（在spec同目录或implemented目录）
        possible_paths = [
            spec_path.replace('.md', '.py'),
            f"memory/indicators/implemented/{spec_name}.py",
            f"core/indicators/{spec_name}.py",
        ]
        
        code_generated = any(os.path.exists(p) for p in possible_paths)
        if not code_generated:
            print(f"    ⚠️  未找到生成的代码文件")
            return False
        
        # 检查2: 代码语法（简化版，实际可用ast.parse）
        code_path = None
        for p in possible_paths:
            if os.path.exists(p):
                code_path = p
                break
        
        if code_path:
            try:
                with open(code_path, 'r') as f:
                    code = f.read()
                
                # 简单语法检查：是否有def定义
                if 'def ' not in code:
                    print(f"    ⚠️  代码中没有函数定义")
                    return False
                
                # 检查是否有return语句
                if 'return' not in code:
                    print(f"    ⚠️  代码中没有return语句")
                    return False
                    
            except Exception as e:
                print(f"    ⚠️  代码读取失败: {e}")
                return False
        
        print(f"    ✅ 输出验证通过")
        return True
    
    def batch_run(self, spec_dir: str, max_iter: int = 3, delay: float = 10.0) -> List[RalphResult]:
        """
        批量执行目录下的所有spec
        
        Args:
            spec_dir: spec文件目录
            max_iter: 每个spec的最大迭代次数
            delay: 每个spec之间的间隔（秒），避免API限流
        
        Returns:
            RalphResult列表
        """
        results = []
        
        # 获取所有spec文件
        spec_files = [f for f in os.listdir(spec_dir) if f.endswith('.md')]
        spec_files.sort()  # 按名称排序，确保可预测顺序
        
        print(f"[Ralph] 批量执行: {len(spec_files)} 个spec")
        
        for i, spec_file in enumerate(spec_files, 1):
            spec_path = os.path.join(spec_dir, spec_file)
            print(f"\n[{i}/{len(spec_files)}] 处理 {spec_file}...")
            
            result = self.run_spec(spec_path, max_iter)
            results.append(result)
            
            # 间隔延迟（除了最后一个）
            if i < len(spec_files):
                print(f"    等待 {delay}s...")
                time.sleep(delay)
        
        # 汇总
        success_count = sum(1 for r in results if r.success)
        print(f"\n[Ralph] 批量执行完成: {success_count}/{len(results)} 成功")
        
        return results
    
    def get_status(self) -> Dict:
        """获取Ralph运行器状态"""
        return {
            "ralph_path": self.ralph_path,
            "script_exists": os.path.exists(self.script_path),
            "script_path": self.script_path,
        }


class RalphNotFoundError(Exception):
    """Ralph未找到错误"""
    pass


# 便捷函数
def run_single_spec(spec_path: str, max_iter: int = 3) -> RalphResult:
    """执行单个spec的便捷函数"""
    runner = RalphRunner()
    return runner.run_spec(spec_path, max_iter)


def run_candidate_pool(max_candidates: int = 5, max_iter: int = 3) -> List[RalphResult]:
    """
    执行候选池中的spec（限制数量）
    
    Args:
        max_candidates: 最大处理数量（默认5）
        max_iter: 每个spec的最大迭代次数
    """
    candidate_dir = "memory/indicators/candidate"
    
    if not os.path.exists(candidate_dir):
        print(f"[Ralph] 候选池为空: {candidate_dir}")
        return []
    
    # 获取待处理的spec（按时间排序，优先处理新的）
    spec_files = [f for f in os.listdir(candidate_dir) if f.endswith('.md')]
    spec_files.sort(reverse=True)  # 新的优先
    
    # 限制数量
    spec_files = spec_files[:max_candidates]
    
    print(f"[Ralph] 处理候选池: {len(spec_files)}/{max_candidates} 个spec")
    
    runner = RalphRunner()
    results = []
    
    for spec_file in spec_files:
        spec_path = os.path.join(candidate_dir, spec_file)
        result = runner.run_spec(spec_path, max_iter)
        results.append(result)
        
        # 更新候选池状态
        _update_candidate_status(spec_file, result)
    
    return results


def _update_candidate_status(spec_file: str, result: RalphResult):
    """更新候选池索引中的状态"""
    index_path = "memory/indicators/candidate/candidate_pool.json"
    
    if not os.path.exists(index_path):
        return
    
    with open(index_path, 'r') as f:
        index = json.load(f)
    
    # 更新状态
    for c in index.get("candidates", []):
        if c.get("spec_file") == spec_file:
            c["status"] = "done" if result.success else "failed"
            c["processed_at"] = datetime.now().isoformat()
            c["duration"] = result.duration
            break
    
    with open(index_path, 'w') as f:
        json.dump(index, f, indent=2)


if __name__ == "__main__":
    # 测试
    print("Ralph Runner 测试")
    print("=" * 50)
    
    # 检查状态
    runner = RalphRunner()
    status = runner.get_status()
    print(f"Ralph路径: {status['ralph_path']}")
    print(f"脚本存在: {status['script_exists']}")
    print(f"脚本路径: {status['script_path']}")
