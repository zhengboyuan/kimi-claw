"""
分析器基类
定义所有分析器的通用接口
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any
import pandas as pd


class BaseAnalyzer(ABC):
    """分析器基类"""
    
    def __init__(self, indicator_code: str, indicator_name: str):
        self.indicator_code = indicator_code
        self.indicator_name = indicator_name
        self.history_data = []  # 历史数据累积
        self.analysis_results = {}  # 分析结果
    
    @abstractmethod
    def analyze(self, new_data: pd.DataFrame) -> Dict[str, Any]:
        """
        执行分析
        
        Args:
            new_data: 新数据 DataFrame，包含 ts 和 value 列
        
        Returns:
            分析结果字典
        """
        pass
    
    @abstractmethod
    def update_model(self, new_data: pd.DataFrame):
        """更新内部模型"""
        pass
    
    def get_summary(self) -> str:
        """获取分析摘要"""
        return f"{self.indicator_name}({self.indicator_code}): 基础分析器"


class RuleEngineAnalyzer(BaseAnalyzer):
    """
    规则引擎分析器（轻量级）
    用于简单规则判断，不走 LLM
    """
    
    def analyze(self, new_data: pd.DataFrame) -> Dict[str, Any]:
        """基础统计分析"""
        if new_data.empty:
            return {"status": "no_data"}
        
        values = new_data['value'].dropna()
        
        return {
            "status": "ok",
            "count": len(values),
            "mean": values.mean(),
            "std": values.std(),
            "min": values.min(),
            "max": values.max(),
            "non_zero_ratio": (values > 0).sum() / len(values) if len(values) > 0 else 0
        }
    
    def update_model(self, new_data: pd.DataFrame):
        """累积数据"""
        self.history_data.append(new_data)


class LLMAgentAnalyzer(BaseAnalyzer):
    """
    LLM 代理分析器（重量级）
    用于复杂分析，需要调用 LLM
    """
    
    def __init__(self, indicator_code: str, indicator_name: str):
        super().__init__(indicator_code, indicator_name)
        self.context = {}  # 上下文信息
    
    def set_context(self, context: Dict):
        """设置分析上下文"""
        self.context = context
    
    def analyze(self, new_data: pd.DataFrame) -> Dict[str, Any]:
        """
        执行深度分析（需要子类实现 LLM 调用）
        """
        raise NotImplementedError("LLM 分析器需要子类实现")
    
    def update_model(self, new_data: pd.DataFrame):
        """更新模型"""
        self.history_data.append(new_data)