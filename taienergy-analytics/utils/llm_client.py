"""
LLM客户端封装
支持多种LLM提供商
"""
import os
from typing import Optional


class LLMClient:
    """
    LLM客户端
    
    支持：
    - OpenAI API
    - 其他兼容OpenAI接口的提供商
    """
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.base_url = base_url or os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
        
        # 延迟导入，避免没有安装依赖时报错
        self._client = None
    
    @property
    def client(self):
        """延迟初始化client"""
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(
                    api_key=self.api_key,
                    base_url=self.base_url
                )
            except ImportError:
                raise ImportError("请安装openai: pip install openai")
        return self._client
    
    def complete(self, prompt: str, temperature: float = 0.7, max_tokens: int = 4000) -> str:
        """
        调用LLM完成文本生成
        
        Args:
            prompt: 提示词
            temperature: 温度参数
            max_tokens: 最大token数
        
        Returns:
            LLM生成的文本
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",  # 使用较便宜的模型
                messages=[
                    {"role": "system", "content": "你是一位专业的光伏设备数据分析专家。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            print(f"[LLM调用错误] {e}")
            # 返回一个默认响应，避免流程中断
            return '{"composite_indicators": [], "error": "' + str(e) + '"}'
    
    def is_available(self) -> bool:
        """检查LLM是否可用"""
        return self.api_key is not None
