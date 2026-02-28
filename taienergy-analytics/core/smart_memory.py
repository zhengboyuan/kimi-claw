"""
智能记忆写入器
核心逻辑：读取→LLM决策→条件写入
避免写入废话，保持记忆质量
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SmartMemoryWriter:
    """
    智能记忆写入器
    
    流程：
    1. 读取现有知识
    2. LLM判断新发现是否值得记录
    3. 根据决策执行写入或跳过
    """
    
    def __init__(self, llm_client=None):
        self.llm = llm_client
        self.decision_log = []
    
    def write(self, file_path: str, new_data: Any, context: Dict) -> bool:
        """
        智能写入主入口
        
        Args:
            file_path: 目标文件路径
            new_data: 新发现的数据
            context: 上下文信息（如device_sn, date等）
            
        Returns:
            bool: 是否实际写入
        """
        # Step 1: 读取现有知识
        existing = self._read_existing(file_path)
        
        # Step 2: 决策（大模型判断）
        decision = self._should_write(file_path, existing, new_data, context)
        
        # Step 3: 执行决策
        if decision["action"] == "SKIP":
            logger.info(f"[SKIP] {file_path}: {decision['reason']}")
            self._log_decision(file_path, "SKIP", decision["reason"])
            return False
        
        elif decision["action"] == "WRITE":
            self._do_write(file_path, new_data)
            logger.info(f"[WRITE] {file_path}: {decision['reason']}")
            self._log_decision(file_path, "WRITE", decision["reason"])
            return True
        
        elif decision["action"] == "MERGE":
            merged = self._merge_data(existing, new_data, decision["merge_strategy"])
            self._do_write(file_path, merged)
            logger.info(f"[MERGE] {file_path}: {decision['reason']}")
            self._log_decision(file_path, "MERGE", decision["reason"])
            return True
        
        elif decision["action"] == "APPEND":
            combined = self._append_data(existing, new_data)
            self._do_write(file_path, combined)
            logger.info(f"[APPEND] {file_path}: {decision['reason']}")
            self._log_decision(file_path, "APPEND", decision["reason"])
            return True
        
        return False
    
    def _read_existing(self, file_path: str) -> Any:
        """读取现有文件内容"""
        if not os.path.exists(file_path):
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    
    def _should_write(self, file_path: str, existing: Any, new_data: Any, context: Dict) -> Dict:
        """
        决策：是否值得写入？
        
        根据文件类型使用不同的决策逻辑
        """
        # 认知层文件：大模型决策
        if "cognitive" in file_path:
            return self._llm_decide_cognitive(existing, new_data, context)
        
        # 设备层文件：混合决策
        elif "devices" in file_path:
            return self._decide_device_memory(existing, new_data, context)
        
        # 日报层：直接写入
        elif "daily" in file_path:
            return {"action": "WRITE", "reason": "日报必须每日生成"}
        
        # 默认：直接写入
        else:
            return {"action": "WRITE", "reason": "默认策略"}
    
    def _llm_decide_cognitive(self, existing: Any, new_data: Any, context: Dict) -> Dict:
        """
        认知层决策：大模型判断新发现是否值得记录
        
        如果LLM不可用，使用规则降级
        """
        # 首次写入
        if existing is None:
            return {"action": "WRITE", "reason": "首次写入"}
        
        # 规则降级（LLM不可用时）
        if self.llm is None:
            return self._rule_based_decide(existing, new_data)
        
        # 构建prompt
        prompt = self._build_decision_prompt(existing, new_data, context)
        
        try:
            response = self.llm.decide(prompt)
            return self._parse_llm_response(response)
        except Exception as e:
            logger.warning(f"LLM决策失败，使用规则降级: {e}")
            return self._rule_based_decide(existing, new_data)
    
    def _build_decision_prompt(self, existing: Any, new_data: Any, context: Dict) -> str:
        """构建决策prompt"""
        return f"""
你是一位知识管理专家，负责判断新发现是否值得记录到知识库中。

## 现有知识
{json.dumps(existing, ensure_ascii=False, indent=2)[:1000]}

## 新发现
{json.dumps(new_data, ensure_ascii=False, indent=2)[:1000]}

## 上下文
{json.dumps(context, ensure_ascii=False, indent=2)}

## 决策规则
1. 如果新发现与现有知识本质重复（如相同指标的相关性），返回 SKIP
2. 如果新发现可以合并到现有记录（如补充信息），返回 MERGE
3. 如果新发现是全新的且有价值，返回 WRITE
4. 如果是列表类型的新条目，返回 APPEND

## 输出格式
请返回JSON格式：
{{
    "action": "SKIP|WRITE|MERGE|APPEND",
    "reason": "简要说明原因",
    "merge_strategy": "如果需要合并，说明如何合并"  // 可选
}}
"""
    
    def _rule_based_decide(self, existing: Any, new_data: Any) -> Dict:
        """规则降级决策（LLM不可用时）"""
        # 简单去重：如果新数据与现有数据相同，跳过
        if existing == new_data:
            return {"action": "SKIP", "reason": "数据完全相同"}
        
        # 检查是否是列表追加
        if isinstance(existing, list) and isinstance(new_data, dict):
            # 检查是否已存在相同条目
            for item in existing:
                if self._is_same_item(item, new_data):
                    return {"action": "SKIP", "reason": "列表中已存在相同条目"}
            return {"action": "APPEND", "reason": "新条目，追加到列表"}
        
        return {"action": "WRITE", "reason": "规则降级：默认写入"}
    
    def _is_same_item(self, item1: Dict, item2: Dict) -> bool:
        """判断两个条目是否本质相同"""
        # 根据关键字段判断
        key_fields = ["metrics", "name", "id", "type"]
        for field in key_fields:
            if field in item1 and field in item2:
                if item1[field] == item2[field]:
                    return True
        return False
    
    def _decide_device_memory(self, existing: Any, new_data: Any, context: Dict) -> Dict:
        """
        设备记忆决策
        
        规则：
        - 有异常时必写
        - 评分变化时必写
        - 无变化时，根据时间判断是否需要刷新
        """
        # 首次写入
        if existing is None:
            return {"action": "WRITE", "reason": "首次记录设备"}
        
        # 有异常必写
        if new_data.get("anomaly") or new_data.get("level") in ["warning", "danger"]:
            return {"action": "WRITE", "reason": "设备异常，必须记录"}
        
        # 评分变化必写
        if existing.get("total_score") != new_data.get("total_score"):
            return {"action": "WRITE", "reason": f"评分变化 {existing.get('total_score')} → {new_data.get('total_score')}"}
        
        # 检查是否需要定时刷新（超过7天未更新）
        last_update = existing.get("timestamp", "")
        if last_update:
            try:
                last = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                days_since = (datetime.now() - last).days
                if days_since >= 7:
                    return {"action": "WRITE", "reason": f"超过{days_since}天未更新，定时刷新"}
            except:
                pass
        
        # 无变化，跳过
        return {"action": "SKIP", "reason": "无变化，无需更新"}
    
    def _do_write(self, file_path: str, data: Any):
        """执行写入"""
        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _merge_data(self, existing: Any, new_data: Any, strategy: str) -> Any:
        """合并数据"""
        if strategy == "update_fields":
            # 更新字段
            result = existing.copy() if isinstance(existing, dict) else {}
            if isinstance(new_data, dict):
                result.update(new_data)
            return result
        
        elif strategy == "deep_merge":
            # 深度合并
            return self._deep_merge(existing, new_data)
        
        # 默认：新数据覆盖
        return new_data
    
    def _deep_merge(self, existing: Dict, new_data: Dict) -> Dict:
        """深度合并两个字典"""
        result = existing.copy()
        for key, value in new_data.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def _append_data(self, existing: Any, new_data: Any) -> list:
        """追加数据到列表"""
        if existing is None:
            return [new_data]
        
        if isinstance(existing, list):
            return existing + [new_data]
        
        # 如果不是列表，转为列表
        return [existing, new_data]
    
    def _log_decision(self, file_path: str, action: str, reason: str):
        """记录决策日志"""
        self.decision_log.append({
            "timestamp": datetime.now().isoformat(),
            "file": file_path,
            "action": action,
            "reason": reason
        })
    
    def get_decision_stats(self) -> Dict:
        """获取决策统计"""
        stats = {"WRITE": 0, "SKIP": 0, "MERGE": 0, "APPEND": 0}
        for log in self.decision_log:
            stats[log["action"]] = stats.get(log["action"], 0) + 1
        return stats


class MemoryGuard:
    """
    简化版强制调用检查
    确保"写入前必须被读取过"
    """
    
    def __init__(self):
        self.last_write = {}  # 文件 -> 上次写入时间
        self.last_read = {}   # 文件 -> {模块: 读取时间}
    
    def before_write(self, file_path: str, writer: str, required_readers: list = None):
        """
        写入前检查
        
        首次写入直接允许
        后续写入检查是否被必需模块读取过
        """
        # 首次写入
        if file_path not in self.last_write:
            return True
        
        # 没有强制读取要求
        if not required_readers:
            return True
        
        # 检查每个必需读取者
        last_write_time = self.last_write[file_path]
        for reader in required_readers:
            read_time = self.last_read.get(file_path, {}).get(reader)
            if not read_time or read_time < last_write_time:
                raise MemoryError(
                    f"[MemoryGuard] {file_path} 上次写入后未被 {reader} 读取，"
                    f"禁止被 {writer} 再次写入！"
                )
        
        return True
    
    def after_write(self, file_path: str, writer: str):
        """记录写入"""
        self.last_write[file_path] = datetime.now()
    
    def after_read(self, file_path: str, reader: str):
        """记录读取"""
        if file_path not in self.last_read:
            self.last_read[file_path] = {}
        self.last_read[file_path][reader] = datetime.now()
