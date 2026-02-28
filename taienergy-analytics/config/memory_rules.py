"""
记忆文件规则配置
定义各记忆文件的写入规则和强制读取关系
"""

from typing import Dict, List

# ========== 记忆文件注册表 ==========
MEMORY_REGISTRY = {
    
    # ========== Layer 3: 认知层 ==========
    "cognitive/relationship_graph.json": {
        "description": "指标关系图谱",
        "layer": "cognitive",
        "required_readers": ["daily_discovery", "feature_engineering"],
        "decision_mode": "llm",  # 大模型决策
        "max_size_mb": 10,
    },
    
    "cognitive/pattern_library.json": {
        "description": "可复用模式库",
        "layer": "cognitive", 
        "required_readers": ["risk_identification", "anomaly_detection"],
        "decision_mode": "llm",
        "max_size_mb": 50,
    },
    
    "cognitive/insights.json": {
        "description": "认知洞察记录",
        "layer": "cognitive",
        "required_readers": ["daily_report"],
        "decision_mode": "llm",
        "max_size_mb": 5,
    },
    
    # ========== Layer 2: 设备层 ==========
    "devices/{sn}/memory.json": {
        "description": "设备长期记忆",
        "layer": "device",
        "required_readers": ["health_scoring", "risk_identification"],
        "decision_mode": "hybrid",  # 混合决策（规则+LLM）
        "max_entries": 100,  # 最多保留100条记录
    },
    
    "devices/{sn}/predictions.json": {
        "description": "预测记录（用于偏差计算）",
        "layer": "device",
        "required_readers": ["health_scoring"],
        "decision_mode": "auto",  # 自动写入
        "max_entries": 30,  # 保留最近30天预测
    },
    
    "devices/{sn}/anomaly_history.json": {
        "description": "异常历史记录",
        "layer": "device",
        "required_readers": ["risk_identification"],
        "decision_mode": "auto",
        "max_entries": 50,
    },
    
    # ========== Layer 1: 日报层 ==========
    "daily/{date}_report.json": {
        "description": "每日资产管理日报",
        "layer": "daily",
        "required_readers": ["monthly_review", "portfolio_analytics"],
        "decision_mode": "auto",  # 每日必须生成
        "max_size_kb": 50,  # 精简版，不超过50KB
        "retention_days": 365,  # 保留1年
    },
    
    # ========== 指标层 ==========
    "indicators/registry.json": {
        "description": "指标注册表（核心资产）",
        "layer": "indicator",
        "required_readers": ["daily_discovery", "evaluation_engine", "monthly_review"],
        "decision_mode": "auto",
        "backup_count": 10,  # 保留10个备份
    },
    
    "indicators/candidate/{name}_{date}.md": {
        "description": "候选指标规格文档",
        "layer": "indicator",
        "required_readers": ["evaluation_engine"],
        "decision_mode": "auto",
        "retention_days": 90,  # 候选保留90天
    },
}


# ========== 决策Prompt模板 ==========
DECISION_PROMPTS = {
    
    "cognitive_relationship": """
你是一位知识管理专家，负责判断新的指标相关性是否值得记录。

## 现有关系图谱
{existing}

## 新发现
{new_data}

## 判断标准
1. 是否是全新的指标组合？（未在现有关系中）
2. 相关系数是否足够高？（>0.85）
3. 是否可以用于生成交叉特征？
4. 是否与现有关系本质重复？

## 输出
返回JSON：
{{
    "action": "SKIP|WRITE|MERGE",
    "reason": "简要说明",
    "confidence": 0.95
}}
""",

    "cognitive_pattern": """
你是一位模式识别专家，判断新发现的模式是否值得入库。

## 现有模式库
{existing}

## 新模式
{new_data}

## 判断标准
1. 模式是否可复用？（跨设备/跨时间）
2. 置信度是否足够高？
3. 是否与现有模式重复？
4. 是否有预测价值？

## 输出
返回JSON：
{{
    "action": "SKIP|WRITE|MERGE",
    "reason": "简要说明",
    "confidence": 0.85
}}
""",

    "device_memory": """
你是一位设备管理专家，判断是否需要更新设备记忆。

## 设备历史
{existing}

## 今日数据
{new_data}

## 判断标准
1. 评分是否有显著变化？（>5分）
2. 是否有新的异常？
3. 是否超过7天未更新？
4. 趋势是否有变化？（改善/恶化）

## 输出
返回JSON：
{{
    "action": "SKIP|WRITE",
    "reason": "简要说明"
}}
""",
}


# ========== 清理规则 ==========
CLEANUP_RULES = {
    "daily_reports": {
        "retention_days": 365,
        "action": "archive",  # 归档而非删除
    },
    "candidate_indicators": {
        "retention_days": 90,
        "action": "delete",  # 超过90天未晋升则删除
    },
    "validation_logs": {
        "retention_days": 7,
        "action": "delete",  # 验证日志只保留7天
    },
    "temp_files": {
        "retention_days": 1,
        "action": "delete",
    },
}
