"""
记忆管理器
负责读写临时记忆文件和结构化指标档案
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path


class MemoryManager:
    """
    记忆管理器
    
    存储结构：
    - L1 静态档案：设备基础信息
    - L2 动态履历：每日原始记录
    - L3 认知知识：深度分析发现的规律
    - L4 指标档案：结构化指标目录 (indicator_catalog.json)
    """
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.memory_dir = Path(memory_dir)
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        
        self.daily_logs_dir = self.memory_dir / "daily_logs"
        self.daily_logs_dir.mkdir(exist_ok=True)
        
        self.analysis_file = self.memory_dir / "temp_analysis.md"
        
        # 结构化指标档案
        self.catalog_file = self.memory_dir / "indicator_catalog.json"
    
    def save_daily_log(self, date_str: str, data: Dict[str, Any]):
        """保存每日原始记录（L2 动态履历）"""
        log_file = self.daily_logs_dir / f"{date_str}.json"
        
        log_entry = {
            "date": date_str,
            "device_sn": self.device_sn,
            "timestamp": datetime.now().isoformat(),
            "data": data
        }
        
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, ensure_ascii=False, indent=2)
    
    def load_daily_log(self, date_str: str) -> Optional[Dict]:
        """加载每日原始记录"""
        log_file = self.daily_logs_dir / f"{date_str}.json"
        
        if not log_file.exists():
            return None
        
        with open(log_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_all_dates(self) -> List[str]:
        """获取所有有记录的日期"""
        dates = []
        for log_file in sorted(self.daily_logs_dir.glob("*.json")):
            dates.append(log_file.stem)
        return dates
    
    def update_analysis_memory(self, section: str, content: str):
        """
        更新分析记忆（追加到 temp_analysis.md）
        
        Args:
            section: 章节名称，如 "ai20_daily_summary"
            content: Markdown 格式的内容
        """
        # 读取现有内容
        existing_content = ""
        if self.analysis_file.exists():
            with open(self.analysis_file, 'r', encoding='utf-8') as f:
                existing_content = f.read()
        
        # 构建新章节
        new_section = f"\n## {section}\n\n{content}\n"
        
        # 检查是否已存在该章节
        section_marker = f"## {section}"
        if section_marker in existing_content:
            # 替换现有章节
            parts = existing_content.split(section_marker)
            if len(parts) >= 2:
                # 找到下一个 ## 或文件结尾
                next_section_idx = parts[1].find("\n## ")
                if next_section_idx == -1:
                    # 这是最后一个章节
                    existing_content = parts[0] + new_section
                else:
                    # 替换中间章节
                    existing_content = parts[0] + new_section + parts[1][next_section_idx:]
        else:
            # 追加新章节
            existing_content += new_section
        
        # 写入文件
        with open(self.analysis_file, 'w', encoding='utf-8') as f:
            f.write(existing_content)
    
    def get_indicator_memory(self, indicator_code: str) -> str:
        """获取指定指标的记忆内容"""
        if not self.analysis_file.exists():
            return ""
        
        with open(self.analysis_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 查找指标相关章节
        section_marker = f"## {indicator_code}_"
        sections = []
        
        lines = content.split('\n')
        current_section = []
        in_target_section = False
        
        for line in lines:
            if line.startswith('## '):
                if in_target_section:
                    sections.append('\n'.join(current_section))
                    current_section = []
                in_target_section = section_marker in line or f"## 指标 {indicator_code}" in line
            
            if in_target_section:
                current_section.append(line)
        
        if current_section:
            sections.append('\n'.join(current_section))
        
        return '\n\n'.join(sections) if sections else ""
    
    def get_full_memory(self) -> str:
        """获取完整记忆内容"""
        if not self.analysis_file.exists():
            return self._init_memory_template()
        
        with open(self.analysis_file, 'r', encoding='utf-8') as f:
            return f.read()
    
    def _init_memory_template(self) -> str:
        """初始化记忆模板"""
        template = f"""# 光伏设备深度分析记忆（临时）

## 设备信息

- **设备 SN**: {self.device_sn}
- **创建时间**: {datetime.now().isoformat()}
- **状态**: 滚动迭代分析中

## 分析概览

| 日期 | 分析指标数 | 新发现 | 异常标记 |
|------|-----------|--------|----------|

## 指标认知库

### ai20 (PV8输入电流)

**基线模型**: 待建立（需要3天+数据）

**行为模式**: 
- 待发现

**异常历史**:
- 无

**深度洞察**:
- 待积累

---

*此文件为临时测试记忆，真实设备部署后将固化到正式知识库*
"""
        
        # 写入初始模板
        with open(self.analysis_file, 'w', encoding='utf-8') as f:
            f.write(template)
        
        return template
    
    def save_insight(self, indicator_code: str, insight_type: str, content: str):
        """
        保存深度洞察
        
        Args:
            indicator_code: 指标代码
            insight_type: 洞察类型（pattern/anomaly/change_point）
            content: 洞察内容
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        
        section_content = f"""
**{insight_type.upper()}** - {timestamp}

{content}
"""
        
        section_name = f"{indicator_code}_insights"
        self.update_analysis_memory(section_name, section_content)
    
    def get_analysis_summary(self) -> Dict[str, Any]:
        """获取分析摘要统计"""
        dates = self.get_all_dates()
        
        return {
            "device_sn": self.device_sn,
            "total_days": len(dates),
            "date_range": f"{dates[0]} ~ {dates[-1]}" if dates else "无数据",
            "memory_file": str(self.analysis_file),
            "daily_logs_count": len(list(self.daily_logs_dir.glob("*.json")))
        }
    
    # ========== 结构化指标档案 (L4) ==========
    
    def load_indicator_catalog(self) -> Dict:
        """
        加载指标档案库
        
        Returns:
            {
                "device_sn": str,
                "created_at": str,
                "updated_at": str,
                "indicators": {code: indicator_info},
                "silent_pool": {code: indicator_info},
                "removed_pool": {code: indicator_info},
                "composite_suggestions": [...]
            }
        """
        if self.catalog_file.exists():
            with open(self.catalog_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # 初始化空档案库
        return {
            "device_sn": self.device_sn,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "indicators": {},
            "silent_pool": {},
            "removed_pool": {},
            "composite_suggestions": []
        }
    
    def save_indicator_catalog(self, catalog: Dict):
        """保存指标档案库"""
        catalog["updated_at"] = datetime.now().isoformat()
        with open(self.catalog_file, 'w', encoding='utf-8') as f:
            json.dump(catalog, f, ensure_ascii=False, indent=2)
    
    def update_indicator_catalog(self, indicator_code: str, metrics: Dict):
        """
        更新指标档案
        
        Args:
            indicator_code: 指标代码
            metrics: 指标元数据
                - name: 中文名称
                - level: L0/L1/L2/L3
                - score: 重要性评分
                - unit: 单位
                - dtype: 数据类型
                - status: 状态
                - is_sentinel: 是否为哨兵指标
        """
        catalog = self.load_indicator_catalog()
        
        if indicator_code not in catalog["indicators"]:
            catalog["indicators"][indicator_code] = {
                "code": indicator_code,
                "first_seen": datetime.now().isoformat(),
                "evaluation_history": []
            }
        
        # 更新指标信息
        catalog["indicators"][indicator_code].update(metrics)
        catalog["indicators"][indicator_code]["last_updated"] = datetime.now().isoformat()
        
        self.save_indicator_catalog(catalog)
    
    def get_indicator_info(self, indicator_code: str) -> Optional[Dict]:
        """获取指标档案信息"""
        catalog = self.load_indicator_catalog()
        return catalog["indicators"].get(indicator_code)
    
    def add_composite_suggestion(self, suggestion: str, related_indicators: List[str]):
        """
        添加复合指标建议（L3）
        
        Args:
            suggestion: LLM 的建议文本
            related_indicators: 相关指标代码列表
        """
        catalog = self.load_indicator_catalog()
        
        catalog["composite_suggestions"].append({
            "suggestion": suggestion,
            "related_indicators": related_indicators,
            "created_at": datetime.now().isoformat(),
            "status": "pending"  # pending/approved/rejected
        })
        
        self.save_indicator_catalog(catalog)
    
    def get_indicators_by_level(self, level: str) -> List[str]:
        """获取指定级别的所有指标代码"""
        catalog = self.load_indicator_catalog()
        return [
            code for code, info in catalog["indicators"].items()
            if info.get("level") == level
        ]
    
    def get_catalog_summary(self) -> Dict:
        """获取档案库摘要"""
        catalog = self.load_indicator_catalog()
        indicators = catalog["indicators"]
        
        return {
            "total": len(indicators),
            "L0_candidates": len([i for i in indicators.values() if i.get("level") == "L0"]),
            "L1_active": len([i for i in indicators.values() if i.get("level") == "L1"]),
            "L2_core": len([i for i in indicators.values() if i.get("level") == "L2"]),
            "L3_synthesized": len(catalog.get("composite_suggestions", [])),
            "silent": len(catalog.get("silent_pool", {})),
            "removed": len(catalog.get("removed_pool", {})),
            "sentinels": len([i for i in indicators.values() if i.get("is_sentinel")])
        }
