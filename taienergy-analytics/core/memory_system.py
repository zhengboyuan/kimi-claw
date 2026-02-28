"""
V4.5 智能记忆系统 - 整合版

核心设计：
1. 三层架构：日报层 → 设备层 → 认知层
2. 数据流动：每层必须被上层读取，否则不写入
3. 智能决策：大模型判断新发现是否值得记录
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class MemorySystem:
    """智能记忆系统主类"""
    
    def __init__(self, llm_client=None):
        self.llm = llm_client
        self.base_path = "memory"
        self._ensure_dirs()
    
    def _ensure_dirs(self):
        """确保目录结构存在"""
        dirs = [
            "memory/daily",
            "memory/devices", 
            "memory/cognitive",
            "memory/indicators/candidate"
        ]
        for d in dirs:
            os.makedirs(d, exist_ok=True)
    
    # ========== Layer 1: 日报层 ==========
    
    def write_daily_report(self, date: str, data: Dict) -> str:
        """
        写入日报（精简版，<50KB）
        
        V5.1 更新：同时输出到旧路径和新规范路径，保持向后兼容
        新规范路径：memory/reports/daily/station/{date}.json
        """
        # 精简数据：只保留关键字段
        compact = {
            "date": date,
            "generated_at": datetime.now().isoformat(),
            "version": "v5.1",
            "portfolio": {
                "total_devices": data.get("total_devices", 16),
                "online": data.get("online", 0),
                "avg_health_score": round(data.get("avg_health_score", 0), 1),
                "risk_distribution": data.get("risk_distribution", {}),
                "trend_alerts": data.get("trend_alerts", 0),
                "maintenance_priority": data.get("maintenance_priority", [])[:5]
            },
            "devices": {
                sn: {
                    "health_score": d.get("health_score"),
                    "level": d.get("level"),
                    "trend_score": d.get("trend_score", 0)
                }
                for sn, d in data.get("devices", {}).items()
            },
            "discovery": {
                "candidates_found": data.get("candidates_found", 0),
                "top_candidate": data.get("top_candidate")
            }
        }
        
        # V5.1 新规范路径
        new_path = f"{self.base_path}/reports/daily/station/{date}.json"
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        self._write_json(new_path, compact)
        
        # 旧路径（向后兼容）
        old_path = f"{self.base_path}/daily/{date}_report.json"
        self._write_json(old_path, compact)
        
        # 检查大小
        size_kb = os.path.getsize(new_path) / 1024
        logger.info(f"[Daily] {date}: {size_kb:.1f} KB (v5.1 path: {new_path})")
        
        return new_path
    
    def read_daily_report(self, date: str) -> Optional[Dict]:
        """读取日报"""
        path = f"{self.base_path}/daily/{date}_report.json"
        return self._read_json(path)
    
    # ========== Layer 2: 设备层 ==========
    
    def write_device_memory(self, sn: str, data: Dict) -> bool:
        """
        智能写入设备记忆
        
        规则：
        - 首次写入：允许
        - 有异常：允许
        - 评分变化：允许
        - 无变化且<7天：跳过
        """
        path = f"{self.base_path}/devices/{sn}/memory.json"
        existing = self._read_json(path)
        
        # 决策
        if existing is None:
            reason = "首次写入"
        elif data.get("level") in ["warning", "danger"]:
            reason = "设备异常"
        elif existing.get("health_score") != data.get("health_score"):
            reason = f"评分变化 {existing.get('health_score')}→{data.get('health_score')}"
        else:
            # 检查是否超过7天
            last = existing.get("timestamp", "")
            if last:
                try:
                    last_dt = datetime.fromisoformat(last.replace('Z', '+00:00'))
                    days = (datetime.now() - last_dt).days
                    if days >= 7:
                        reason = f"超过{days}天未更新"
                    else:
                        logger.info(f"[Device/{sn}] SKIP: 无变化，{days}天前更新")
                        return False
                except:
                    reason = "时间解析失败，强制更新"
            else:
                reason = "无时间戳，强制更新"
        
        # 写入
        record = {
            "sn": sn,
            "date": data.get("date"),
            "health_score": data.get("health_score"),
            "level": data.get("level"),
            "dimensions": data.get("dimensions"),
            "timestamp": datetime.now().isoformat()
        }
        
        self._write_json(path, record)
        logger.info(f"[Device/{sn}] WRITE: {reason}")
        return True
    
    def read_device_memory(self, sn: str) -> Optional[Dict]:
        """读取设备记忆"""
        path = f"{self.base_path}/devices/{sn}/memory.json"
        return self._read_json(path)
    
    # ========== Layer 3: 认知层 ==========
    
    def write_relationship(self, new_relation: Dict) -> bool:
        """
        智能写入关系图谱
        
        大模型决策：是否与现有关系重复？
        """
        path = f"{self.base_path}/cognitive/relationship_graph.json"
        existing = self._read_json(path) or {"relationships": []}
        
        # 检查是否已存在相同关系
        for rel in existing["relationships"]:
            if (set(rel.get("metrics", [])) == set(new_relation.get("metrics", []))):
                logger.info(f"[Cognitive] SKIP: 关系已存在 {new_relation.get('metrics')}")
                return False
        
        # 添加新关系
        existing["relationships"].append({
            **new_relation,
            "discovered_at": datetime.now().isoformat(),
            "verified_count": 1
        })
        
        self._write_json(path, existing)
        logger.info(f"[Cognitive] WRITE: 新关系 {new_relation.get('metrics')}")
        return True
    
    def read_relationships(self) -> List[Dict]:
        """读取所有关系"""
        path = f"{self.base_path}/cognitive/relationship_graph.json"
        data = self._read_json(path)
        return data.get("relationships", []) if data else []
    
    def write_pattern(self, pattern: Dict) -> bool:
        """写入模式库"""
        path = f"{self.base_path}/cognitive/pattern_library.json"
        existing = self._read_json(path) or {"patterns": []}
        
        # 去重检查
        for p in existing["patterns"]:
            if p.get("signature") == pattern.get("signature"):
                logger.info(f"[Cognitive] SKIP: 模式已存在")
                return False
        
        existing["patterns"].append({
            **pattern,
            "created_at": datetime.now().isoformat()
        })
        
        self._write_json(path, existing)
        logger.info(f"[Cognitive] WRITE: 新模式 {pattern.get('name')}")
        return True
    
    # ========== 指标层 ==========
    
    def write_candidate(self, name: str, date: str, spec: Dict) -> bool:
        """写入候选指标"""
        path = f"{self.base_path}/indicators/candidate/{name}_{date}.md"
        
        if os.path.exists(path):
            logger.info(f"[Candidate] SKIP: {name} 已存在")
            return False
        
        content = self._format_candidate_md(spec)
        with open(path, 'w') as f:
            f.write(content)
        
        logger.info(f"[Candidate] WRITE: {name}")
        return True
    
    def read_candidates(self) -> List[Dict]:
        """读取所有候选指标"""
        candidates = []
        path = f"{self.base_path}/indicators/candidate"
        if not os.path.exists(path):
            return candidates
        
        for f in os.listdir(path):
            if f.endswith('.md'):
                # 解析文件名提取信息
                parts = f.replace('.md', '').split('_')
                if len(parts) >= 2:
                    candidates.append({
                        'file': f,
                        'name': '_'.join(parts[:-1]),
                        'date': parts[-1]
                    })
        return candidates
    
    def update_registry(self, indicator: Dict) -> bool:
        """更新指标注册表"""
        path = f"{self.base_path}/indicators/registry.json"
        registry = self._read_json(path) or {"version": 1, "indicators": {}}
        
        name = indicator.get("name")
        if name in registry["indicators"]:
            # 更新
            registry["indicators"][name].update(indicator)
            registry["indicators"][name]["updated_at"] = datetime.now().isoformat()
        else:
            # 新增
            registry["indicators"][name] = {
                **indicator,
                "created_at": datetime.now().isoformat()
            }
        
        registry["version"] += 1
        self._write_json(path, registry)
        logger.info(f"[Registry] UPDATE: {name}")
        return True
    
    def read_registry(self) -> Dict:
        """读取注册表"""
        path = f"{self.base_path}/indicators/registry.json"
        return self._read_json(path) or {"version": 0, "indicators": {}}
    
    def read_recent_reports(self, days: int = 30) -> List[Dict]:
        """读取最近N天的日报"""
        reports = []
        daily_path = f"{self.base_path}/daily"
        if not os.path.exists(daily_path):
            return reports
        
        # 获取最近N天的日期
        dates = []
        for i in range(days):
            d = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            dates.append(d)
        
        for date_str in dates:
            report = self.read_daily_report(date_str)
            if report:
                reports.append(report)
        
        return reports
    
    def write_comparison_insight(self, insight: Dict) -> bool:
        """
        写入对比洞察（大模型发现的新对比维度）
        """
        path = f"{self.base_path}/cognitive/pattern_library.json"
        existing = self._read_json(path) or {"patterns": []}
        
        # 检查是否已存在（按signature去重）
        for p in existing["patterns"]:
            if p.get("signature") == insight.get("signature"):
                # 更新验证次数
                p["verified_count"] = p.get("verified_count", 0) + 1
                p["last_verified"] = datetime.now().isoformat()
                self._write_json(path, existing)
                logger.info(f"[Cognitive] UPDATE: 对比洞察验证次数+1 {insight.get('signature')}")
                return False
        
        # 新洞察
        new_pattern = {
            **insight,
            "type": "comparison_pattern",
            "created_at": datetime.now().isoformat(),
            "verified_count": 1,
            "status": "candidate"  # candidate | verified | codified
        }
        
        existing["patterns"].append(new_pattern)
        self._write_json(path, existing)
        logger.info(f"[Cognitive] WRITE: 新对比洞察 {insight.get('name')}")
        return True
    
    def get_comparison_patterns(self, status: str = None) -> List[Dict]:
        """获取对比模式（可选按状态过滤）"""
        path = f"{self.base_path}/cognitive/pattern_library.json"
        data = self._read_json(path)
        if not data:
            return []
        
        patterns = data.get("patterns", [])
        comparison_patterns = [p for p in patterns if p.get("type") == "comparison_pattern"]
        
        if status:
            comparison_patterns = [p for p in comparison_patterns if p.get("status") == status]
        
        return comparison_patterns
    
    # ========== 工具方法 ==========
    
    def _read_json(self, path: str) -> Optional[Dict]:
        """读取JSON文件"""
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    
    def _write_json(self, path: str, data: Dict):
        """写入JSON文件"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _format_candidate_md(self, spec: Dict) -> str:
        """格式化候选指标Markdown"""
        return f"""# 候选指标: {spec.get('name', 'unknown')}

## 发现信息
- **发现日期**: {spec.get('discovered_at', 'unknown')}
- **发现方式**: {spec.get('source', 'unknown')}
- **信息增益**: {spec.get('info_gain', 0):.4f}

## 定义
```python
{spec.get('formula', '# TODO')}
```

## 依赖指标
{spec.get('dependencies', [])}

## 状态
- [ ] 业务规则验证
- [ ] 四维评分
- [ ] Ralph实现
- [ ] 生产验证

---
*自动生成于 {datetime.now().isoformat()}*
"""
    
    def get_stats(self) -> Dict:
        """获取记忆系统统计"""
        stats = {
            "daily_reports": 0,
            "device_memories": 0,
            "relationships": 0,
            "patterns": 0,
            "candidates": 0,
            "registry_indicators": 0
        }
        
        # 统计日报
        daily_path = f"{self.base_path}/daily"
        if os.path.exists(daily_path):
            stats["daily_reports"] = len([f for f in os.listdir(daily_path) if f.endswith('.json')])
        
        # 统计设备记忆
        devices_path = f"{self.base_path}/devices"
        if os.path.exists(devices_path):
            stats["device_memories"] = len([d for d in os.listdir(devices_path) 
                                          if os.path.isdir(f"{devices_path}/{d}")])
        
        # 统计关系
        rel_path = f"{self.base_path}/cognitive/relationship_graph.json"
        rel_data = self._read_json(rel_path)
        if rel_data:
            stats["relationships"] = len(rel_data.get("relationships", []))
        
        # 统计模式
        pat_path = f"{self.base_path}/cognitive/pattern_library.json"
        pat_data = self._read_json(pat_path)
        if pat_data:
            stats["patterns"] = len(pat_data.get("patterns", []))
        
        # 统计候选
        stats["candidates"] = len(self.read_candidates())
        
        # 统计注册表
        reg = self.read_registry()
        stats["registry_indicators"] = len(reg.get("indicators", {}))
        stats["candidates"] = len(reg.get("candidates", {}))
        
        return stats
    
    # ========== 指标进化层 (V5.1新增) ==========
    
    def write_evolution_report(self, report: Dict) -> str:
        """写入指标进化报告"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        path = f"{self.base_path}/reports/evolution/{date_str}.json"
        
        record = {
            "date": date_str,
            "generated_at": datetime.now().isoformat(),
            **report
        }
        
        self._write_json(path, record)
        logger.info(f"[Evolution] Report saved: {path}")
        return path
    
    def read_evolution_history(self, days: int = 30) -> List[Dict]:
        """读取最近进化历史"""
        reports = []
        path = f"{self.base_path}/reports/evolution"
        if not os.path.exists(path):
            return reports
        
        for i in range(days):
            d = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            report = self._read_json(f"{path}/{d}.json")
            if report:
                reports.append(report)
        
        return reports
    
    def get_evolution_stats(self) -> Dict:
        """获取指标进化统计"""
        from core.indicator_evolution import IndicatorRegistry
        reg = IndicatorRegistry()
        
        return {
            "total_approved": len(reg.get_indicators("approved")),
            "pending_candidates": len(reg.get_candidates()),
            "evolution_rounds": len(reg.data.get("evolution_history", [])),
            "by_round": reg.data.get("evolution_history", [])
        }
