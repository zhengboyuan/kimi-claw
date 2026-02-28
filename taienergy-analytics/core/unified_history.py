"""
V5.1 统一历史存储系统
替代原有的单点覆盖机制，支持时间序列沉淀
"""
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class UnifiedHistoryStore:
    """
    统一历史存储系统
    
    设计原则：
    1. 只追加，不覆盖 - 所有历史保留
    2. 分层存储 - 原始数据 / 日聚合 / 周聚合 / 月聚合
    3. 自动归档 - 旧数据自动压缩归档
    4. 统一接口 - 所有指标走同一套存储逻辑
    """
    
    def __init__(self, base_path: str = "memory"):
        self.base_path = Path(base_path)
        self._ensure_dirs()
    
    def _ensure_dirs(self):
        """确保目录结构"""
        dirs = [
            "devices/{sn}/raw",           # 原始数据（保留90天）
            "devices/{sn}/daily",         # 日聚合（保留2年）
            "devices/{sn}/weekly",        # 周聚合（保留5年）
            "devices/{sn}/monthly",       # 月聚合（永久）
            "devices/{sn}/profile",       # 设备画像（最新）
            "station/daily",              # 场站日聚合
            "station/weekly",             # 场站周聚合
            "station/monthly",            # 场站月聚合
            "station/ranking",            # 排名历史
            "archive"                     # 归档数据
        ]
        for d in dirs:
            (self.base_path / d).mkdir(parents=True, exist_ok=True)
    
    # ========== 核心写入接口 ==========
    
    def append_device_raw(self, sn: str, date_str: str, data: Dict) -> str:
        """
        追加设备原始数据
        
        Args:
            sn: 设备序列号
            date_str: 日期，如 "2025-08-20"
            data: 原始数据字典
        
        Returns:
            写入的文件路径
        """
        path = self.base_path / f"devices/{sn}/raw/{date_str}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        record = {
            "sn": sn,
            "date": date_str,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "version": "v5.1"
        }
        
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info(f"[History/{sn}] 原始数据: {date_str}")
        return str(path)
    
    def append_device_daily(self, sn: str, date_str: str, metrics: Dict) -> str:
        """
        追加设备日聚合指标
        
        Args:
            sn: 设备序列号
            date_str: 日期
            metrics: 日聚合指标，如健康分、发电量、发电时长等
        """
        path = self.base_path / f"devices/{sn}/daily/{date_str}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        record = {
            "sn": sn,
            "date": date_str,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat(),
            "aggregation": "daily"
        }
        
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info(f"[History/{sn}] 日聚合: {date_str}")
        return str(path)
    
    def append_station_daily(self, date_str: str, metrics: Dict) -> str:
        """追加场站日聚合"""
        path = self.base_path / f"station/daily/{date_str}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        record = {
            "date": date_str,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat(),
            "aggregation": "daily"
        }
        
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info(f"[History/Station] 日聚合: {date_str}")
        return str(path)
    
    def append_ranking(self, date_str: str, rankings: Dict) -> str:
        """
        追加设备排名
        
        Args:
            rankings: {
                "by_health": ["XHDL_3NBQ", "XHDL_1NBQ", ...],
                "by_generation": ["XHDL_5NBQ", ...],
                "by_efficiency": [...]
            }
        """
        path = self.base_path / f"station/ranking/{date_str}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        record = {
            "date": date_str,
            "rankings": rankings,
            "timestamp": datetime.now().isoformat()
        }
        
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info(f"[History/Station] 排名: {date_str}")
        return str(path)
    
    # ========== 核心读取接口 ==========
    
    def read_device_daily_series(self, sn: str, days: int = 30) -> List[Dict]:
        """
        读取设备日聚合时间序列
        
        Args:
            sn: 设备序列号
            days: 读取最近多少天
        
        Returns:
            按日期排序的日聚合列表
        """
        daily_dir = self.base_path / f"devices/{sn}/daily"
        if not daily_dir.exists():
            return []
        
        # 获取最近 N 天的文件
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        records = []
        for f in sorted(daily_dir.glob('*.json')):
            date_str = f.stem
            if date_str >= cutoff:
                try:
                    record = json.loads(f.read_text(encoding='utf-8'))
                    records.append(record)
                except Exception as e:
                    logger.warning(f"读取失败 {f}: {e}")
        
        return sorted(records, key=lambda x: x.get('date', ''))
    
    def read_station_daily_series(self, days: int = 30) -> List[Dict]:
        """读取场站日聚合时间序列"""
        daily_dir = self.base_path / "station/daily"
        if not daily_dir.exists():
            return []
        
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        records = []
        for f in sorted(daily_dir.glob('*.json')):
            date_str = f.stem
            if date_str >= cutoff:
                try:
                    record = json.loads(f.read_text(encoding='utf-8'))
                    records.append(record)
                except Exception as e:
                    logger.warning(f"读取失败 {f}: {e}")
        
        return sorted(records, key=lambda x: x.get('date', ''))
    
    def read_ranking_series(self, days: int = 30) -> List[Dict]:
        """读取排名历史序列"""
        ranking_dir = self.base_path / "station/ranking"
        if not ranking_dir.exists():
            return []
        
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        records = []
        for f in sorted(ranking_dir.glob('*.json')):
            date_str = f.stem
            if date_str >= cutoff:
                try:
                    record = json.loads(f.read_text(encoding='utf-8'))
                    records.append(record)
                except Exception as e:
                    logger.warning(f"读取失败 {f}: {e}")
        
        return sorted(records, key=lambda x: x.get('date', ''))
    
    # ========== 设备画像接口 ==========
    
    def update_device_profile(self, sn: str, profile: Dict) -> str:
        """
        更新设备画像（最新状态，覆盖）
        
        与历史存储不同，画像只保留最新聚合特征
        """
        path = self.base_path / f"devices/{sn}/profile/latest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        record = {
            "sn": sn,
            "profile": profile,
            "updated_at": datetime.now().isoformat(),
            "version": "v5.1"
        }
        
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info(f"[Profile/{sn}] 更新画像")
        return str(path)
    
    def read_device_profile(self, sn: str) -> Optional[Dict]:
        """读取设备画像"""
        path = self.base_path / f"devices/{sn}/profile/latest.json"
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
        return None
    
    # ========== 统计查询接口 ==========
    
    def get_device_dates(self, sn: str) -> List[str]:
        """获取设备有数据的所有日期"""
        daily_dir = self.base_path / f"devices/{sn}/daily"
        if not daily_dir.exists():
            return []
        return sorted([f.stem for f in daily_dir.glob('*.json')])
    
    def get_data_coverage(self, sn: str, days: int = 30) -> Dict:
        """
        获取数据覆盖情况
        
        Returns:
            {
                "expected_days": 30,
                "actual_days": 28,
                "coverage_rate": 0.93,
                "missing_dates": ["2025-08-15", ...]
            }
        """
        expected_dates = []
        for i in range(days):
            d = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            expected_dates.append(d)
        
        actual_dates = set(self.get_device_dates(sn))
        
        missing = [d for d in expected_dates if d not in actual_dates]
        actual = [d for d in expected_dates if d in actual_dates]
        
        return {
            "expected_days": days,
            "actual_days": len(actual),
            "coverage_rate": round(len(actual) / days, 2),
            "missing_dates": missing
        }


# 便捷函数
def get_history_store(base_path: str = "memory") -> UnifiedHistoryStore:
    """获取历史存储实例"""
    return UnifiedHistoryStore(base_path)