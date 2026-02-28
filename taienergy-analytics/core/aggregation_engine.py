"""
V5.1 自动聚合引擎
生成设备画像和场站排名
"""
import json
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

from core.unified_history import UnifiedHistoryStore, get_history_store
from core.discovery_rules import DiscoveryRuleEngine


class AggregationEngine:
    """自动聚合引擎"""
    
    def __init__(self, history_store: UnifiedHistoryStore = None):
        self.history = history_store or get_history_store()
        self.rule_engine = DiscoveryRuleEngine()
    
    def generate_device_profile(self, sn: str, days: int = 30) -> Dict:
        """生成设备画像"""
        daily_series = self.history.read_device_daily_series(sn, days)
        
        if len(daily_series) < 3:
            return {"sn": sn, "status": "insufficient_data"}
        
        health_scores = [d['metrics'].get('health_score') for d in daily_series 
                        if d['metrics'].get('health_score') is not None]
        
        profile = {
            "sn": sn,
            "generated_at": datetime.now().isoformat(),
            "lookback_days": days,
            "data_points": len(daily_series),
            "health": {
                "current": health_scores[-1] if health_scores else None,
                "avg": round(np.mean(health_scores), 2) if health_scores else None,
                "trend": "stable"
            }
        }
        
        self.history.update_device_profile(sn, profile)
        return profile
    
    def generate_station_ranking(self, date_str: str, device_profiles: Dict[str, Dict]) -> Dict:
        """生成场站排名"""
        health_ranking = sorted(
            [(sn, p['health']['current']) for sn, p in device_profiles.items() 
             if p.get('health', {}).get('current') is not None],
            key=lambda x: x[1], reverse=True
        )
        
        return {
            "date": date_str,
            "rankings": {"by_health": [sn for sn, _ in health_ranking]},
            "top_performers": {"by_health": [sn for sn, _ in health_ranking[:3]]},
            "bottom_performers": {"by_health": [sn for sn, _ in health_ranking[-3:]]}
        }