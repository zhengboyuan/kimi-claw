"""
V5.1 三层指标发现 Pipeline（最小可用版）
"""
from __future__ import annotations

from typing import Dict, Optional, Any
from pathlib import Path
import json
import pandas as pd

from core.daily_discovery import IndicatorDiscovery
from core.composite_engine_v43 import discover_daily_candidates_v43
from core.indicator_taxonomist import review_candidates_with_llm


def _build_df_from_raw_data(raw_data: Dict[str, Any]) -> pd.DataFrame:
    """
    raw_data: {code: DataFrame(value column)}
    返回: 按列拼接的 DataFrame（以最短长度对齐）
    """
    # 优先按时间戳对齐（如果存在 ts/time 列）
    time_dfs = []
    series_list = []
    for code, df in raw_data.items():
        if hasattr(df, "columns") and "value" in df.columns:
            local = df.copy()
            ts_col = None
            for candidate in ("ts", "time", "timestamp"):
                if candidate in local.columns:
                    ts_col = candidate
                    break
            if ts_col:
                local = local[[ts_col, "value"]].dropna()
                local = local.set_index(ts_col)
                local = local.rename(columns={"value": code})
                time_dfs.append(local)
            else:
                values = local["value"].dropna().values.tolist()
                if values:
                    series_list.append(pd.Series(values, name=code))

    if not time_dfs and not series_list:
        return pd.DataFrame()

    # 如果包含带时间索引的数据，按索引对齐
    if time_dfs:
        aligned = pd.concat(time_dfs, axis=1, join="inner")
        if aligned.empty:
            return aligned
        if series_list:
            min_len = min(len(aligned), min(len(s) for s in series_list))
            aligned = aligned.iloc[:min_len].copy()
            for s in series_list:
                aligned[s.name] = s.iloc[:min_len].values
        return aligned

    # 否则退回到最短长度拼接
    min_len = min(len(s) for s in series_list)
    trimmed = {s.name: s.iloc[:min_len].values for s in series_list}
    return pd.DataFrame(trimmed)


def run_discovery_pipeline(
    date_str: str,
    device_data: Dict,
    llm_client=None,
    write_pending: bool = False,
) -> Dict[str, Any]:
    """
    返回结构：
    {
      "layer1": [CandidateIndicator...],
      "layer2": {device_sn: {candidates: {...}}},
      "layer3": {"approved_indicators": [...], "rejected_candidates": [...]}
    }
    """
    # Layer 1
    discovery = IndicatorDiscovery(llm_client=llm_client)
    layer1_candidates = discovery.scan_daily(date_str, device_data)

    # Layer 2（按设备）
    layer2_results = {}
    for device_sn, data in device_data.items():
        if not data or "raw_data" not in data:
            continue
        df_day = _build_df_from_raw_data(data["raw_data"])
        if df_day.empty:
            continue
        layer2_results[device_sn] = discover_daily_candidates_v43(df_day, day_index=0)

    # Layer 3（语义评审/命名）
    # 汇总 Layer2 candidates
    l2_candidates_flat = {}
    for sn, r in layer2_results.items():
        for name, info in r.get("candidates", {}).items():
            l2_candidates_flat[f"{sn}:{name}"] = info

    layer3_result = {"approved_indicators": [], "rejected_candidates": []}
    if l2_candidates_flat:
        context = {"date": date_str, "existing_count": 0}
        layer3_result = review_candidates_with_llm(l2_candidates_flat, context, llm_client=llm_client)

        if write_pending and layer3_result.get("approved_indicators"):
            _write_llm_pending(date_str, layer3_result["approved_indicators"])

    return {
        "layer1": layer1_candidates,
        "layer2": layer2_results,
        "layer3": layer3_result,
    }


def _write_llm_pending(date_str: str, indicators: list) -> None:
    base = Path("config/indicators/llm_generated/pending")
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{date_str}_pending.json"
    payload = {"date": date_str, "indicators": indicators}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
