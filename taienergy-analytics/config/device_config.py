"""
设备配置文件
支持动态指标发现和进化
"""

# 泰能平台配置
TAIENERGY_CONFIG = {
    "base_url": "https://taienergy.console.chintiot.net",
    "appid": "076c1320524e4cdaaf6904da90fcf910",
    "appkey": "c7b655dd-33a1-49fa-b7d8-49023afb9827",
}

# 核心基准指标配置
CORE_BENCHMARKS = {
    "daytime": "ai56",    # 日间基准：有功功率
    "nighttime": "ai61",  # 夜间基准：内部温度
}

# 哨兵指标（禁止自动淘汰）
SENTINEL_INDICATORS = [
    "ai63",   # 设备状态
    "ai64",   # 故障码
    "di39", "di40", "di41", "di42", "di43", "di44",  # 告警类
]

# 测试设备配置
# 注意：实际指标列表将从 GetDevicePropertyList 接口或自动探测获取
DEVICES = {
    "XHDL_1NBQ": {
        "name": "测试逆变器",
        "type": "inverter",
        "capacity_kw": 50,
        "location": "测试站点",
        # 探测配置
        "discovery": {
            "probe_range": 200,        # 探测范围 ai1-200, di1-200
            "full_scan_interval": 7,   # 全量扫描间隔（天）
            "daily_probe_sample": 10,  # 日常抽样探测数量
        },
        # 进化阈值
        "evolution": {
            "upgrade_threshold": 0.6,   # 升级阈值
            "upgrade_days": 3,          # 升级所需连续天数
            "downgrade_days": 7,        # 降级所需连续天数
            "silent_days": 7,           # 进入静默池天数
            "remove_days": 30,          # 彻底移除天数
        }
    }
}

# 分析配置
ANALYSIS_CONFIG = {
    "start_date": "2025-02-22",
    "min_data_points": 5,
    "change_point_threshold": 2.0,
    "outlier_std_threshold": 2.5,
    "seasonality_periods": [24, 12],
    "batch_size": 20,  # 每批查询的指标数量（避免 API 超时）
}
