"""
系统配置 - 集中管理所有硬编码常量
"""

# 设备配置
DEVICE_CONFIG = {
    "count": 16,  # 逆变器数量
    "name_prefix": "XHDL",
    "name_suffix": "NBQ",
}

# 场站配置
STATION_CONFIG = {
    "installed_capacity_kw": 16000,  # 装机容量 16MW
    "rated_power_per_device_kw": 1000,  # 单台额定功率
}

# 历史数据配置
HISTORY_CONFIG = {
    "default_days": 30,  # 默认历史天数
    "health_history_limit": 30,  # 健康记录最大保留天数
    "observation_days": 30,  # 指标观察期
    "knowledge_distill_interval": 30,  # 知识蒸馏间隔
}

# 阈值配置
THRESHOLD_CONFIG = {
    "default_percentage": 30,  # 默认百分比阈值
    "health_score_warning": 70,  # 健康分警告线
    "health_score_danger": 50,  # 健康分危险线
}

# 时间配置
TIME_CONFIG = {
    "hours_per_day": 24,
    "autocorr_max_lags": 24,  # 自相关最大滞后
}

# 指标发现配置
DISCOVERY_CONFIG = {
    "layer1_rule_missing_rate": 0.95,
    "layer1_rule_cv_threshold": 5.0,
    "layer1_rule_cv_threshold_trend": 10.0,
    "layer1_rule_info_gain_min": 0.0,
}

# 数据质量配置
DATA_QUALITY_CONFIG = {
    "min_data_points_per_day": 24,  # 每天最少数据点
    "completeness_threshold": 0.95,  # 完整性阈值
}

# 测点编码配置
POINT_CONFIG = {
    "power_point_code": "ai56",  # 有功功率点位
    "generation_point_code": "ai68",  # 当日发电量点位
}
