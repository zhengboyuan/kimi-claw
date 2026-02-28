# 光伏设备滚动迭代深度分析系统

## 项目结构

```
taienergy-analytics/
├── README.md                   # 项目说明
├── requirements.txt            # Python依赖
├── main.py                     # 主入口脚本
│
├── config/
│   └── device_config.py        # 设备配置（SN、指标列表）
│
├── core/                       # 核心分析模块
│   ├── base_analyzer.py        # 分析器基类
│   └── time_series_analyzer.py # 时间序列深度分析
│       - 趋势分解（线性回归）
│       - 周期发现（自相关、FFT）
│       - 变点检测（CUSUM）
│       - 异常检测（Z-score）
│
├── skills/                     # Skill实现
│   ├── skill_1_data_collector.py    # 数据获取与归一化
│   ├── skill_6_deep_analyzer.py     # 根因诊断助手
│   └── skill_10_daily_reporter.py   # 资产简报生成
│
├── workflows/
│   └── daily_inspection.py     # 每日巡检工作流
│       - 漏斗触发逻辑
│       - 质量检查阻断
│       - 规则引擎分析
│       - LLM唤醒（异常时）
│       - 记忆更新
│
├── utils/
│   ├── taienergy_api.py        # 泰能平台API封装
│   └── memory_manager.py       # 记忆文件管理
│
└── memory/
    ├── temp_analysis.md        # 分析认知（临时）
    └── daily_logs/             # 每日原始记录
```

## 核心特性

### 1. 滚动迭代分析
- 从起始日期逐日累积分析
- 每日基于历史记忆发现新规律
- 自动建立指标行为模型

### 2. 深度分析（非简单统计）
- **趋势分解**: 长期趋势 + 周期性 + 残差
- **变点检测**: CUSUM算法检测行为突变
- **周期发现**: 自相关 + 傅里叶变换
- **异常检测**: 基于基线模型的偏离度

### 3. 漏斗触发（Safeguards）
- 数据质量 < 60: 阻断
- 正常: 轻量级规则引擎
- 异常: 唤醒LLM深度诊断

### 4. 三层记忆体系
- L1: 静态档案（设备信息）
- L2: 动态履历（每日记录）
- L3: 认知知识（深度洞察）

## 使用方法

```bash
# 安装依赖
pip install -r requirements.txt

# 单日分析
python main.py --date 2025-02-22

# 滚动分析（从2025-02-22到今天）
python main.py --start-date 2025-02-22

# 指定范围
python main.py --start-date 2025-02-22 --end-date 2025-02-24
```

## 输出

- 控制台: 实时分析日志
- 记忆文件: `memory/temp_analysis.md`
- 每日记录: `memory/daily_logs/YYYY-MM-DD.json`