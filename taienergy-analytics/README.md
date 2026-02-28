# 光伏设备滚动迭代深度分析系统

## 系统概述

基于泰能平台 API，对光伏逆变器设备进行逐日深度分析，自动发现指标规律（趋势、周期、异常模式）。

## 核心特性

- **滚动迭代**：从起始日期逐日分析，累积认知
- **深度分析**：时间序列分解、变点检测、周期发现
- **临时记忆**：测试阶段使用 Markdown 文件存储
- **自动进化**：每日对比历史，发现新规律

## 目录结构

```
taienergy-analytics/
├── core/                       # 核心分析模块
│   ├── __init__.py
│   ├── base_analyzer.py        # 分析器基类
│   ├── time_series_analyzer.py # 时间序列深度分析
│   └── change_point_detector.py # 变点检测
├── skills/                     # Skill 实现
│   ├── __init__.py
│   ├── skill_1_data_collector.py    # 数据获取
│   ├── skill_6_deep_analyzer.py     # 深度分析
│   └── skill_10_daily_reporter.py   # 日报生成
├── memory/                     # 临时记忆存储
│   ├── temp_analysis.md        # 分析认知（临时）
│   └── daily_logs/             # 每日原始记录
├── workflows/                  # 工作流
│   └── daily_inspection.py     # 每日巡检工作流
├── utils/                      # 工具函数
│   ├── __init__.py
│   ├── taienergy_api.py        # 泰能 API 封装
│   └── data_processor.py       # 数据处理
├── config/                     # 配置
│   └── device_config.py        # 设备配置
├── tests/                      # 测试
│   └── test_analyzer.py
└── main.py                     # 主入口
```

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置设备
编辑 config/device_config.py

# 3. 运行每日分析
python main.py --device XHDL_1NBQ --start-date 2025-02-22

# 4. 查看分析报告
cat memory/temp_analysis.md
```

## 分析指标

基于物模型 136 个参数，重点关注：
- 电流类：ai52-ai54（电网三相）、ai6-ai44（PV组串）
- 电压类：ai49-ai51（电网三相）、ai5-ai43（PV组串）
- 功率类：ai56（有功）、ai57（无功）、ai45（输入功率）
- 温度类：ai61（内部温度）
- 发电量：ai67（累计）、ai68（当日）

## 记忆文件格式

见 memory/temp_analysis.md 模板

## V5.1 指标治理与技能使用规范

为避免脚本与指标“杂草化”，V5.1 强制以下规范：

**指标新增规范**
1. 所有指标必须登记到 `config/indicators/registry.json` 才能参与计算与报表。
2. 指标必须包含字段：`source`（user/constructed/llm）、`scope`（station/inverter/string）、`level`（L1/L2/L3）、`lifecycle_status`（pending/approved/retired）。
3. LLM 发现指标只能进入 `config/indicators/llm_generated/pending/`，批准后再合并进 registry。

**脚本与能力使用规范**
1. 所有脚本必须登记在 `skills_registry.yaml`，未登记脚本禁止被入口调用。
2. 脚本必须归类：`atomic` / `workflow` / `entrypoint`。
3. 报表生成必须走 workflow，并输出到 `memory/reports/`。

**大模型调用规范**
1. LLM 只允许通过既定 workflow 触发，不允许直接调用数据采集接口。
2. LLM 产出必须是结构化 JSON，进入 pending 审核区。
3. 任何 LLM 产出不得直接写入 registry。
