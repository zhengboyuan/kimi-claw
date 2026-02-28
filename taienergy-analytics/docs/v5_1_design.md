# 资产运营分析 V5.1 设计文档

**版本**: V5.1  
**日期**: 2026-02-28  
**作者**: 资产运营团队（草案）

---

## 一、设计目标

在 V5.0 基础上建立**指标体系 + 生命周期 + 报表体系 + 能力脚本管理**的闭环，使系统可扩展但不混乱。

核心目标：
- 指标具备统一注册表与生命周期管理
- 指标发现具备“候选-审核-上线-退役”闭环
- 报表具备模板化、层级化（场站/逆变器）
- 脚本能力统一注册，入口统一，避免“杂草化”

---

## 二、核心原则

1. 指标是资产，必须注册
2. 发现与上线解耦
3. 数据层、指标层、报表层职责分离
4. 脚本必须归类、注册、可追踪
5. 入口统一、执行路径清晰

---

## 三、总体架构（修订版）

```
taienergy-analytics/
├── config/
│   ├── station/
│   │   └── station_config.yaml
│   └── indicators/
│       ├── registry.json            # 指标注册表（权威）
│       ├── user/                    # 用户指标
│       ├── constructed/             # 系统组合指标
│       └── llm_generated/           # LLM 发现指标
│           ├── pending/             # 待审核
│           ├── approved/            # 已通过
│           └── retired/             # 退役
│
├── core/
│   ├── indicator_calculator.py      # 只读 registry.json 计算
│   ├── indicator_registry.py        # 注册表读写/校验
│   ├── discovery/                   # 三层发现引擎
│   └── lifecycle_manager.py         # 生命周期流转
│
├── skills_registry.yaml             # 统一脚本注册表（新增）
├── skills/
│   ├── atomic/                      # 原子能力
│   ├── workflows/                   # 组合能力
│   └── entrypoints/                 # 统一入口
│
├── reports/
│   ├── templates/
│   │   ├── daily_station.md
│   │   ├── daily_inverter.md
│   │   ├── weekly_station.md
│   │   └── weekly_inverter.md
│   └── report_generator.py
│
├── memory/
│   ├── indicators/
│   │   ├── registry_history.json
│   │   └── decisions/
│   ├── reports/
│   ├── insights/
│   └── devices/
│
└── docs/
    └── v5_1_design.md
```

---

## 四、指标体系设计

### 4.1 指标注册表格式（示例）

```json
{
  "id": "pr_value",
  "name": "系统效率(PR值)",
  "source": "user",
  "scope": "station",
  "level": "L2",
  "formula": "actual_energy / theoretical_energy",
  "inputs": ["actual_energy", "theoretical_energy"],
  "aggregation": "daily",
  "lifecycle_status": "approved",
  "owner": "user",
  "version": "1.0",
  "created_at": "2026-02-28"
}
```

### 4.2 字段规范

- `source`: user / constructed / llm
- `scope`: station / inverter / string
- `level`: L1 / L2 / L3
- `lifecycle_status`: pending / approved / retired

### 4.3 注册表作为唯一权威

- 指标计算、报表生成、评估必须从 `registry.json` 读取
- 禁止硬编码指标列表

---

## 五、三层指标发现

保持 V5.0 设计：
- Layer 1: 基础特征挖掘
- Layer 2: 组合构造
- Layer 3: 语义创新

所有新指标统一进入 `pending`，由生命周期管理流转。

---

## 六、脚本与能力管理（新增核心章节）

### 6.1 三层能力结构

**A. 原子能力（Atomic）**
- 只做一件事，可复用
- 例如：`query_device_history`、`calc_pr_value`

**B. 组合能力（Workflow）**
- 由多个原子能力组合形成业务动作
- 例如：`daily_station_report`、`weekly_inspection`

**C. 入口层（Entrypoint）**
- 用户指令统一入口，解析参数、路由到 workflow

### 6.2 统一脚本注册表

`skills_registry.yaml` 作为脚本登记中心：

```yaml
- id: query_device_history
  type: atomic
  input: [device_sn, point_code, start, end]
  output: json
  owner: taienergy-device
  tags: [query, device]

- id: daily_inverter_report
  type: workflow
  input: [device_sn, date]
  output: markdown
  owner: taienergy-analytics
  tags: [report, daily]
```

**规则**
- 无注册脚本不得调用
- 每个脚本必须明确 owner、输入输出、标签

---

## 七、报表体系设计

### 7.1 报表分层
- 场站级报表
- 逆变器级报表

### 7.2 报表模板化
- `daily_station.md`
- `daily_inverter.md`
- `weekly_station.md`
- `weekly_inverter.md`

### 7.3 输出路径规范

```
memory/reports/daily/station/2026-02-28.md
memory/reports/daily/inverter/XHDL_1NBQ/2026-02-28.md
```

---

## 八、发现闭环与记忆统一

- `pending → approved → retired`
- 所有变更写入 `memory/indicators/registry_history.json`
- 审核记录写入 `memory/indicators/decisions/`
- 洞察归档到 `memory/insights/`

---

## 九、用户故事（Usage Stories）

### 9.1 新增用户指标（批量导入）
**作为** 资产运营人员  
**我希望** 导入一批已定义的指标  
**以便** 快速建立基础指标体系  

验收：
- 写入 `registry.json`
- `source = user`
- `lifecycle_status = approved`
- 记录到 `registry_history.json`

### 9.2 系统发现指标（候选）
**作为** 系统  
**我希望** 自动发现异常特征并形成候选指标  
**以便** 让用户审核  

验收：
- 写入 `llm_generated/pending/`
- 去重机制生效

### 9.3 审核并上线指标
**作为** 运营人员  
**我希望** 审核候选指标并上线  
**以便** 参与计算  

验收：
- 移入 `approved`
- 更新 `registry.json`
- 写入审核记录

### 9.4 生成场站日报
**作为** 运营人员  
**我希望** 生成某场站日报  
**以便** 查看关键指标表现  

验收：
- 调用 `daily_station_report`
- 输出到 `memory/reports/daily/station/`

### 9.5 生成逆变器日报
**作为** 运维人员  
**我希望** 生成某设备日报  
**以便** 判断是否异常  

验收：
- 调用 `daily_inverter_report`
- 输出到 `memory/reports/daily/inverter/`

### 9.6 指标版本追踪
**作为** 运营主管  
**我希望** 查看指标历史版本变化  
**以便** 理解指标演化  

验收：
- `registry_history.json` 记录变更
- 可追溯时间和来源

### 9.7 查询设备历史数据
**作为** 运营人员  
**我希望** 查询设备某指标历史数据  
**以便** 快速排查  

验收：
- 调用 `query_device_history`
- 返回 JSON/表格结果

### 9.8 脚本管理与统一入口
**作为** 系统管理员  
**我希望** 所有脚本必须登记  
**以便** 防止脚本杂草化  

验收：
- 新脚本必须登记 `skills_registry.yaml`
- 无登记脚本不可被 entrypoint 调用

---

## 十、实施路线

1. V5.1-alpha
- 引入 `registry.json`
- 引入 `skills_registry.yaml`
- 核心脚本完成登记

2. V5.1-beta
- workflow 标准化
- 报表模板落地

3. V5.1-rc
- 生命周期闭环
- 记忆结构统一

4. V5.1
- 全量迁移旧脚本

---

**文档版本**: v1.0  
**最后更新**: 2026-02-28
