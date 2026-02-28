# 指标规格: current_power_ratio

## 发现背景
- **发现日期**: 2025-08-15
- **发现方式**: cross_features
- **触发条件**: 数据扫描自动发现
- **信息增益**: 0.9571
- **变异系数**: 0.0140

## 计算定义
```python
def calculate_current_power_ratio(data):
    return data['ai56'] * data['ai68'] / 0.96
```

## 依赖指标
['ai56', 'ai68']

## 验收标准（Ralph验证）
- [ ] 代码通过 flake8 检查
- [ ] 单元测试覆盖 3 种边界场景（空数据/异常值/缺失值）
- [ ] 用最近7天数据实测，输出格式正确
- [ ] 执行时间 < 500ms/设备/天

## 输出信号
<promise>DONE</promise> 当所有标准验证通过
