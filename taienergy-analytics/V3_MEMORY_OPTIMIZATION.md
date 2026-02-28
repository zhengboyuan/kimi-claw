# V3.0 内存优化方案

## 问题诊断

### 根本原因
1. **DataFrame循环merge** - `_merge_daily_data` 方法中逐个merge，内存碎片累积
2. **重复timestamp列** - 每个指标都带timestamp，数据冗余
3. **Outer join膨胀** - 时间戳不完全一致时产生大量NaN
4. **无内存释放** - 大对象（daily_data）在处理后未显式释放

### 证据
- OOM时进程占用3GB内存
- DataFrame本身仅20KB数据
- PerformanceWarning: DataFrame is highly fragmented

---

## 优化方案

### 方案1：重构_merge_daily_data（推荐）

**原代码问题：**
```python
# 循环merge，内存碎片
merged = dfs[0]
for df in dfs[1:]:
    merged = pd.merge(merged, df, on='timestamp', how='outer')  # 每次创建新DF
```

**优化后：**
```python
def _merge_daily_data(self, daily_data: Dict) -> pd.DataFrame:
    """合并所有指标数据为DataFrame（内存优化版）"""
    import pandas as pd
    
    # 1. 提取所有数据为字典，避免创建多个小DataFrame
    data_dict = {'timestamp': None}
    
    for code, df in daily_data.items():
        if df.empty or 'value' not in df.columns:
            continue
        if data_dict['timestamp'] is None:
            data_dict['timestamp'] = df['timestamp'].values
        data_dict[code] = df['value'].values
    
    if data_dict['timestamp'] is None:
        return pd.DataFrame()
    
    # 2. 一次性创建DataFrame，避免碎片
    return pd.DataFrame(data_dict)
```

### 方案2：使用pd.concat替代merge

```python
def _merge_daily_data(self, daily_data: Dict) -> pd.DataFrame:
    """合并所有指标数据为DataFrame（concat优化版）"""
    import pandas as pd
    
    # 1. 只提取value列，设置timestamp为index
    dfs = []
    for code, df in daily_data.items():
        if df.empty or 'value' not in df.columns:
            continue
        temp = df[['timestamp', 'value']].set_index('timestamp')
        temp.columns = [code]
        dfs.append(temp)
    
    if not dfs:
        return pd.DataFrame()
    
    # 2. 一次性concat，然后reset_index
    merged = pd.concat(dfs, axis=1)
    merged = merged.reset_index()
    
    return merged
```

### 方案3：流式处理（不合并DataFrame）

```python
def run_stage4_composite_evolution_streaming(self, date_str: str, daily_data: Dict) -> Dict:
    """Stage 4: 流式处理，不合并大DataFrame"""
    
    # 直接提取需要的列，不合并
    pv_data = {k: v for k, v in daily_data.items() if k in ['ai10', 'ai12', 'ai16', 'ai20']}
    power_data = {k: v for k, v in daily_data.items() if k in ['ai45', 'ai56']}
    vol_data = {k: v for k, v in daily_data.items() if k in ['ai49', 'ai50', 'ai51']}
    
    # 分别计算复合指标，不保存中间结果
    survivors = {}
    
    # 组串差异
    survivors.update(self._calc_pv_diff(pv_data))
    
    # 效率损失
    survivors.update(self._calc_efficiency_loss(power_data))
    
    # 三相不平衡
    survivors.update(self._calc_vol_unbalance(vol_data))
    
    return survivors
```

### 方案4：分批处理 + 显式GC

```python
import gc

def run_rolling_analysis_v3(self, start_date: str, end_date: str):
    """分批处理，每30天释放一次内存"""
    
    batch_size = 30
    current = start_dt
    
    while current <= end_dt:
        batch_end = min(current + timedelta(days=batch_size), end_dt)
        
        # 处理这一批
        self._process_batch(current, batch_end)
        
        # 显式释放内存
        gc.collect()
        
        current = batch_end + timedelta(days=1)
```

---

## 推荐实施顺序

1. **立即实施**：方案1（重构_merge_daily_data）- 改动最小，效果明显
2. **短期优化**：方案4（分批+GC）- 解决OOM根本问题
3. **长期优化**：方案3（流式处理）- 架构升级

---

## 预期效果

| 优化 | 内存占用 | 实现难度 |
|------|----------|----------|
| 原代码 | 3GB+ | - |
| 方案1 | <500MB | 低 |
| 方案4 | <1GB | 低 |
| 方案3 | <200MB | 中 |

---

**记录时间**：2026-02-25 14:25
