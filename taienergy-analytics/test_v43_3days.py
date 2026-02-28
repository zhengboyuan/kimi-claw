import sys
sys.path.insert(0, '.')
from datetime import datetime, timedelta
from workflows.daily_evolution_v43 import run_v43_daily_evolution

print('='*70)
print('V4.3 7月15-17日 3天迭代测试')
print('='*70)

dates = ['2025-07-15', '2025-07-16', '2025-07-17']

for day, date_str in enumerate(dates, 1):
    print(f'\n{"="*70}')
    print(f'第{day}天: {date_str}')
    print(f'{"="*70}')
    
    try:
        result = run_v43_daily_evolution(date_str)
        
        # 汇总结果
        new_indicators = sum(r['new_registered'] for r in result['stage2_indicators'].values())
        evaluated = sum(r['indicators_evaluated'] for r in result['stage2_indicators'].values())
        promoted = sum(len(r['promoted']) for r in result['stage2_indicators'].values())
        retired = sum(len(r['retired']) for r in result['stage2_indicators'].values())
        valid_devices = result['stage1_data']['valid']
        
        print(f'\n【第{day}天结果汇总】')
        print(f'  有效设备: {valid_devices}/16')
        print(f'  新发现指标: {new_indicators}个')
        print(f'  评估指标: {evaluated}个')
        print(f'  晋升L2: {promoted}个')
        print(f'  淘汰: {retired}个')
        
        # 每台设备详情
        print(f'\n  设备详情:')
        for sn, r in result['stage2_indicators'].items():
            if r['new_registered'] > 0 or r['indicators_evaluated'] > 0:
                print(f'    {sn}: +{r["new_registered"]}新, 评估{r["indicators_evaluated"]}个')
        
    except Exception as e:
        print(f'\n错误: {str(e)}')
        import traceback
        traceback.print_exc()

print(f'\n{"="*70}')
print('3天测试完成!')
print(f'{"="*70}')
