"""
资产运营分析 - 场站发电量汇总
计算16台逆变器总发电量
"""
import json
import os
from datetime import datetime, timedelta

def calculate_station_power(date_str):
    """计算指定日期的场站总发电量"""
    memory_base = '/root/.openclaw/workspace/taienergy-analytics/memory'
    
    # 从设备记忆中读取功率数据
    total_power = 0
    device_powers = []
    
    for i in range(1, 17):
        sn = f'XHDL_{i}NBQ'
        device_file = f'{memory_base}/devices/{sn}/memory.json'
        
        if os.path.exists(device_file):
            with open(device_file) as f:
                data = json.load(f)
            
            # 查找指定日期的记录
            for record in data.get('daily_records', []):
                if record.get('date') == date_str:
                    power = record.get('avg_power', 0)
                    device_powers.append((sn, power))
                    total_power += power
                    break
    
    return {
        'date': date_str,
        'total_power': total_power,
        'device_count': len(device_powers),
        'device_powers': sorted(device_powers, key=lambda x: x[1], reverse=True)
    }

if __name__ == '__main__':
    # 计算昨天
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 或者指定日期
    test_date = '2025-10-01'
    
    result = calculate_station_power(test_date)
    
    print(f'【{result["date"]} 场站发电情况】')
    print('='*60)
    print(f'\n【发电量汇总】')
    print(f'  场站总功率: {result["total_power"]:.2f} kW')
    print(f'  在线设备: {result["device_count"]}/16')
    print(f'  单台平均: {result["total_power"]/16:.2f} kW')
    print(f'  估算日发电量: {result["total_power"] * 24 / 1000:.2f} MWh')
    
    print(f'\n【设备明细】')
    for sn, power in result['device_powers']:
        status = '⚠️' if power == 0 else '✅'
        print(f'  {status} {sn}: {power:.2f} kW')
