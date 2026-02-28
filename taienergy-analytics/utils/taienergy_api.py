"""
泰能平台 API 封装
基于已测试的接口实现
增加自动发现能力
增加超时重试机制（V3.0优化）
"""
import requests
import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple


class TaienergyAPI:
    """泰能平台 API 客户端"""
    
    # 超时重试配置
    DEFAULT_TIMEOUT = 10       # 默认超时10秒
    MAX_RETRIES = 3            # 最大重试3次
    RETRY_DELAY = 2            # 重试间隔2秒
    
    def __init__(self, base_url: str, appid: str, appkey: str):
        self.base_url = base_url
        self.appid = appid
        self.appkey = appkey
        self._token = None
        self._property_cache = {}  # 缓存设备属性列表 {device_sn: {ppk: property_info}}
        self._discovered_points_cache = {}  # 缓存发现的点位 {device_sn: [ppk, ...]}
    
    def _request_with_retry(
        self, 
        method: str, 
        url: str, 
        headers: Dict = None, 
        json_data: Dict = None,
        timeout: int = None,
        max_retries: int = None
    ) -> requests.Response:
        """
        带重试机制的HTTP请求
        
        Args:
            method: 请求方法 (get/post)
            url: 请求URL
            headers: 请求头
            json_data: JSON数据
            timeout: 超时时间（秒）
            max_retries: 最大重试次数
        
        Returns:
            Response对象
        
        Raises:
            Exception: 所有重试失败后抛出异常
        """
        timeout = timeout or self.DEFAULT_TIMEOUT
        max_retries = max_retries or self.MAX_RETRIES
        
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                if method.lower() == 'get':
                    response = requests.get(url, headers=headers, timeout=timeout)
                else:
                    response = requests.post(
                        url, 
                        headers=headers, 
                        json=json_data,
                        timeout=timeout
                    )
                
                # 检查HTTP状态码
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                print(f"    ⚠️ 请求超时 (尝试 {attempt + 1}/{max_retries}): {str(e)[:50]}")
                if attempt < max_retries - 1:
                    time.sleep(self.RETRY_DELAY)
                    
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                print(f"    ⚠️ 连接错误 (尝试 {attempt + 1}/{max_retries}): {str(e)[:50]}")
                if attempt < max_retries - 1:
                    time.sleep(self.RETRY_DELAY * 2)  # 连接错误等待更久
                    
            except requests.exceptions.RequestException as e:
                last_exception = e
                print(f"    ⚠️ 请求异常 (尝试 {attempt + 1}/{max_retries}): {str(e)[:50]}")
                if attempt < max_retries - 1:
                    time.sleep(self.RETRY_DELAY)
        
        # 所有重试失败
        raise Exception(f"请求失败，已重试{max_retries}次: {last_exception}")
    
    def _get_token(self) -> str:
        """获取访问令牌（带重试）"""
        if self._token:
            return self._token
            
        url = f"{self.base_url}/api/vc-backend/Third/ThirdLogin/Login"
        headers = {
            "appid": self.appid,
            "appkey": self.appkey
        }
        
        response = self._request_with_retry(
            method='get',
            url=url,
            headers=headers,
            timeout=10,
            max_retries=3
        )
        
        data = response.json()
        
        if data.get("code") == 200:
            self._token = data.get("data")
            return self._token
        else:
            raise Exception(f"登录失败: {data}")
    
    def get_device_properties(self, device_sn: str) -> Dict[str, Dict]:
        """
        获取设备属性列表
        
        Args:
            device_sn: 设备序列号
            
        Returns:
            属性字典 {ppk: {ppn, unit, dtype, ...}}
        """
        # 检查缓存
        if device_sn in self._property_cache:
            return self._property_cache[device_sn]
        
        url = f"{self.base_url}/api/vc-backend/third/device/GetDevicePropertyList"
        
        token = self._get_token()
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": token
        }
        
        payload = {
            "deviceSn": device_sn
        }
        
        response = self._request_with_retry(
            method='post',
            url=url,
            headers=headers,
            json_data=payload,
            timeout=20,
            max_retries=3
        )
        
        data = response.json()
        
        if data.get("code") == 200:
            properties = data.get("data", [])
            # 转换为字典格式 {ppk: property_info}
            property_dict = {}
            for prop in properties:
                ppk = prop.get("ppk")
                if ppk:
                    property_dict[ppk] = {
                        "ppk": ppk,
                        "ppn": prop.get("ppn", ppk),  # 属性名称（中文）
                        "unit": prop.get("unit", ""),  # 单位
                        "dtype": prop.get("dtype", "float"),  # 数据类型
                        "description": prop.get("description", "")  # 描述（如有）
                    }
            
            # 缓存结果
            self._property_cache[device_sn] = property_dict
            print(f"  获取到 {len(property_dict)} 个设备属性")
            return property_dict
        else:
            raise Exception(f"获取设备属性失败: {data}")
    
    def discover_available_points(
        self, 
        device_sn: str, 
        probe_range: int = 200,
        force_full_scan: bool = False
    ) -> List[str]:
        """
        自动发现设备可用点位
        
        策略：
        1. 首次运行：全量探测 ai1-ai{probe_range}, di1-di{probe_range}
        2. 日常运行：返回已缓存的点位列表（除非 force_full_scan=True）
        
        Args:
            device_sn: 设备序列号
            probe_range: 探测范围（默认 200）
            force_full_scan: 是否强制全量扫描
            
        Returns:
            可用点位代码列表
        """
        # 检查缓存（非强制扫描时）
        if not force_full_scan and device_sn in self._discovered_points_cache:
            cached = self._discovered_points_cache[device_sn]
            print(f"  使用缓存的点位列表: {len(cached)} 个")
            return cached
        
        print(f"  执行全量点位探测 (范围: ai1-ai{probe_range}, di1-di{probe_range})...")
        
        # 生成探测列表
        probe_points = []
        probe_points.extend([f"ai{i}" for i in range(1, probe_range + 1)])
        probe_points.extend([f"di{i}" for i in range(1, probe_range + 1)])
        
        # 分批探测（每批20个，查询1小时数据）
        available_points = []
        batch_size = 20
        
        # 获取当前时间前1小时的时间戳
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=1)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        
        for i in range(0, len(probe_points), batch_size):
            batch = probe_points[i:i+batch_size]
            
            try:
                batch_data = self.query_history_data(
                    device_sn=device_sn,
                    point_codes=batch,
                    start_time=start_ts,
                    end_time=end_ts,
                    interval=1,
                    timetype=3
                )
                
                # 检查哪些点位有数据
                if batch_data:
                    # 获取返回数据中所有非空的字段
                    for record in batch_data:
                        for key in record.keys():
                            if key not in ['ts', 'timestamp'] and record[key] is not None:
                                if key not in available_points:
                                    available_points.append(key)
                
            except Exception as e:
                # 探测失败，继续下一批
                continue
            
            # 每10批报告一次进度
            if (i // batch_size + 1) % 10 == 0:
                print(f"    进度: {min(i+batch_size, len(probe_points))}/{len(probe_points)}, 已发现 {len(available_points)} 个点位")
        
        # 排序并缓存结果
        available_points.sort()
        self._discovered_points_cache[device_sn] = available_points
        
        print(f"  探测完成: 共发现 {len(available_points)} 个可用点位")
        return available_points
    
    def probe_unknown_indicators(
        self,
        device_sn: str,
        known_indicators: List[str],
        sample_size: int = 10
    ) -> List[str]:
        """
        抽样探测未知指标
        
        日常运行时使用：随机抽取 sample_size 个未监控的点位尝试读取
        用于发现厂家静默更新的新点位
        
        Args:
            device_sn: 设备序列号
            known_indicators: 已知的指标列表
            sample_size: 抽样数量
            
        Returns:
            新发现的指标代码列表
        """
        # 获取全量探测结果
        all_points = self.discover_available_points(device_sn, force_full_scan=False)
        
        # 找出未监控的点位
        known_set = set(known_indicators)
        unknown_points = [p for p in all_points if p not in known_set]
        
        if not unknown_points:
            return []
        
        # 随机抽样
        import random
        sample_points = random.sample(unknown_points, min(sample_size, len(unknown_points)))
        
        print(f"  抽样探测 {len(sample_points)} 个未知点位...")
        
        # 验证这些点位当前是否有数据
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=1)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        
        new_indicators = []
        
        try:
            batch_data = self.query_history_data(
                device_sn=device_sn,
                point_codes=sample_points,
                start_time=start_ts,
                end_time=end_ts,
                interval=1,
                timetype=3
            )
            
            if batch_data:
                for record in batch_data:
                    for key in record.keys():
                        if key not in ['ts', 'timestamp'] and record[key] is not None:
                            if key not in known_set and key not in new_indicators:
                                new_indicators.append(key)
                                print(f"    发现新指标: {key}")
        
        except Exception as e:
            print(f"    探测失败: {e}")
        
        return new_indicators
    
    def get_property_name(self, device_sn: str, ppk: str) -> str:
        """
        获取属性中文名称
        
        Args:
            device_sn: 设备序列号
            ppk: 属性标识（如 temp26）
            
        Returns:
            中文名称（如 "单体温度26"），如未找到则返回 ppk
        """
        properties = self.get_device_properties(device_sn)
        prop = properties.get(ppk)
        if prop:
            return prop.get("ppn", ppk)
        return ppk
    
    def get_all_property_codes(self, device_sn: str) -> List[str]:
        """
        获取所有属性代码列表
        
        Args:
            device_sn: 设备序列号
            
        Returns:
            属性代码列表 [ppk1, ppk2, ...]
        """
        properties = self.get_device_properties(device_sn)
        return list(properties.keys())
    
    def query_history_data(
        self,
        device_sn: str,
        point_codes: List[str],
        start_time: int,
        end_time: int,
        interval: int = 1,
        timetype: int = 3,
        dtype: int = 1
    ) -> List[Dict]:
        """
        查询设备历史数据
        
        Args:
            device_sn: 设备序列号
            point_codes: 点位代码列表
            start_time: 开始时间戳（毫秒）
            end_time: 结束时间戳（毫秒）
            interval: 时间间隔量
            timetype: 时间类型 (3=时, 4=分)
            dtype: 数据类型 (0=AVG, 1=MAX, 2=MIN, 3=SUM)
        
        Returns:
            数据列表，每个元素包含 ts 和各指标值
        """
        url = f"{self.base_url}/api/vc-backend/third/device/historydata"
        
        token = self._get_token()
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": token
        }
        
        payload = {
            "deviceSn": device_sn,
            "pointCode": point_codes,
            "starttime": start_time,
            "endtime": end_time,
            "interval": interval,
            "timetype": timetype,
            "dtype": dtype
        }
        
        # 使用重试机制
        response = self._request_with_retry(
            method='post',
            url=url,
            headers=headers,
            json_data=payload,
            timeout=15,  # 历史数据查询稍长
            max_retries=3
        )
        
        data = response.json()
        
        if data.get("code") == 200:
            return data.get("data", [])
        else:
            raise Exception(f"查询失败: {data}")
    
    def query_daily_data(
        self,
        device_sn: str,
        point_codes: List[str],
        date_str: str,
        interval: int = 15,  # 15分钟间隔
        timetype: int = 4    # 4=分钟级
    ) -> List[Dict]:
        """
        查询单日数据（实际值，分钟级）
        
        正确处理北京时间到UTC时间戳的转换
        
        Args:
            device_sn: 设备序列号
            point_codes: 点位代码列表
            date_str: 日期字符串，如 "2025-02-22"
            interval: 时间间隔（分钟）
            timetype: 时间类型 (4=分)
        """
        # 北京时间转UTC时间戳
        # API要求北京时间的时间戳，需要减去8小时得到UTC
        beijing_offset = timedelta(hours=8)
        
        start_dt = datetime.strptime(f"{date_str} 00:00:00", "%Y-%m-%d %H:%M:%S") - beijing_offset
        end_dt = datetime.strptime(f"{date_str} 23:59:59", "%Y-%m-%d %H:%M:%S") - beijing_offset
        
        start_ts = int(start_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ts = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        
        return self.query_history_data(
            device_sn=device_sn,
            point_codes=point_codes,
            start_time=start_ts,
            end_time=end_ts,
            interval=interval,
            timetype=timetype,
            dtype=3  # 实际值
        )


# 全局 API 实例
_api_instance = None

def get_api() -> TaienergyAPI:
    """获取 API 实例（单例）"""
    global _api_instance
    if _api_instance is None:
        from config.device_config import TAIENERGY_CONFIG
        _api_instance = TaienergyAPI(
            base_url=TAIENERGY_CONFIG["base_url"],
            appid=TAIENERGY_CONFIG["appid"],
            appkey=TAIENERGY_CONFIG["appkey"]
        )
    return _api_instance
