"""
Skill 1: 数据获取与归一化器
基于泰能平台 API 获取数据并进行初步处理
"""
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from utils.taienergy_api import get_api
from config.device_config import DEVICES


class DataCollector:
    """
    数据获取与归一化器
    
    职责：
    1. 从泰能平台拉取设备历史数据
    2. 获取设备属性列表（中文名称映射）
    3. 数据清洗（剔除无效值、格式转换）
    4. 数据质量评估
    
    第一阶段改造：
    - DataFrame key 统一使用 ai1-200/di1-200 代码
    - 中文名称存入 metadata，LLM 交互时再查表替换
    - 彻底消除重复计算（130个指标→65个真实指标）
    
    Safeguard 1: 禁止 LLM 接触原始时序数据，只传统计值
    """
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.device_config = DEVICES.get(device_sn, {})
        self.api = get_api()
        self.data_quality_score = 100  # 数据质量评分
        self._property_map = {}  # 属性映射缓存 {ppk: ppn}
        self._property_unit_map = {}  # 单位映射 {ppk: unit}
        self._property_type_map = {}  # 类型映射 {ppk: type} - 新增
    
    def _load_device_properties(self) -> Dict[str, str]:
        """
        加载设备属性列表，建立代码到中文名称的映射
        
        Returns:
            {ppk: ppn} 映射字典
        """
        if self._property_map:
            return self._property_map
        
        try:
            properties = self.api.get_device_properties(self.device_sn)
            self._property_map = {ppk: info["ppn"] for ppk, info in properties.items()}
            self._property_unit_map = {ppk: info.get("unit", "") for ppk, info in properties.items()}
            # 新增：自动分类指标类型
            self._property_type_map = self._classify_indicators(properties)
            print(f"  已加载 {len(self._property_map)} 个属性名称映射")
            return self._property_map
        except Exception as e:
            print(f"  ⚠️ 获取属性列表失败: {e}，将使用原始代码")
            return {}
    
    def _classify_indicators(self, properties: Dict) -> Dict[str, str]:
        """
        自动分类指标类型（用于差异化评分）
        
        分类规则：
        - power: 功率类（输入功率、输出功率等）
        - current: 电流类
        - voltage: 电压类
        - frequency: 频率类
        - temperature: 温度类
        - status: 状态/DI类
        """
        type_map = {}
        
        for ppk, info in properties.items():
            ppn = info.get("ppn", "").lower()
            unit = info.get("unit", "").lower()
            
            # 根据名称和单位判断类型
            if "功率" in ppn or "power" in ppn or unit in ["w", "kw", "mw"]:
                type_map[ppk] = "power"
            elif "电流" in ppn or "current" in ppn or unit in ["a"]:
                type_map[ppk] = "current"
            elif "电压" in ppn or "voltage" in ppn or unit in ["v", "kv"]:
                type_map[ppk] = "voltage"
            elif "频率" in ppn or "frequency" in ppn or unit in ["hz"]:
                type_map[ppk] = "frequency"
            elif "温度" in ppn or "temperature" in ppn or unit in ["°c", "℃", "c"]:
                type_map[ppk] = "temperature"
            elif ppk.startswith("di") or "状态" in ppn or "status" in ppn:
                type_map[ppk] = "status"
            else:
                type_map[ppk] = "other"
        
        return type_map
    
    def get_property_name(self, ppk: str) -> str:
        """获取属性中文名称"""
        if not self._property_map:
            self._load_device_properties()
        return self._property_map.get(ppk, ppk)
    
    def get_property_unit(self, ppk: str) -> str:
        """获取属性单位"""
        if not self._property_unit_map:
            self._load_device_properties()
        return self._property_unit_map.get(ppk, "")
    
    def get_property_type(self, ppk: str) -> str:
        """获取属性类型（用于差异化评分）"""
        if not self._property_type_map:
            self._load_device_properties()
        return self._property_type_map.get(ppk, "other")
    
    def get_all_property_codes(self) -> List[str]:
        """获取所有属性代码"""
        if not self._property_map:
            self._load_device_properties()
        return list(self._property_map.keys()) if self._property_map else []
    
    def get_indicator_metadata(self) -> Dict[str, Dict]:
        """
        获取所有指标的元数据（用于 LLM 交互时查表）
        
        Returns:
            {
                "ai45": {"name": "逆变器输入功率", "unit": "W", "type": "power"},
                "ai51": {"name": "逆变器电网C相电压", "unit": "V", "type": "voltage"}
            }
        """
        if not self._property_map:
            self._load_device_properties()
        
        metadata = {}
        for code in self._property_map.keys():
            metadata[code] = {
                "name": self._property_map.get(code, code),
                "unit": self._property_unit_map.get(code, ""),
                "type": self._property_type_map.get(code, "other")
            }
        return metadata
    
    def collect_daily_data(
        self,
        date_str: str,
        indicators: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        收集单日数据
        
        Args:
            date_str: 日期，如 "2025-02-22"
            indicators: 指标列表，默认使用设备配置的全部指标
        
        Returns:
            指标代码 -> DataFrame 的字典（key 为 ai1-200/di1-200 代码）
        """
        # 首先加载属性列表（如果没有指定指标）
        if indicators is None:
            indicators = self.get_all_property_codes()
            if not indicators:
                # 如果获取属性失败，回退到配置中的指标
                indicators = self.device_config.get("all_indicators", [])
        
        # 第一阶段改造：只保留 ai1-200 和 di1-200 格式的代码
        filtered_indicators = [
            ind for ind in indicators 
            if (ind.startswith("ai") or ind.startswith("di")) and ind[2:].isdigit()
        ]
        
        if len(filtered_indicators) < len(indicators):
            print(f"  过滤后指标: {len(indicators)} -> {len(filtered_indicators)} 个")
        
        print(f"[{self.device_sn}] 正在获取 {date_str} 的数据，共 {len(filtered_indicators)} 个指标...")
        
        # 分批查询，避免 API 超时
        batch_size = 20  # 每批查询 20 个指标
        all_raw_data = []
        
        for i in range(0, len(filtered_indicators), batch_size):
            batch = filtered_indicators[i:i+batch_size]
            print(f"  查询批次 {i//batch_size + 1}/{(len(filtered_indicators)-1)//batch_size + 1}: {len(batch)} 个指标")
            
            try:
                batch_data = self.api.query_daily_data(
                    device_sn=self.device_sn,
                    point_codes=batch,
                    date_str=date_str,
                    interval=1,
                    timetype=3
                )
                all_raw_data.extend(batch_data)
            except Exception as e:
                print(f"    批次查询失败: {e}")
                continue
        
        if not all_raw_data:
            print(f"[{self.device_sn}] {date_str} 无数据")
            return {}
        
        # 转换为 DataFrame 格式（使用代码作为 key）
        result = self._process_raw_data(all_raw_data, filtered_indicators)
        
        # 数据质量评估
        self._assess_data_quality(result)
        
        print(f"  ✅ 成功获取 {len(result)} 个指标数据（共 {len(filtered_indicators)} 个）")
        print(f"  数据质量评分: {self.data_quality_score:.1f}/100")
        
        return result
    
    def _process_raw_data(
        self,
        raw_data: List[Dict],
        indicators: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """
        处理原始数据
        
        第一阶段改造：
        - DataFrame key 使用指标代码（ai1-200/di1-200）
        - 中文名称存入 df.attrs 元数据
        """
        result = {}
        
        # 转换为 DataFrame
        df = pd.DataFrame(raw_data)
        
        if df.empty:
            return result
        
        # 转换时间戳
        df['timestamp'] = pd.to_datetime(df['ts'], unit='ms')
        df['hour'] = df['timestamp'].dt.hour
        
        # 按指标拆分，使用代码作为 key
        for indicator in indicators:
            if indicator in df.columns:
                # 获取中文名称和单位
                chinese_name = self.get_property_name(indicator)
                unit = self.get_property_unit(indicator)
                ind_type = self.get_property_type(indicator)
                
                indicator_df = df[['timestamp', 'hour', indicator]].copy()
                indicator_df.rename(columns={indicator: 'value'}, inplace=True)
                
                # 数据清洗：转换非数值为 NaN
                indicator_df['value'] = pd.to_numeric(indicator_df['value'], errors='coerce')
                
                # 添加元数据到 df.attrs（不污染 key）
                indicator_df.attrs['code'] = indicator
                indicator_df.attrs['name'] = chinese_name
                indicator_df.attrs['unit'] = unit
                indicator_df.attrs['type'] = ind_type  # 新增：类型标记
                
                # 使用代码作为 key（不是中文名称）
                result[indicator] = indicator_df
        
        return result
    
    def _assess_data_quality(self, data: Dict[str, pd.DataFrame]):
        """
        评估数据质量
        
        评分标准：
        - 数据完整度（40分）：缺失率 < 10%
        - 数据有效性（30分）：有效值比例 > 80%
        - 时间连续性（30分）：时间间隔均匀
        """
        if not data:
            self.data_quality_score = 0
            return
        
        scores = []
        
        for indicator, df in data.items():
            if df.empty:
                continue
            
            score = 100
            
            # 1. 完整度检查（期望 24 小时数据）
            completeness = len(df) / 24 * 40
            score = min(score, completeness + 60)  # 基础分 60
            
            # 2. 有效性检查
            valid_ratio = df['value'].notna().sum() / len(df)
            if valid_ratio < 0.5:  # 有效值少于 50%
                score -= 30
            elif valid_ratio < 0.8:
                score -= 15
            
            # 3. 异常值检查（简单的 3-sigma 原则）
            values = df['value'].dropna()
            if len(values) > 3:
                mean = values.mean()
                std = values.std()
                if std > 0:
                    outliers = (abs(values - mean) > 3 * std).sum()
                    outlier_ratio = outliers / len(values)
                    if outlier_ratio > 0.1:  # 异常值超过 10%
                        score -= 20
            
            scores.append(score)
        
        # 取平均质量分
        self.data_quality_score = sum(scores) / len(scores) if scores else 0
    
    def get_data_quality_report(self) -> Dict:
        """获取数据质量报告"""
        return {
            "device_sn": self.device_sn,
            "quality_score": self.data_quality_score,
            "is_healthy": self.data_quality_score >= 90,
            "can_proceed": self.data_quality_score >= 60  # 低于 60 分阻断分析
        }
    
    def collect_date_range(
        self,
        start_date: str,
        end_date: str,
        indicators: Optional[List[str]] = None
    ) -> Dict[str, List[pd.DataFrame]]:
        """
        收集日期范围数据（用于批量分析）
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = {}
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            daily_data = self.collect_daily_data(date_str, indicators)
            
            for indicator, df in daily_data.items():
                if indicator not in all_data:
                    all_data[indicator] = []
                all_data[indicator].append(df)
            
            current_dt += timedelta(days=1)
        
        return all_data
    
    # ========== 指标探测方法 ==========
    
    def discover_all_indicators(self, probe_range: int = 200) -> List[str]:
        """
        全量探测设备可用指标
        
        Args:
            probe_range: 探测范围（默认 ai1-200, di1-200）
            
        Returns:
            可用指标代码列表
        """
        return self.api.discover_available_points(
            device_sn=self.device_sn,
            probe_range=probe_range,
            force_full_scan=True
        )
    
    def probe_unknown_indicators(
        self,
        known_indicators: List[str],
        sample_size: int = 10
    ) -> List[str]:
        """
        抽样探测未知指标
        
        日常运行时使用：随机抽取 sample_size 个未监控的点位尝试读取
        用于发现厂家静默更新的新点位
        
        Args:
            known_indicators: 已知的指标列表
            sample_size: 抽样数量
            
        Returns:
            新发现的指标代码列表
        """
        return self.api.probe_unknown_indicators(
            device_sn=self.device_sn,
            known_indicators=known_indicators,
            sample_size=sample_size
        )
