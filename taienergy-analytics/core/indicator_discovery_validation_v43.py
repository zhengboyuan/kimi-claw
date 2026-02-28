"""
指标发现与验证闭环引擎 (Indicator Discovery & Validation Loop)

核心机制：
Day 1: 穷举变异 → 发现候选指标 → LLM评审 → 注册L3
Day 2+: 每日评估指标表现 → 更新生命值 → 优胜劣汰
持续: 新指标不断孵化，老指标持续验证
"""
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np


class IndicatorDiscoveryValidationEngine:
    """
    指标发现与验证闭环引擎
    
    达尔文进化 + 持续验证
    """
    
    # 进化阈值
    PROMOTION_THRESHOLD = 0.8    # 晋升L2阈值
    RETIREMENT_THRESHOLD = 0.2   # 淘汰阈值
    CERTIFICATION_FLOOR = 0.25   # LLM认证保底
    
    def __init__(self, device_sn: str):
        self.device_sn = device_sn
        self.catalog_path = f"memory/indicator_catalog_{device_sn}.json"
        self.validation_log_path = f"memory/validation_logs/{device_sn}"
        os.makedirs(self.validation_log_path, exist_ok=True)
        
        self.catalog = self._load_catalog()
    
    def _load_catalog(self) -> Dict:
        """加载指标目录"""
        if os.path.exists(self.catalog_path):
            with open(self.catalog_path, 'r') as f:
                return json.load(f)
        return {
            'device_sn': self.device_sn,
            'created_at': datetime.now().isoformat(),
            'indicators': {
                'L0_Candidates': {},   # 候选池
                'L1_Active': {},       # 观察期
                'L2_Core': {},         # 核心指标
                'L3_Synthesized': {},  # 复合指标
                'L4_Retired': {}       # 退役指标
            },
            'discovery_history': [],
            'validation_stats': {
                'total_discovered': 0,
                'total_promoted': 0,
                'total_retired': 0
            }
        }
    
    def _save_catalog(self):
        """保存指标目录"""
        os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
        with open(self.catalog_path, 'w') as f:
            json.dump(self.catalog, f, indent=2, default=str)
    
    # =========================================================================
    # Day 1: 指标发现阶段
    # =========================================================================
    
    def discover_day1(self, df_day: pd.DataFrame, date_str: str) -> Dict:
        """
        第一天：穷举变异，发现候选指标
        
        Returns:
            发现的候选指标列表
        """
        print(f"\n{'='*60}")
        print(f"Day 1 指标发现: {self.device_sn} @ {date_str}")
        print(f"{'='*60}")
        
        from core.composite_engine_v42 import CompositeIndicatorEngineV42
        
        # 1. 穷举变异
        engine = CompositeIndicatorEngineV42(df_day)
        result = engine.generate_and_evaluate()
        
        candidates = result['candidates']
        print(f"\n发现 {len(candidates)} 个候选指标:")
        
        # 2. LLM评审（模拟）
        approved = []
        for name, info in candidates.items():
            # 检查是否已存在
            exists = self._check_formula_exists(info['formula'])
            if exists:
                print(f"  ⚠️ {name}: 已存在，跳过")
                continue
            
            # 模拟LLM评审
            review = self._llm_review_indicator(name, info)
            
            if review['approved']:
                indicator_id = f"ind_{self.catalog['validation_stats']['total_discovered']:03d}"
                
                indicator = {
                    'id': indicator_id,
                    'name': review['name'],
                    'formula': info['formula'],
                    'description': review['description'],
                    'physical_meaning': review['physical_meaning'],
                    'birth_date': date_str,
                    'age_days': 0,
                    'survival_score': self.CERTIFICATION_FLOOR,  # 初始保底
                    'daily_scores': [],
                    'status': 'L1_Active',  # 直接进入观察期
                    'discovery_context': {
                        'max_value': info.get('max_value', 0),
                        'mean_value': info.get('mean_value', 0),
                        'feature': info.get('feature', '')
                    }
                }
                
                self.catalog['indicators']['L1_Active'][indicator_id] = indicator
                self.catalog['validation_stats']['total_discovered'] += 1
                approved.append(indicator)
                
                print(f"  ✅ {indicator_id}: {indicator['name']}")
                print(f"     公式: {indicator['formula']}")
                print(f"     意义: {indicator['physical_meaning'][:50]}...")
        
        # 3. 记录发现历史
        self.catalog['discovery_history'].append({
            'date': date_str,
            'candidates_found': len(candidates),
            'approved': len(approved),
            'indicators': [ind['id'] for ind in approved]
        })
        
        self._save_catalog()
        
        print(f"\nDay 1 完成: 注册 {len(approved)} 个新指标")
        return {
            'date': date_str,
            'candidates_found': len(candidates),
            'approved': len(approved),
            'indicators': approved
        }
    
    def _check_formula_exists(self, formula: str, threshold: float = None) -> bool:
        """检查公式是否已存在（V4.3: 允许不同阈值）"""
        for level in ['L1_Active', 'L2_Core', 'L3_Synthesized', 'L4_Retired']:
            for ind in self.catalog['indicators'][level].values():
                # 公式相同且阈值相近（<0.5倍）视为重复
                if ind['formula'] == formula:
                    if threshold is None:
                        return True
                    existing_threshold = ind.get('discovery_context', {}).get('threshold_used', 0)
                    if abs(existing_threshold - threshold) / max(threshold, 0.001) < 0.5:
                        return True
        return False
    
    def _llm_review_indicator(self, name: str, info: Dict) -> Dict:
        """模拟LLM评审指标"""
        # 这里应该调用真实LLM，现在模拟
        
        # 根据公式特征生成评审结果
        formula = info['formula']
        
        if 'diff_pct' in name or 'abs(' in formula:
            return {
                'approved': True,
                'name': '组串电流离散率',
                'description': '衡量PV组串间电流差异程度',
                'physical_meaning': '反映组串遮挡、老化或不匹配问题'
            }
        elif 'unbalance' in name or 'max(' in formula:
            return {
                'approved': True,
                'name': '电网电压不平衡度',
                'description': '衡量三相电压不平衡程度',
                'physical_meaning': '反映电网质量和设备负载均衡性'
            }
        elif 'efficiency' in name or '1.0 -' in formula:
            return {
                'approved': True,
                'name': '逆变器效率损耗',
                'description': '衡量能量转换损耗',
                'physical_meaning': '反映逆变器老化和温降影响'
            }
        else:
            return {
                'approved': True,
                'name': f'复合指标_{name}',
                'description': '自动发现的复合指标',
                'physical_meaning': '待进一步分析'
            }
    
    # =========================================================================
    # Day 2+: 持续验证阶段
    # =========================================================================
    
    def validate_daily(self, df_day: pd.DataFrame, date_str: str) -> Dict:
        """
        每日验证所有活跃指标 + 发现新指标
        
        V4.3核心：每日"验证+发现"闭环
        """
        print(f"\n{'='*60}")
        print(f"每日验证与发现: {self.device_sn} @ {date_str}")
        print(f"{'='*60}")
        
        result = {
            'date': date_str,
            'indicators_evaluated': 0,
            'new_discovered': 0,
            'new_registered': 0,
            'scores_updated': 0,
            'promoted': [],
            'retired': [],
            'alerts': []
        }
        
        # ========== 阶段1: 发现新指标（V4.3） ==========
        print("\n【阶段1】发现新指标...")
        from core.composite_engine_v43 import CompositeIndicatorEngineV43
        
        # 计算day_index
        day_index = 0
        if hasattr(self, 'current_day_index'):
            day_index = self.current_day_index
        
        engine = CompositeIndicatorEngineV43(df_day, day_index)
        candidates = discovery_result['candidates']
        
        print(f"  发现 {len(candidates)} 个候选突变")
        
        new_indicators = []
        for name, info in candidates.items():
            # 检查是否已存在（传入阈值）
            threshold = info.get('threshold_used', 0.01)
            if self._check_formula_exists(info['formula'], threshold):
                print(f"  ⚠️ {name}: 已存在（阈值相近），跳过")
                continue
            
            # LLM评审
            review = self._llm_review_indicator(name, info)
            
            if review['approved']:
                indicator_id = f"ind_{self.catalog['validation_stats']['total_discovered']:03d}"
                
                indicator = {
                    'id': indicator_id,
                    'name': review['name'],
                    'formula': info['formula'],
                    'description': review['description'],
                    'physical_meaning': review['physical_meaning'],
                    'birth_date': date_str,
                    'age_days': 0,
                    'survival_score': self.CERTIFICATION_FLOOR,
                    'daily_scores': [],
                    'status': 'L1_Active',
                    'discovery_context': {
                        'max_value': info.get('max_value', 0),
                        'mean_value': info.get('mean_value', 0),
                        'feature': info.get('feature', ''),
                        'threshold_used': threshold  # V4.3: 记录阈值
                    }
                }
                
                self.catalog['indicators']['L1_Active'][indicator_id] = indicator
                self.catalog['validation_stats']['total_discovered'] += 1
                new_indicators.append(indicator)
                result['new_registered'] += 1
                
                print(f"  ✅ {indicator_id}: {indicator['name']}")
        
        result['new_discovered'] = len(candidates)
        
        # ========== 阶段2: 验证所有L1指标（新老一起） ==========
        print(f"\n【阶段2】验证所有L1指标（共{len(self.catalog['indicators']['L1_Active'])}个）...")
        
        for ind_id, indicator in list(self.catalog['indicators']['L1_Active'].items()):
            eval_result = self._evaluate_indicator(indicator, df_day, date_str)
            result['indicators_evaluated'] += 1
            
            # 更新生命值
            old_score = indicator['survival_score']
            new_score = self._update_survival_score(indicator, eval_result)
            indicator['survival_score'] = new_score
            indicator['age_days'] += 1
            
            # 记录每日评分
            indicator['daily_scores'].append({
                'date': date_str,
                'score': new_score,
                'evaluation': eval_result
            })
            
            result['scores_updated'] += 1
            
            # 检查晋升
            if new_score >= self.PROMOTION_THRESHOLD and indicator['age_days'] >= 14:
                self._promote_indicator(ind_id, indicator)
                result['promoted'].append(ind_id)
                print(f"    🎉 {ind_id} 晋升L2!")
            
            # 检查淘汰
            elif new_score < self.RETIREMENT_THRESHOLD:
                self._retire_indicator(ind_id, indicator)
                result['retired'].append(ind_id)
                print(f"    💀 {ind_id} 淘汰!")
        
        # ========== 阶段3: 记录发现历史 ==========
        self.catalog['discovery_history'].append({
            'date': date_str,
            'candidates_found': len(candidates),
            'new_registered': len(new_indicators),
            'total_active': len(self.catalog['indicators']['L1_Active']),
            'indicators': [ind['id'] for ind in new_indicators]
        })
        
        # ========== 阶段4: 保存 ==========
        self._save_validation_log(date_str, result)
        self._save_catalog()
        
        print(f"\n{'='*60}")
        print(f"每日验证与发现完成:")
        print(f"  候选发现: {result['new_discovered']}个")
        print(f"  新注册: {result['new_registered']}个")
        print(f"  评估指标: {result['indicators_evaluated']}个")
        print(f"  更新分数: {result['scores_updated']}个")
        print(f"  晋升: {len(result['promoted'])}个")
        print(f"  淘汰: {len(result['retired'])}个")
        print(f"{'='*60}")
        
        return result
        self._save_catalog()
        
        print(f"\n验证完成:")
        print(f"  评估: {result['indicators_evaluated']}个指标")
        print(f"  更新: {result['scores_updated']}个分数")
        print(f"  晋升: {len(result['promoted'])}个")
        print(f"  淘汰: {len(result['retired'])}个")
        
        return result
    
    def _evaluate_indicator(self, indicator: Dict, df_day: pd.DataFrame, date_str: str) -> Dict:
        """评估单个指标当日表现"""
        try:
            # 计算公式值
            values = df_day.eval(indicator['formula'])
            values = values.replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(values) == 0:
                return {'valid': False, 'reason': 'no_data'}
            
            max_val = float(values.max())
            mean_val = float(values.mean())
            std_val = float(values.std())
            
            # 判断波动有效性
            # V4.3评分逻辑：有效波动+0.2，休眠-0.01（减半）
            fluctuation = std_val / mean_val if mean_val > 0 else 0
            
            if max_val > 0.05:  # 5%显著波动
                effectiveness = 'effective'
            elif fluctuation > 0.1:  # 10%相对波动
                effectiveness = 'moderate'
            else:
                effectiveness = 'dormant'
            
            return {
                'valid': True,
                'max_value': max_val,
                'mean_value': mean_val,
                'std_value': std_val,
                'fluctuation': fluctuation,
                'effectiveness': effectiveness
            }
            
        except Exception as e:
            return {'valid': False, 'reason': str(e)[:50]}
    
    def _update_survival_score(self, indicator: Dict, eval_result: Dict) -> float:
        """更新生命值（V4.1算法）"""
        old_score = indicator['survival_score']
        
        if not eval_result.get('valid'):
            # 无效数据，轻微惩罚
            delta = -0.02
        elif eval_result['effectiveness'] == 'effective':
            # 有效波动，奖励
            delta = 0.2
        elif eval_result['effectiveness'] == 'moderate':
            # 中等波动，小奖励
            delta = 0.05
        else:
            # V4.3: 休眠惩罚减半（-0.02 → -0.01）
            delta = -0.01
        
        new_score = old_score + delta
        
        # LLM认证保底
        if indicator.get('llm_certified') and new_score < self.CERTIFICATION_FLOOR:
            new_score = self.CERTIFICATION_FLOOR
        
        # 边界限制
        return max(0.0, min(1.0, new_score))
    
    def _promote_indicator(self, ind_id: str, indicator: Dict):
        """晋升指标到L2"""
        indicator['status'] = 'L2_Core'
        indicator['promotion_date'] = datetime.now().isoformat()
        
        # 移动目录
        del self.catalog['indicators']['L1_Active'][ind_id]
        self.catalog['indicators']['L2_Core'][ind_id] = indicator
        
        self.catalog['validation_stats']['total_promoted'] += 1
    
    def _retire_indicator(self, ind_id: str, indicator: Dict):
        """淘汰指标（V4.3: 7天保护期）"""
        # 保护期内不淘汰
        if indicator['age_days'] < 7:
            print(f"    🛡️ {ind_id}: 保护期内，暂不淘汰")
            return
        
        indicator['status'] = 'L4_Retired'
        indicator['retirement_date'] = datetime.now().isoformat()
        indicator['final_score'] = indicator['survival_score']
        
        # 移动目录
        del self.catalog['indicators']['L1_Active'][ind_id]
        self.catalog['indicators']['L4_Retired'][ind_id] = indicator
        
        self.catalog['validation_stats']['total_retired'] += 1
    
    def _save_validation_log(self, date_str: str, result: Dict):
        """保存验证日志"""
        log_file = f"{self.validation_log_path}/{date_str}.json"
        with open(log_file, 'w') as f:
            json.dump(result, f, indent=2)
    
    # =========================================================================
    # 查询与报告
    # =========================================================================
    
    def get_indicator_lifecycle_report(self) -> str:
        """生成指标生命周期报告"""
        lines = []
        lines.append(f"\n{'='*60}")
        lines.append(f"指标生命周期报告: {self.device_sn}")
        lines.append(f"{'='*60}")
        
        stats = self.catalog['validation_stats']
        lines.append(f"\n累计统计:")
        lines.append(f"  发现: {stats['total_discovered']}个")
        lines.append(f"  晋升: {stats['total_promoted']}个")
        lines.append(f"  淘汰: {stats['total_retired']}个")
        lines.append(f"  存活: {stats['total_discovered'] - stats['total_retired']}个")
        
        lines.append(f"\n当前状态:")
        for level in ['L1_Active', 'L2_Core', 'L3_Synthesized', 'L4_Retired']:
            count = len(self.catalog['indicators'][level])
            lines.append(f"  {level}: {count}个")
        
        lines.append(f"\nL1活跃指标详情:")
        for ind_id, ind in self.catalog['indicators']['L1_Active'].items():
            lines.append(f"  {ind_id}: {ind['name']}")
            lines.append(f"    年龄: {ind['age_days']}天")
            lines.append(f"    生命值: {ind['survival_score']:.2f}")
        
        lines.append(f"\n{'='*60}")
        return "\n".join(lines)


# 便捷函数
def run_discovery_validation_loop(device_sn: str, df_day: pd.DataFrame, date_str: str, 
                                  is_day1: bool = False) -> Dict:
    """
    运行发现-验证闭环（便捷函数）
    
    Args:
        device_sn: 设备SN
        df_day: 当日数据
        date_str: 日期
        is_day1: 是否为第一天（发现模式）
    
    Returns:
        执行结果
    """
    engine = IndicatorDiscoveryValidationEngine(device_sn)
    
    if is_day1:
        return engine.discover_day1(df_day, date_str)
    else:
        return engine.validate_daily(df_day, date_str)
