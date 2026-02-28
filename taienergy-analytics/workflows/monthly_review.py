"""
V4.4 月度评审工作流

每月执行：
1. 候选指标评审（入库/废弃）
2. 临时指标转正评审
3. 指标效果评估
4. 低价值指标清理
5. 生成月度报告
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicator_lifecycle_manager import IndicatorLifecycleManager
from core.emergency_analyzer import EmergencyAnalyzer
from core.cross_device_learner import CrossDeviceLearner


class MonthlyReviewWorkflow:
    """V4.4 月度评审工作流"""
    
    DEVICE_CLUSTER = [f'XHDL_{i}NBQ' for i in range(1, 17)]
    
    def __init__(self):
        self.results_base_path = "memory/monthly"
        os.makedirs(self.results_base_path, exist_ok=True)
    
    def run_monthly_review(self, date_str: str) -> Dict:
        """
        运行月度评审
        
        Args:
            date_str: 月度日期，如 "2025-07-01"
        
        Returns:
            月度评审结果
        """
        month_key = date_str[:7]  # 2025-07
        
        print(f"\n{'='*80}")
        print(f"V4.4 月度评审: {month_key}")
        print(f"{'='*80}")
        
        result = {
            'month': month_key,
            'review_date': date_str,
            'generated_at': datetime.now().isoformat(),
            'candidate_review': {},
            'temp_indicator_review': {},
            'effectiveness_assessment': {},
            'cleanup_results': {},
            'new_catalog_version': None
        }
        
        # 阶段1: 候选指标评审
        print(f"\n【阶段1】候选指标评审")
        result['candidate_review'] = self._review_candidates(date_str)
        
        # 阶段2: 临时指标转正评审
        print(f"\n【阶段2】临时指标转正评审")
        result['temp_indicator_review'] = self._review_temp_indicators(date_str)
        
        # 阶段3: 指标效果评估
        print(f"\n【阶段3】指标效果评估")
        result['effectiveness_assessment'] = self._assess_indicator_effectiveness(month_key)
        
        # 阶段4: 清理低价值指标
        print(f"\n【阶段4】清理低价值指标")
        result['cleanup_results'] = self._cleanup_low_value_indicators(month_key)
        
        # 阶段5: 生成新指标库版本
        print(f"\n【阶段5】生成指标库版本")
        result['new_catalog_version'] = self._generate_catalog_version(month_key)
        
        # 保存月度评审报告
        self._save_monthly_report(result)
        
        # 输出摘要
        self._print_summary(result)
        
        return result
    
    def _review_candidates(self, date_str: str) -> Dict:
        """评审候选指标"""
        manager = IndicatorLifecycleManager()
        
        # 执行评审
        review_result = manager.review_candidates(date_str)
        
        return {
            'reviewed_count': review_result.get('promoted', 0) + 
                            review_result.get('rejected', 0) + 
                            review_result.get('extended', 0),
            'promoted': review_result.get('promoted', 0),
            'rejected': review_result.get('rejected', 0),
            'extended': review_result.get('extended', 0),
            'promoted_ids': review_result.get('promoted_ids', []),
            'rejected_ids': review_result.get('rejected_ids', [])
        }
    
    def _review_temp_indicators(self, date_str: str) -> Dict:
        """评审临时指标转正"""
        analyzer = EmergencyAnalyzer()
        
        # 获取所有临时指标
        temp_indicators = analyzer.get_temp_indicators()
        
        # 人工评审模拟（实际应该有人工介入）
        # 这里简化：验证次数>=3的转正，否则废弃
        approved = []
        rejected = []
        
        for ind in temp_indicators:
            # 检查验证历史（简化）
            # 实际应该检查该指标在实际使用中的效果
            verification_count = len(ind.get('verification_history', []))
            
            if verification_count >= 3:
                approved.append(ind)
            else:
                rejected.append(ind)
        
        # 清理过期的
        expired_count = analyzer.clean_expired_temp_indicators(date_str)
        
        return {
            'total_temp': len(temp_indicators),
            'approved': len(approved),
            'rejected': len(rejected),
            'expired_cleaned': expired_count,
            'approved_ids': [i['id'] for i in approved]
        }
    
    def _assess_indicator_effectiveness(self, month_key: str) -> Dict:
        """评估指标效果"""
        # 读取当月指标库
        catalog_path = f"memory/indicators/catalog_v{month_key}.json"
        
        if not os.path.exists(catalog_path):
            return {'assessed_count': 0, 'high_value': [], 'low_value': []}
        
        with open(catalog_path, 'r') as f:
            catalog = json.load(f)
        
        indicators = catalog.get('indicators', {})
        
        high_value = []
        low_value = []
        
        for ind_id, ind_info in indicators.items():
            # 简化评估：验证设备数>=3且使用>=14天为高价值
            verified_devices = len(ind_info.get('verified_devices', []))
            
            # 这里简化处理，实际需要更复杂的效果追踪
            if verified_devices >= 3:
                high_value.append(ind_id)
            elif verified_devices <= 1:
                low_value.append(ind_id)
        
        return {
            'assessed_count': len(indicators),
            'high_value': high_value,
            'low_value': low_value,
            'high_value_rate': round(len(high_value) / len(indicators) * 100, 1) if indicators else 0
        }
    
    def _cleanup_low_value_indicators(self, month_key: str) -> Dict:
        """清理低价值指标"""
        catalog_path = f"memory/indicators/catalog_v{month_key}.json"
        
        if not os.path.exists(catalog_path):
            return {'cleaned_count': 0}
        
        with open(catalog_path, 'r') as f:
            catalog = json.load(f)
        
        indicators = catalog.get('indicators', {})
        to_remove = []
        
        for ind_id, ind_info in indicators.items():
            # 低价值标准：验证设备<=1 且 入库超过90天
            verified_devices = len(ind_info.get('verified_devices', []))
            promoted_date = ind_info.get('promoted_date', '2099-12-31')
            
            days_since_promoted = (
                datetime.now() - datetime.fromisoformat(promoted_date)
            ).days
            
            if verified_devices <= 1 and days_since_promoted > 90:
                to_remove.append(ind_id)
        
        # 执行清理
        for ind_id in to_remove:
            del catalog['indicators'][ind_id]
        
        # 保存
        with open(catalog_path, 'w') as f:
            json.dump(catalog, f, indent=2)
        
        return {
            'cleaned_count': len(to_remove),
            'cleaned_ids': to_remove
        }
    
    def _generate_catalog_version(self, month_key: str) -> Dict:
        """生成指标库版本"""
        catalog_path = f"memory/indicators/catalog_v{month_key}.json"
        
        if not os.path.exists(catalog_path):
            return {'version': month_key, 'indicator_count': 0}
        
        with open(catalog_path, 'r') as f:
            catalog = json.load(f)
        
        indicators = catalog.get('indicators', {})
        
        # 统计
        by_source = {}
        for ind in indicators.values():
            source = ind.get('source', 'unknown')
            by_source[source] = by_source.get(source, 0) + 1
        
        return {
            'version': month_key,
            'indicator_count': len(indicators),
            'by_source': by_source,
            'generated_at': datetime.now().isoformat()
        }
    
    def _save_monthly_report(self, result: Dict):
        """保存月度评审报告"""
        month_key = result['month']
        report_path = f"{self.results_base_path}/{month_key}_review.json"
        
        with open(report_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        print(f"\n月度评审报告已保存: {report_path}")
    
    def _print_summary(self, result: Dict):
        """打印摘要"""
        print(f"\n{'='*80}")
        print(f"V4.4 月度评审摘要 ({result['month']})")
        print(f"{'='*80}")
        
        candidate = result['candidate_review']
        print(f"\n候选指标评审:")
        print(f"  入库: {candidate.get('promoted', 0)} 个")
        print(f"  废弃: {candidate.get('rejected', 0)} 个")
        print(f"  延期: {candidate.get('extended', 0)} 个")
        
        temp = result['temp_indicator_review']
        print(f"\n临时指标评审:")
        print(f"  转正: {temp.get('approved', 0)} 个")
        print(f"  废弃: {temp.get('rejected', 0)} 个")
        print(f"  过期清理: {temp.get('expired_cleaned', 0)} 个")
        
        effectiveness = result['effectiveness_assessment']
        print(f"\n指标效果评估:")
        print(f"  高价值: {len(effectiveness.get('high_value', []))} 个")
        print(f"  低价值: {len(effectiveness.get('low_value', []))} 个")
        
        cleanup = result['cleanup_results']
        print(f"\n清理结果: {cleanup.get('cleaned_count', 0)} 个低价值指标")
        
        catalog = result['new_catalog_version']
        if catalog:
            print(f"\n指标库版本 {catalog['version']}: {catalog['indicator_count']} 个指标")
        
        print(f"\n{'='*80}")


# 便捷函数
def run_v44_monthly_review(date_str: str) -> Dict:
    """运行V4.4月度评审"""
    workflow = MonthlyReviewWorkflow()
    return workflow.run_monthly_review(date_str)


if __name__ == '__main__':
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else '2025-07-01'
    run_v44_monthly_review(date)