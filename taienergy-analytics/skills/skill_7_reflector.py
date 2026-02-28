"""
Skill 7: 反思与验证器（第二阶段核心组件）

职责：
1. 每日对假设进行验证
2. 计算认知增量
3. 生成反思 Prompt
4. 触发 LLM 进行深度反思

运行频率：每日一次
"""
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

from core.hypothesis_registry import HypothesisRegistry


class Reflector:
    """
    反思与验证器
    
    工作流程：
    1. 加载所有 testing 状态的假设
    2. 对每个假设，用今日数据进行验证
    3. 更新假设权重和状态
    4. 记录认知增量
    5. 对失败的假设生成反思 Prompt
    """
    
    def __init__(self, device_sn: str, memory_dir: str = "memory"):
        self.device_sn = device_sn
        self.registry = HypothesisRegistry(device_sn, memory_dir)
    
    def daily_reflection(
        self,
        date_str: str,
        data_dict: Dict[str, pd.DataFrame],
        llm_client=None
    ) -> Dict:
        """
        每日反思流程
        
        Args:
            date_str: 日期字符串
            data_dict: 当日数据 {indicator_code: df}
            llm_client: LLM 客户端（用于生成反思）
        
        Returns:
            反思结果摘要
        """
        print(f"\n[Skill 7] 开始每日反思: {date_str}")
        
        # 1. 获取所有 testing 状态的假设
        testing_hyps = self.registry.get_testing_hypotheses()
        
        if not testing_hyps:
            print("  没有待验证的假设")
            return {"verified": 0, "failed": 0, "new_insights": []}
        
        print(f"  待验证假设: {len(testing_hyps)} 个")
        
        # 2. 验证每个假设
        verified_count = 0
        failed_count = 0
        reflections = []
        
        for hyp in testing_hyps:
            result = self._verify_single_hypothesis(hyp, data_dict)
            
            if result.get("is_success"):
                verified_count += 1
            elif result.get("status") == "failed":
                failed_count += 1
                # 生成反思
                if llm_client:
                    reflection = self._generate_reflection(hyp, result, llm_client)
                    reflections.append(reflection)
        
        # 3. 记录认知增量
        new_insights = self._extract_new_insights(data_dict, testing_hyps)
        cognitive_gain = self.registry.record_cognitive_gain(
            date_str=date_str,
            new_insights=new_insights,
            source="daily_reflection"
        )
        
        # 4. 输出摘要
        print(f"\n[Skill 7] 反思完成:")
        print(f"  验证成功: {verified_count} 个")
        print(f"  验证失败: {failed_count} 个")
        print(f"  认知增量: {cognitive_gain}")
        
        return {
            "date": date_str,
            "verified": verified_count,
            "failed": failed_count,
            "cognitive_gain": cognitive_gain,
            "new_insights": new_insights,
            "reflections": reflections
        }
    
    def _verify_single_hypothesis(
        self,
        hyp: Dict,
        data_dict: Dict[str, pd.DataFrame]
    ) -> Dict:
        """
        验证单个假设
        
        解析假设逻辑，用实际数据验证
        示例假设："ai45 * 0.98 ≈ ai56"
        """
        hyp_id = hyp["id"]
        logic = hyp["logic"]
        related_indicators = hyp.get("related_indicators", [])
        
        # 检查相关指标数据是否齐全
        missing_indicators = [
            ind for ind in related_indicators 
            if ind not in data_dict or data_dict[ind].empty
        ]
        
        if missing_indicators:
            return {
                "hyp_id": hyp_id,
                "error": f"缺少指标数据: {missing_indicators}",
                "is_success": False
            }
        
        # 解析假设逻辑并验证
        # 这里实现几种常见的假设模式
        try:
            actual_value, expected_value, deviation = self._parse_and_verify(
                logic, data_dict
            )
            
            # 记录验证结果
            result = self.registry.verify_hypothesis(
                hyp_id=hyp_id,
                actual_value=actual_value,
                expected_value=expected_value,
                deviation=deviation,
                test_data={
                    "logic": logic,
                    "related_indicators": related_indicators
                }
            )
            
            status_icon = "✓" if result["is_success"] else "✗"
            print(f"    {status_icon} {hyp_id}: {logic[:40]}... (偏差 {deviation:.2%}, 权重 {result['weight']:.2f})")
            
            return result
            
        except Exception as e:
            print(f"    ! {hyp_id}: 验证失败 - {e}")
            return {
                "hyp_id": hyp_id,
                "error": str(e),
                "is_success": False
            }
    
    def _parse_and_verify(
        self,
        logic: str,
        data_dict: Dict[str, pd.DataFrame]
    ) -> tuple:
        """
        解析假设逻辑并验证
        
        支持的逻辑模式：
        1. "A * factor ≈ B" - A 乘以系数约等于 B
        2. "A ≈ B * factor" - A 约等于 B 乘以系数
        3. "A > threshold" - A 大于阈值
        4. "A < threshold" - A 小于阈值
        5. "A correlates with B" - A 与 B 相关
        
        Returns:
            (actual_value, expected_value, deviation)
        """
        logic = logic.lower().strip()
        
        # 模式 1: "ai45 * 0.98 ≈ ai56"
        if "*" in logic and "≈" in logic:
            return self._verify_ratio_pattern(logic, data_dict)
        
        # 模式 2: "ai56 > 100" (阈值判断)
        elif ">" in logic or "<" in logic:
            return self._verify_threshold_pattern(logic, data_dict)
        
        # 模式 3: "ai45 correlates with ai56"
        elif "correlates" in logic or "相关" in logic:
            return self._verify_correlation_pattern(logic, data_dict)
        
        else:
            # 默认：计算相关指标的均值差异
            return self._verify_default(logic, data_dict)
    
    def _verify_ratio_pattern(
        self,
        logic: str,
        data_dict: Dict[str, pd.DataFrame]
    ) -> tuple:
        """验证比例模式: 'A * factor ≈ B'"""
        # 简单解析：提取指标代码和系数
        import re
        
        # 提取 aiXX 或 diXX
        indicators = re.findall(r'[ad]i\d+', logic)
        # 提取数字（系数或阈值）
        numbers = re.findall(r'\d+\.?\d*', logic)
        
        if len(indicators) >= 2 and len(numbers) >= 1:
            ind_a = indicators[0]
            ind_b = indicators[1]
            factor = float(numbers[0])
            
            # 获取数据
            df_a = data_dict.get(ind_a)
            df_b = data_dict.get(ind_b)
            
            if df_a is None or df_b is None:
                raise ValueError(f"缺少指标数据: {ind_a} 或 {ind_b}")
            
            # 计算日均值
            mean_a = df_a['value'].mean()
            mean_b = df_b['value'].mean()
            
            # 验证：A * factor 应该 ≈ B
            expected_b = mean_a * factor
            actual_b = mean_b
            
            # 计算偏差
            if expected_b != 0:
                deviation = abs(actual_b - expected_b) / abs(expected_b)
            else:
                deviation = 1.0 if actual_b != 0 else 0.0
            
            return actual_b, expected_b, deviation
        
        raise ValueError(f"无法解析比例模式: {logic}")
    
    def _verify_threshold_pattern(
        self,
        logic: str,
        data_dict: Dict[str, pd.DataFrame]
    ) -> tuple:
        """验证阈值模式: 'A > threshold' 或 'A < threshold'"""
        import re
        
        indicators = re.findall(r'[ad]i\d+', logic)
        numbers = re.findall(r'\d+\.?\d*', logic)
        
        if len(indicators) >= 1 and len(numbers) >= 1:
            ind = indicators[0]
            threshold = float(numbers[0])
            
            df = data_dict.get(ind)
            if df is None:
                raise ValueError(f"缺少指标数据: {ind}")
            
            # 计算日均值
            mean_val = df['value'].mean()
            
            # 验证
            if ">" in logic:
                is_pass = mean_val > threshold
            else:
                is_pass = mean_val < threshold
            
            # 返回二元结果
            actual = 1.0 if is_pass else 0.0
            expected = 1.0
            deviation = 0.0 if is_pass else 1.0
            
            return actual, expected, deviation
        
        raise ValueError(f"无法解析阈值模式: {logic}")
    
    def _verify_correlation_pattern(
        self,
        logic: str,
        data_dict: Dict[str, pd.DataFrame]
    ) -> tuple:
        """验证相关性模式: 'A correlates with B'"""
        import re
        
        indicators = re.findall(r'[ad]i\d+', logic)
        
        if len(indicators) >= 2:
            ind_a = indicators[0]
            ind_b = indicators[1]
            
            df_a = data_dict.get(ind_a)
            df_b = data_dict.get(ind_b)
            
            if df_a is None or df_b is None:
                raise ValueError(f"缺少指标数据: {ind_a} 或 {ind_b}")
            
            # 合并数据
            merged = pd.merge(
                df_a[['timestamp', 'value']],
                df_b[['timestamp', 'value']],
                on='timestamp',
                suffixes=('_a', '_b')
            )
            
            if len(merged) < 5:
                raise ValueError("数据点不足，无法计算相关性")
            
            # 计算相关系数
            correlation = merged['value_a'].corr(merged['value_b'])
            
            # 相关性强度的绝对值
            actual = abs(correlation) if not pd.isna(correlation) else 0.0
            expected = 0.7  # 期望相关性 > 0.7
            deviation = max(0, expected - actual)
            
            return actual, expected, deviation
        
        raise ValueError(f"无法解析相关性模式: {logic}")
    
    def _verify_default(
        self,
        logic: str,
        data_dict: Dict[str, pd.DataFrame]
    ) -> tuple:
        """默认验证：计算相关指标的均值差异"""
        import re
        
        indicators = re.findall(r'[ad]i\d+', logic)
        
        if len(indicators) >= 2:
            ind_a = indicators[0]
            ind_b = indicators[1]
            
            df_a = data_dict.get(ind_a)
            df_b = data_dict.get(ind_b)
            
            if df_a is not None and df_b is not None:
                mean_a = df_a['value'].mean()
                mean_b = df_b['value'].mean()
                
                # 计算相对差异
                if mean_a != 0:
                    deviation = abs(mean_a - mean_b) / abs(mean_a)
                else:
                    deviation = 0.0 if mean_b == 0 else 1.0
                
                return mean_b, mean_a, deviation
        
        # 如果无法解析，返回默认值
        return 0.0, 0.0, 1.0
    
    def _generate_reflection(
        self,
        hyp: Dict,
        result: Dict,
        llm_client
    ) -> str:
        """
        为失败的假设生成反思
        
        Prompt 设计：
        - 输入：假设逻辑、验证历史、失败原因
        - 输出：反思文本（为什么失败、如何修正）
        """
        hyp_id = hyp["id"]
        logic = hyp["logic"]
        test_history = hyp.get("test_history", [])
        
        # 构建反思 Prompt
        prompt = f"""# 假设验证失败反思

## 假设信息
- ID: {hyp_id}
- 逻辑: {logic}
- 当前状态: {result.get('status', 'unknown')}
- 成功次数: {hyp.get('success_count', 0)}
- 失败次数: {hyp.get('fail_count', 0)}

## 最近验证记录
"""
        # 添加最近 3 次验证记录
        for i, record in enumerate(test_history[-3:]):
            prompt += f"""
{i+1}. 时间: {record.get('timestamp', 'N/A')}
   实际值: {record.get('actual', 'N/A')}
   预期值: {record.get('expected', 'N/A')}
   偏差: {record.get('deviation', 'N/A'):.2%}
   结果: {'成功' if record.get('is_success') else '失败'}
"""
        
        prompt += """
## 你的任务
请分析这个假设为什么验证失败，给出反思：
1. 可能的原因是什么？（如：系数需要调整、条件需要细化、假设本身不成立）
2. 如何修正这个假设？（给出具体建议）
3. 是否需要放弃这个假设，转而寻找新的规律？

请用中文回答，简明扼要。
"""
        
        # 调用 LLM
        try:
            reflection = llm_client.generate(prompt)
            
            # 保存反思
            self.registry.add_reflection(hyp_id, reflection)
            
            print(f"    [反思] {hyp_id}: 已生成反思")
            return reflection
            
        except Exception as e:
            print(f"    [反思] {hyp_id}: 生成反思失败 - {e}")
            return ""
    
    def _extract_new_insights(
        self,
        data_dict: Dict[str, pd.DataFrame],
        hypotheses: List[Dict]
    ) -> List[str]:
        """
        从数据中提取新洞察
        
        简单实现：检查是否有新的相关性或异常模式
        后续可以引入更复杂的模式识别
        """
        insights = []
        
        # 示例：检查是否有指标呈现强相关性（但不在假设中）
        # 这里可以扩展为自动发现新假设的逻辑
        
        return insights
    
    def generate_hypotheses_from_llm(
        self,
        data_dict: Dict[str, pd.DataFrame],
        llm_client,
        max_hypotheses: int = 3
    ) -> List[str]:
        """
        让 LLM 基于数据生成新假设
        
        用于自动发现新规律
        """
        # 准备数据摘要
        data_summary = []
        for code, df in list(data_dict.items())[:10]:  # 取前 10 个指标
            if not df.empty:
                mean_val = df['value'].mean()
                std_val = df['value'].std()
                data_summary.append(f"{code}: 均值={mean_val:.2f}, 标准差={std_val:.2f}")
        
        prompt = f"""# 光伏设备数据规律发现

## 今日数据摘要
{chr(10).join(data_summary)}

## 你的任务
基于以上数据，提出 {max_hypotheses} 个关于这些指标之间关系的假设。

假设格式示例：
- "ai45 * 0.98 ≈ ai56" (输入功率乘以转换效率约等于输出功率)
- "ai61 > 45 时 ai60 < 0.96" (温度高于45度时效率低于96%)
- "ai45 correlates with ai56" (输入功率与输出功率相关)

请只输出假设，每行一个，不要解释。
"""
        
        try:
            response = llm_client.generate(prompt)
            
            # 解析假设
            new_hypotheses = []
            for line in response.strip().split('\n'):
                line = line.strip()
                if line and not line.startswith('#') and not line.startswith('-'):
                    line = line.lstrip('- ').strip()
                
                if line and len(line) > 10:  # 过滤太短的行
                    # 提取相关指标
                    import re
                    indicators = re.findall(r'[ad]i\d+', line)
                    
                    if len(indicators) >= 2:
                        hyp_id = self.registry.create_hypothesis(
                            logic=line,
                            related_indicators=indicators,
                            source="llm_generation"
                        )
                        new_hypotheses.append(hyp_id)
            
            print(f"  [假设生成] 生成 {len(new_hypotheses)} 个新假设")
            return new_hypotheses
            
        except Exception as e:
            print(f"  [假设生成] 失败: {e}")
            return []
