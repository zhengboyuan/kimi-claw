#!/usr/bin/env python3
"""
V5.1 指标审批脚本
将 pending 状态的指标审批合并到 registry
"""
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 路径配置
PENDING_DIR = Path("config/indicators/llm_generated/pending")
APPROVED_DIR = Path("config/indicators/llm_generated/approved")
RETIRED_DIR = Path("config/indicators/llm_generated/retired")
REGISTRY_PATH = Path("config/indicators/registry.json")
HISTORY_PATH = Path("memory/indicators/registry_history.json")


class IndicatorApprover:
    """指标审批管理器"""
    
    def __init__(self):
        self.pending_dir = PENDING_DIR
        self.approved_dir = APPROVED_DIR
        self.retired_dir = RETIRED_DIR
        self.registry_path = REGISTRY_PATH
        self.history_path = HISTORY_PATH
        
        # 确保目录存在
        for d in [self.pending_dir, self.approved_dir, self.retired_dir, self.history_path.parent]:
            d.mkdir(parents=True, exist_ok=True)
    
    def list_pending(self) -> List[Dict]:
        """列出所有 pending 指标"""
        pending = []
        if not self.pending_dir.exists():
            return pending
        
        for f in self.pending_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                data['_file'] = f.name
                pending.append(data)
            except Exception as e:
                print(f"  ⚠️ 读取失败 {f.name}: {e}")
        
        return pending
    
    def approve(self, indicator_id: str, reviewer: str = "system", notes: str = "") -> bool:
        """
        审批通过一个指标
        
        Args:
            indicator_id: 指标ID
            reviewer: 审批人
            notes: 审批备注
        
        Returns:
            是否成功
        """
        # 查找 pending 文件
        pending_file = self.pending_dir / f"{indicator_id}.json"
        if not pending_file.exists():
            print(f"  ❌ Pending 指标不存在: {indicator_id}")
            return False
        
        # 读取指标定义
        try:
            indicator = json.loads(pending_file.read_text())
        except Exception as e:
            print(f"  ❌ 读取指标失败: {e}")
            return False
        
        # 更新状态
        indicator['lifecycle_status'] = 'approved'
        indicator['approved_at'] = datetime.now().isoformat()
        indicator['approved_by'] = reviewer
        indicator['approval_notes'] = notes
        
        # 1. 移动到 approved 目录
        approved_file = self.approved_dir / f"{indicator_id}.json"
        approved_file.write_text(json.dumps(indicator, ensure_ascii=False, indent=2), encoding='utf-8')
        pending_file.unlink()
        print(f"  ✓ 移动到 approved: {indicator_id}")
        
        # 2. 合并到 registry
        self._merge_to_registry(indicator)
        
        # 3. 记录历史
        self._record_history('approve', indicator, reviewer, notes)
        
        return True
    
    def reject(self, indicator_id: str, reviewer: str = "system", reason: str = "") -> bool:
        """拒绝一个指标"""
        pending_file = self.pending_dir / f"{indicator_id}.json"
        if not pending_file.exists():
            print(f"  ❌ Pending 指标不存在: {indicator_id}")
            return False
        
        try:
            indicator = json.loads(pending_file.read_text())
        except Exception as e:
            print(f"  ❌ 读取指标失败: {e}")
            return False
        
        # 更新状态
        indicator['lifecycle_status'] = 'retired'
        indicator['rejected_at'] = datetime.now().isoformat()
        indicator['rejected_by'] = reviewer
        indicator['reject_reason'] = reason
        
        # 移动到 retired
        retired_file = self.retired_dir / f"{indicator_id}.json"
        retired_file.write_text(json.dumps(indicator, ensure_ascii=False, indent=2), encoding='utf-8')
        pending_file.unlink()
        print(f"  ✓ 移动到 retired: {indicator_id}")
        
        # 记录历史
        self._record_history('reject', indicator, reviewer, reason)
        
        return True
    
    def retire(self, indicator_id: str, reviewer: str = "system", reason: str = "") -> bool:
        """退役一个已批准的指标"""
        # 从 registry 中移除
        registry = self._load_registry()
        if indicator_id not in registry.get('indicators', {}):
            print(f"  ❌ Registry 中不存在: {indicator_id}")
            return False
        
        indicator = registry['indicators'].pop(indicator_id)
        indicator['lifecycle_status'] = 'retired'
        indicator['retired_at'] = datetime.now().isoformat()
        indicator['retired_by'] = reviewer
        indicator['retire_reason'] = reason
        
        # 保存 registry
        self._save_registry(registry)
        
        # 移动到 retired
        retired_file = self.retired_dir / f"{indicator_id}.json"
        retired_file.write_text(json.dumps(indicator, ensure_ascii=False, indent=2), encoding='utf-8')
        
        # 清理 approved
        approved_file = self.approved_dir / f"{indicator_id}.json"
        if approved_file.exists():
            approved_file.unlink()
        
        print(f"  ✓ 退役指标: {indicator_id}")
        
        # 记录历史
        self._record_history('retire', indicator, reviewer, reason)
        
        return True
    
    def _load_registry(self) -> Dict:
        """加载 registry"""
        if not self.registry_path.exists():
            return {"version": "v5.1", "updated_at": None, "indicators": {}}
        return json.loads(self.registry_path.read_text(encoding='utf-8'))
    
    def _save_registry(self, registry: Dict):
        """保存 registry"""
        registry['updated_at'] = datetime.now().isoformat()
        self.registry_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding='utf-8')
    
    def _merge_to_registry(self, indicator: Dict):
        """合并指标到 registry"""
        registry = self._load_registry()
        
        ind_id = indicator.get('id')
        if not ind_id:
            print(f"  ❌ 指标缺少 id")
            return
        
        # 添加/更新指标
        registry['indicators'][ind_id] = indicator
        self._save_registry(registry)
        print(f"  ✓ 合并到 registry: {ind_id}")
    
    def _record_history(self, action: str, indicator: Dict, operator: str, notes: str):
        """记录审批历史"""
        history = []
        if self.history_path.exists():
            try:
                history = json.loads(self.history_path.read_text())
            except:
                pass
        
        history.append({
            'timestamp': datetime.now().isoformat(),
            'action': action,  # approve, reject, retire
            'indicator_id': indicator.get('id'),
            'indicator_name': indicator.get('name'),
            'operator': operator,
            'notes': notes
        })
        
        self.history_path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding='utf-8')
    
    def batch_approve(self, reviewer: str = "system") -> int:
        """批量审批所有 pending 指标"""
        pending = self.list_pending()
        if not pending:
            print("  没有 pending 指标需要审批")
            return 0
        
        print(f"\n发现 {len(pending)} 个 pending 指标:")
        for p in pending:
            print(f"  - {p.get('id')}: {p.get('name')}")
        
        approved_count = 0
        for p in pending:
            ind_id = p.get('id')
            if self.approve(ind_id, reviewer, "批量审批"):
                approved_count += 1
        
        return approved_count
    
    def sync_approved_to_registry(self):
        """同步所有 approved 指标到 registry（用于修复）"""
        if not self.approved_dir.exists():
            print("  没有 approved 目录")
            return
        
        registry = self._load_registry()
        count = 0
        
        for f in self.approved_dir.glob("*.json"):
            try:
                indicator = json.loads(f.read_text())
                ind_id = indicator.get('id')
                if ind_id:
                    registry['indicators'][ind_id] = indicator
                    count += 1
            except Exception as e:
                print(f"  ⚠️ 读取失败 {f.name}: {e}")
        
        self._save_registry(registry)
        print(f"  ✓ 同步 {count} 个 approved 指标到 registry")


def main():
    """命令行入口"""
    import sys
    
    approver = IndicatorApprover()
    
    if len(sys.argv) < 2:
        print("Usage: python approve_indicators.py <command> [args...]")
        print("")
        print("Commands:")
        print("  list                          列出 pending 指标")
        print("  approve <indicator_id>       审批单个指标")
        print("  reject <indicator_id> [reason] 拒绝单个指标")
        print("  retire <indicator_id> [reason] 退役已批准指标")
        print("  batch-approve                 批量审批所有 pending")
        print("  sync                          同步 approved 到 registry")
        return
    
    cmd = sys.argv[1]
    
    if cmd == 'list':
        pending = approver.list_pending()
        if pending:
            print(f"\nPending 指标 ({len(pending)}):")
            for p in pending:
                print(f"  - {p.get('id')}: {p.get('name')} (source: {p.get('source')})")
        else:
            print("\n没有 pending 指标")
    
    elif cmd == 'approve':
        if len(sys.argv) < 3:
            print("Usage: approve <indicator_id>")
            return
        approver.approve(sys.argv[2])
    
    elif cmd == 'reject':
        if len(sys.argv) < 3:
            print("Usage: reject <indicator_id> [reason]")
            return
        reason = sys.argv[3] if len(sys.argv) > 3 else ""
        approver.reject(sys.argv[2], reason=reason)
    
    elif cmd == 'retire':
        if len(sys.argv) < 3:
            print("Usage: retire <indicator_id> [reason]")
            return
        reason = sys.argv[3] if len(sys.argv) > 3 else ""
        approver.retire(sys.argv[2], reason=reason)
    
    elif cmd == 'batch-approve':
        count = approver.batch_approve()
        print(f"\n✅ 审批完成: {count} 个指标")
    
    elif cmd == 'sync':
        approver.sync_approved_to_registry()
    
    else:
        print(f"Unknown command: {cmd}")


if __name__ == '__main__':
    main()