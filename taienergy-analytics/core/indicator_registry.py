"""V5.1 指标注册表读写"""
import json
from pathlib import Path
from typing import Dict


REGISTRY_PATH = Path("config/indicators/registry.json")


def read_registry() -> Dict:
    """读取 registry，带强校验"""
    if not REGISTRY_PATH.exists():
        return {"version": "v5.1", "updated_at": None, "indicators": {}}
    
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    
    # 强校验：失败时抛出带字段名的明确错误
    from .registry_validator import validate_registry_strict
    try:
        validate_registry_strict(registry)
    except Exception as e:
        raise RuntimeError(f"Registry 校验失败: {e}")
    
    return registry


def write_registry(registry: Dict) -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
