"""V5.1 指标注册表读写"""
import json
from pathlib import Path
from typing import Dict


REGISTRY_PATH = Path("config/indicators/registry.json")


def read_registry() -> Dict:
    if not REGISTRY_PATH.exists():
        return {"version": "v5.1", "updated_at": None, "indicators": {}}
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def write_registry(registry: Dict) -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
