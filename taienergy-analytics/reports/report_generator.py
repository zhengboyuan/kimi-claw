"""
V5.1 报表生成器（轻量模板渲染）
"""
from pathlib import Path
from typing import Dict
from collections import defaultdict


def render_template(template_path: str, context: Dict) -> str:
    template = Path(template_path).read_text(encoding="utf-8")
    safe_context = defaultdict(str, context)
    return template.format_map(safe_context)


def write_report(output_path: str, content: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
