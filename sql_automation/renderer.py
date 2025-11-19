from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateError


class SqlRenderer:
    def __init__(self, templates_root: Path) -> None:
        self.templates_root = templates_root
        loader = FileSystemLoader(str(templates_root))
        self.env = Environment(
            loader=loader,
            undefined=StrictUndefined,  # unknown vars -> error
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render(self, template_path: str, parameters: Dict[str, Any]) -> str:
        try:
            template = self.env.get_template(template_path)
        except TemplateError as e:
            raise RuntimeError(f"failed to load SQL template '{template_path}': {e}") from e
        try:
            sql = template.render(**parameters)
        except TemplateError as e:
            raise RuntimeError(f"failed to render SQL template '{template_path}': {e}") from e
        return sql.strip()
