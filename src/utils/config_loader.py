from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def load_config(config_dir: str = "configs") -> dict[str, Any]:
    """Load and merge YAML config files with environment variable substitution.

    Args:
        config_dir: Directory containing YAML config files.

    Returns:
        Merged configuration dictionary.
    """
    merged: dict[str, Any] = {}
    config_path = Path(config_dir)

    for filename in ("pipeline_config.yaml", "column_mappings.yaml", "business_rules.yaml"):
        filepath = config_path / filename
        if filepath.exists():
            with filepath.open(encoding="utf-8") as file:
                content = file.read()

            for key, value in os.environ.items():
                content = content.replace(f"${{{key}}}", value)

            merged.update(yaml.safe_load(content) or {})

    return merged