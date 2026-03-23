"""Config loader — merges all YAML configs and substitutes environment variables."""

import os
from typing import Any

import yaml


def load_config(config_dir: str = "configs") -> dict[str, Any]:
    """Load and merge all YAML configs, substituting environment variables.

    Args:
        config_dir: Path to the directory containing YAML config files.

    Returns:
        Merged config dictionary with environment variable substitutions applied.
    """
    merged: dict[str, Any] = {}
    for filename in ["pipeline_config.yaml", "column_mappings.yaml", "business_rules.yaml"]:
        filepath = os.path.join(config_dir, filename)
        if os.path.exists(filepath):
            with open(filepath) as f:
                content = f.read()
            for key, value in os.environ.items():
                content = content.replace(f"${{{key}}}", value)
            merged.update(yaml.safe_load(content) or {})
    return merged
