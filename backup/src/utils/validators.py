"""Input validation helpers."""

from typing import Any


def require_config_key(config: dict[str, Any], *keys: str) -> None:
    """Assert that all required keys are present in config.

    Args:
        config: Config dictionary to validate.
        *keys: Dot-separated key paths (e.g. "source.table").

    Raises:
        KeyError: If any required key is missing.
    """
    for key_path in keys:
        parts = key_path.split(".")
        node: Any = config
        for part in parts:
            if not isinstance(node, dict) or part not in node:
                raise KeyError(f"Required config key missing: '{key_path}'")
            node = node[part]
