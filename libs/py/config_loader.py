import os
from typing import Any, Dict

import yaml


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_sources_config() -> Dict[str, Any]:
    path = os.path.join("config", "sources.yaml")
    return load_yaml(path)


def get_retry_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return (cfg.get("defaults", {}) or {}).get("retry", {}) or {}


def get_cache_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return (cfg.get("defaults", {}) or {}).get("cache", {}) or {}

