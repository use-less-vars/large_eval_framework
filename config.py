import json
from pathlib import Path

def load_strategy_params(config_path: str) -> dict:
    """Load strategy params"""
    with open(Path(config_path)) as f:
        all_params = json.load(f)
        return all_params

