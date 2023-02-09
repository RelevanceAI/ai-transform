import json
from typing import Dict, Any


def format_logging_info(info: Dict[str, Any]):
    return json.dumps(info, indent=4, sort_keys=True)
