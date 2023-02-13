import json
import pprint

from typing import Dict, Any


def format_logging_info(
    info: Dict[str, Any],
    indent=4,
    width=80,
    depth=None,
    compact=True,
    sort_dicts=False,
):
    return "\n" + pprint.pformat(
        info,
        indent=indent,
        width=width,
        depth=depth,
        compact=compact,
        sort_dicts=sort_dicts,
    )
