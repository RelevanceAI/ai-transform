import pprint
import logging
import datetime
from icecream import ic
from typing import Dict, Any, List
from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList


def format_logging_info(info: Dict[str, Any], indent=4, width=80, depth=None, compact=True, sort_dicts=False):
    return "\n" + pprint.pformat(info, indent=indent, width=width, depth=depth, compact=compact, sort_dicts=sort_dicts)


class Logger:
    def __init__(self):
        self._logger = logging.getLogger("WORKFLOW")
        logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        self._logger.setLevel(logging.DEBUG)

    def __call__(self, info: Any, no_vectors: bool = True) -> None:
        if isinstance(info, Document):
            info_to_log = info.to_json()
            if no_vectors:
                info_to_log = {k: v for k, v in info.items() if "_vector_" not in k}

        elif isinstance(info, DocumentList):
            info_to_log = [document.to_json() for document in info]
        elif isinstance(info, List):
            if isinstance(info[0], Document):
                info_to_log = [document.to_json() for document in info]

        else:
            info_to_log = info

        self._logger.debug(format_logging_info(info_to_log))


def time_format():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp} | "


ic.configureOutput(prefix=time_format, includeContext=True)
# Change all printing statements
