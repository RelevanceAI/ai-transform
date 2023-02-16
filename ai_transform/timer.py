import os
import time


class Timer:
    def start(self) -> None:
        if "_WORKFLOW_START_TIME" not in os.environ:
            os.environ["_WORKFLOW_START_TIME"] = str(time.time())

    def stop(self) -> float:
        if "_WORKFLOW_FINISH_TIME" not in os.environ:
            os.environ["_WORKFLOW_FINISH_TIME"] = str(time.time())

            _WORKFLOW_START_TIME = float(os.getenv("_WORKFLOW_START_TIME"))
            _WORKFLOW_FINISH_TIME = float(os.getenv("_WORKFLOW_FINISH_TIME"))

            return _WORKFLOW_FINISH_TIME - _WORKFLOW_START_TIME
