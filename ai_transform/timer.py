import os
import time


class Timer:
    def start(self) -> None:
        if os.getenv("_WORKFLOW_START_TIME") == 0:
            os.environ["_WORKFLOW_START_TIME"] = time.time()

    def stop(self) -> float:
        if os.getenv("_WORKFLOW_FINISH_TIME") == 0:
            os.environ["_WORKFLOW_FINISH_TIME"] = time.time()
            _WORKFLOW_START_TIME = os.getenv("_WORKFLOW_START_TIME")
            _WORKFLOW_FINISH_TIME = os.getenv("_WORKFLOW_FINISH_TIME")
            return _WORKFLOW_FINISH_TIME - _WORKFLOW_START_TIME
