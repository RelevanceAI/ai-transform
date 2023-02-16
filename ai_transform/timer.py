import time


class Timer:
    def __init__(self) -> None:
        self._start_time = 0
        self._finish_time = 0

    @property
    def timer_value(self) -> float:
        return self._finish_time - self._start_time

    def start(self) -> None:
        self._start_time = time.time()

    def stop(self) -> float:
        self._finish_time = time.time()
        return self.get()

    def get(self) -> float:
        assert (
            self._start_time > 0 and self._finish_time > 0
        ), "Please make sure you have called `ai_transform.timer_start()` and `ai_transform.timer_end()`"
        return self.timer_value

    def reset(self) -> None:
        self._start_time = 0
        self._finish_time = 0
