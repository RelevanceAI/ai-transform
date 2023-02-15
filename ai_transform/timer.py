import time

TIMER_START = 0
TIMER_END = 0


class Timer:
    def start(self):
        global TIMER_START
        TIMER_START = time.time()

    def stop(self):
        global TIMER_END
        TIMER_END = time.time()
        return self.get()

    def get(self):
        global TIMER_START
        global TIMER_END
        assert (
            TIMER_START > 0 and TIMER_END > 0
        ), "Please make sure you have called `ai_transform.timer_start()` and `ai_transform.timer_end()`"
        return TIMER_END - TIMER_START

    def reset(self):
        global TIMER_START
        global TIMER_END
        TIMER_START = 0
        TIMER_END = 0
