import time

START_TIME = 0
FINISH_TIME = 0


class Timer:
    def start(self) -> None:
        global START_TIME
        if START_TIME == 0:
            START_TIME = time.time()

    def stop(self) -> float:
        global FINISH_TIME
        if FINISH_TIME == 0:
            FINISH_TIME = time.time()
            return FINISH_TIME - START_TIME
