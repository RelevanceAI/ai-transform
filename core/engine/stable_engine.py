from typing import Any

from core.engine import AbstractEngine


class StableEngine(AbstractEngine):
    def apply(self) -> Any:

        iterator = self.iterate()
        for chunk in iterator:
            new_batch = self.operator(chunk)
            self.update_chunk(new_batch)

        return
