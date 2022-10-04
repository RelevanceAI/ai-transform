from typing import Any

from core.engine.abstract_engine import AbstractEngine

from tqdm.auto import tqdm


class StableEngine(AbstractEngine):
    def __init__(self, show_progress_bar: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = show_progress_bar

    def apply(self) -> Any:

        iterator = self.iterate()

        for chunk in tqdm(
            iterator, disable=(not self._show_progress_bar), total=len(iterator)
        ):
            new_batch = self.operator(chunk)
            self.update_chunk(new_batch)

        return
