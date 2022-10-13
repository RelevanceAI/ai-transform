import logging
from typing import Any

from workflows_core.engine.abstract_engine import AbstractEngine

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)

class StableEngine(AbstractEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = kwargs.pop("show_progress_bar", True)

    def apply(self) -> Any:

        iterator = self.iterate()

        for chunk in tqdm(
            iterator,
            desc=repr(self.operator),
            disable=(not self._show_progress_bar),
            total=self.num_chunks,
        ):
            new_batch = self.operator(chunk)
            result = self.update_chunk(new_batch)
            logging.debug(result)

        return
