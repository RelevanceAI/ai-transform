import logging
import traceback
from typing import Any

from workflows_core.engine.abstract_engine import AbstractEngine

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


class StableEngine(AbstractEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = kwargs.pop("show_progress_bar", True)

    def apply(self) -> float:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """

        iterator = self.iterate()
        successful_chunks = 0

        for chunk in tqdm(
            iterator,
            desc=repr(self.operator),
            disable=(not self._show_progress_bar),
            total=self.num_chunks,
        ):
            try:
                new_batch = self.operator(chunk)
            except Exception as e:
                logger.error(chunk)
                logger.error(traceback.format_exc())
            else:
                result = self.update_chunk(new_batch)
                successful_chunks += 1
                logger.debug(result)

        return successful_chunks / self.num_chunks
