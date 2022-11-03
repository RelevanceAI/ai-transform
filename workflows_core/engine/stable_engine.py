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

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """

        iterator = self.iterate()
        successful_chunks = 0
        error_logs = []

        for chunk_counter, chunk in enumerate(tqdm(
            iterator,
            desc=repr(self.operator),
            disable=(not self._show_progress_bar),
            total=self.num_chunks,
        )):
            try:
                new_batch = self.operator(chunk)
            except Exception as e:
                chunk_error_log = {
                    "exception": str(e),
                    "traceback": traceback.format_exc(),
                    "chunk_ids": [document["_id"] for document in chunk],
                }
                error_logs.append(chunk_error_log)
                logger.error(chunk)
                logger.error(traceback.format_exc())
            else:
                # if there is no exception then this block will be executed
                # we only update schema on the first chunk 
                # otherwise it breaks down how the backend handles
                # schema updates
                result = self.update_chunk(
                    new_batch, 
                    update_schema=chunk_counter < self.MAX_SCHEMA_UPDATE_LIMITER
                )
                successful_chunks += 1
                logger.debug(result)
            
            finally:
                # executes after everything wraps up
                if self.job_id:
                    self.update_progress(chunk_counter + 1)

        self._error_logs = error_logs
        if self.num_chunks > 0:
            self._success_ratio = successful_chunks / self.num_chunks
