import logging
import traceback
from typing import Any

from workflows_core.engine.abstract_engine import AbstractEngine
from tqdm.auto import tqdm


logger = logging.getLogger(__file__)


class InMemoryEngine(AbstractEngine):
    def __init__(self, show_progress_bar: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = show_progress_bar
        self._progress = tqdm(
            desc=repr(self.operator),
            total=self.num_chunks * 2,
            disable=(not show_progress_bar),
        )

    def apply(self) -> Any:

        iterator = self.iterate()
        error_logs = []

        documents = []
        for chunk in iterator:
            documents += chunk
            self._progress.update(1)

        try:
            new_batch = self.operator(documents)
        except Exception as e:
            chunk_error_log = {
                "exception": str(e),
                "traceback": traceback.format_exc(),
                "chunk_ids": [document["_id"] for document in chunk],
            }
            error_logs.append(chunk_error_log)
            logger.error(chunk)
            logger.error(traceback.format_exc())
            self._success_ratio = 0.0
        else:
            self._success_ratio = 1.0

        # Update this in series
        for i in range(self._num_chunks):
            chunk = new_batch[i * self.chunksize : (i + 1) * self._chunksize]
            self.update_chunk(
                chunk, 
                ingest_in_background=True, 
                # Update schema only on the first chunk otherwise it crashes the
                # schema update
                update_schema=True if i < self.MAX_SCHEMA_UPDATE_LIMITER else False
            )
            if self.job_id:
                self.update_progress(i + 1)

        # WE have to remove this code to avoid hammering the server
        # def payload_generator():
        #     for i in range(self._num_chunks):
        #         yield new_batch[i * self._chunksize : (i + 1) * self._chunksize]
        #         self._progress.update(1)

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     futures = [
        #         executor.submit(self.update_chunk, payload)
        #         for payload in payload_generator()
        #     ]

        #     for future in concurrent.futures.as_completed(futures):
        #         try:
        #             result = future.result()
        #         except Exception as e:
        #             logging.error(e)
        #             logging.error(traceback.format_exc())
        #         else:
        #             logging.debug(result)
