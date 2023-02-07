import os
import logging
import traceback

from typing import Any

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.payload_optimiser import get_optimal_chunksize
from tqdm.auto import tqdm


logger = logging.getLogger(__file__)


class InMemoryEngine(AbstractEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def apply(self) -> Any:

        self.update_progress(0)
        iterator = self.iterate()
        error_logs = []

        progress = 0
        documents = []

        for chunk in iterator:
            documents += chunk

            if self.job_id:
                self.update_progress(progress)

        try:
            transformed_documents = self.operator(documents)
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

        batch_to_insert = []
        payload_size = 0
        upload_progress = 0

        # we only get 50 documents because this is an expensive operation
        push_chunksize = get_optimal_chunksize(transformed_documents[:50])
        for batch_to_insert in self.chunk_documents(
            push_chunksize, transformed_documents
        ):
            logger.debug({"payload_size": payload_size})
            result = self.update_chunk(
                batch_to_insert,
                update_schema=upload_progress < self.MAX_SCHEMA_UPDATE_LIMITER,
                ingest_in_background=True,
            )
            logger.debug(result)

            progress += 1
            upload_progress += 1

            if self.job_id:
                self.update_progress(progress)

        # executes after everything wraps up
        if self.job_id:
            self.update_progress(progress + 1)
