import os
import logging
import traceback

from typing import Any

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.payload_optimiser import get_sizeof_document_mb
from tqdm.auto import tqdm


logger = logging.getLogger(__file__)


class InMemoryEngine(AbstractEngine):
    def __init__(self, show_progress_bar: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = show_progress_bar
        self._progress = tqdm(
            desc=repr(self.operator),
            total=self.num_chunks,
            disable=(not show_progress_bar),
        )

    def apply(self) -> Any:

        self.update_progress(0)
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

        max_payload_size = float(os.getenv("WORKFLOWS_MAX_MB", 20))

        num_updates = 0
        batch_to_insert = []
        payload_size = 0

        for document in chunk:
            document_size = get_sizeof_document_mb(document)
            if payload_size + document_size >= max_payload_size:

                logger.debug({"payload_size": payload_size})
                result = self.update_chunk(
                    batch_to_insert,
                    update_schema=num_updates < self.MAX_SCHEMA_UPDATE_LIMITER,
                    ingest_in_background=True,
                )
                logger.debug(result)
                batch_to_insert = []
                payload_size = 0
                num_updates += 1

                if self.job_id:
                    self.update_progress(num_updates)

            batch_to_insert.append(document)
            payload_size += document_size

        # executes after everything wraps up
        if self.job_id:
            self.update_progress(num_updates + 1)

        result = self.update_chunk(
            batch_to_insert,
            update_schema=True,
            ingest_in_background=True,
        )
        logger.debug(result)
