"""
    Stable Engine Pseudo-algorithm-
        1. Downloads about 1000 documents.    
        2. Processes it in much smaller chunks.
        3. Upserts all 1000 documents.
        4. Repeat until dataset has finished looping
    
    We download a large chunk and upsert large chunks to avoid hammering 
    our servers.

"""
import logging
import traceback
import math
import time
from json import JSONDecodeError
from typing import Any
from typing import Optional, List
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.document_list import DocumentList
from workflows_core.types import Filter
from workflows_core.errors import MaxRetriesError
from tqdm.auto import tqdm

logger = logging.getLogger(__file__)

class StableEngine(AbstractEngine):
    def __init__(self, *args, pull_chunksize: int=1000, **kwargs):
        """
        Parameters
        -----------

        pull_chunksize
            the number of documents that are downloaded

        """
        self._pull_chunksize=pull_chunksize
        super().__init__(*args, **kwargs)
        self._show_progress_bar = kwargs.pop("show_progress_bar", True)
    

    def chunk_documents(self, documents: DocumentList, chunksize: int=20):
        num_chunks = math.ceil(len(documents) / chunksize)
        for i in range(num_chunks):
            yield documents[(i * chunksize): ((i + 1)*chunksize)]

    def iterate(
        self,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        max_retries: int = 5,
    ):

        if filters is None:
            filters = self._filters

        filters += self._get_workflow_filter()

        if select_fields is None:
            select_fields = self._select_fields

        retry_count = 0
        while True:
            try:
                chunk = self._dataset.get_documents(
                    self._pull_chunksize,
                    filters=filters,
                    select_fields=select_fields,
                    after_id=self._after_id,
                    worker_number=self.worker_number,
                )
            except (ConnectionError, JSONDecodeError) as e:
                logger.error(e)
                retry_count += 1
                time.sleep(1)

                if retry_count >= max_retries:
                    raise MaxRetriesError("max number of retries exceeded")
            else:
                self._after_id = chunk["after_id"]
                if not chunk["documents"]:
                    break
                yield chunk["documents"]
                retry_count = 0

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """

        iterator = self.iterate()
        successful_chunks = 0
        error_logs = []

        for chunk_counter, large_chunk in enumerate(tqdm(
            iterator,
            desc=repr(self.operator),
            disable=(not self._show_progress_bar),
            total=self.num_chunks,
        )):
            chunk_to_update = []

            for chunk in self.chunk_documents(large_chunk, chunksize=self._chunksize):
                try:
                    new_batch = self.operator(chunk)
                    successful_chunks += 1

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
                    chunk_to_update.extend(new_batch)
                    
            result = self.update_chunk(
                chunk_to_update,
                update_schema=chunk_counter < self.MAX_SCHEMA_UPDATE_LIMITER,
                ingest_in_background=True
            )
            logger.debug(result)
                
            # executes after everything wraps up
            if self.job_id:
                self.update_progress(chunk_counter + 1)

        self._error_logs = error_logs
        if self.num_chunks > 0:
            self._success_ratio = successful_chunks / self.num_chunks
