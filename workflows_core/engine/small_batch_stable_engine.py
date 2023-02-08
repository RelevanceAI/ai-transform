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

from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.document_list import DocumentList
from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


class SmallBatchStableEngine(AbstractEngine):
    def __init__(
        self,
        dataset: Dataset,
        operator: AbstractOperator,
        pull_chunksize: int = 5,
        transform_threshold: int = 1000,
        transform_chunksize: int = 20,
        *args,
        **kwargs
    ):
        """
        Parameters
        -----------

        pull_chunksize: int
            the number of documents that are downloaded
        transform_threshold: int
            transform_threshold is the minimum amount of documents
            that are required before it starts running the transform
            function whereas the transform_chunksize is the
            number of documents each transform operation will take.
        transform_chunksize: int
            the number of documents that get passed to the operator

        """
        super().__init__(
            dataset=dataset,
            operator=operator,
            pull_chunksize=pull_chunksize,
            *args,
            **kwargs
        )

        self._transform_threshold = transform_threshold
        self._transform_chunksize = transform_chunksize

        self._show_progress_bar = kwargs.pop("show_progress_bar", True)
        self._num_chunks = self._size // self._transform_threshold + 1

    def _filter_for_non_empty_list(self, docs: DocumentList):
        # if there are more keys than just _id in each document
        # then return that as a list of Documents
        # length of a dictionary is just 1 if there is only 1 key
        return DocumentList([d for d in docs if len(d) > 1])

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """
        iterator = self.iterate()
        successful_chunks = 0
        error_logs = []

        batch = []

        for chunk_counter, small_chunk in tqdm(
            enumerate(iterator),
            desc=repr(self.operator),
            disable=(not self._show_progress_bar),
            total=self._num_chunks,
        ):
            if self._use_document_progress:
                self.update_progress(chunk_counter)
            batch += small_chunk

            if len(batch) >= self._transform_threshold:
                chunk_to_update = []

                for chunk in AbstractEngine.chunk_documents(
                    self._transform_chunksize, batch
                ):
                    # place here and not in large_chunk to ensure consistency
                    # across progress and success etc.
                    chunk = self._filter_for_non_empty_list(chunk)
                    if len(chunk) == 0:
                        new_batch = []

                    try:
                        # note: do not put an IF inside ths try-except-else loop - the if code will not work
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
                        chunk_to_update += new_batch

                    # We want to make sure the schema updates
                    # on the first chunk upserting
                    if chunk_counter < self.MAX_SCHEMA_UPDATE_LIMITER:
                        ingest_in_background = False
                    else:
                        ingest_in_background = True

                result = self.update_chunk(
                    chunk_to_update,
                    update_schema=chunk_counter < self.MAX_SCHEMA_UPDATE_LIMITER,
                    ingest_in_background=ingest_in_background,
                )

                successful_chunks += 1
                logger.debug(result)
                batch = []

        # executes after everything wraps up
        if self.job_id and self._use_document_progress:
            self.update_progress(chunk_counter + 1)

        self._error_logs = error_logs
        if self.num_chunks > 0:
            self._success_ratio = successful_chunks / self.num_chunks
            logger.debug({"success_ratio": self._success_ratio})
