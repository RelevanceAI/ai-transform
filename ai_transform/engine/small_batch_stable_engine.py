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

from typing import List

from ai_transform.logger import format_logging_info
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.utils.document_list import DocumentList
from ai_transform.utils.document import Document

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

    def _filter_for_non_empty_list(self, docs: DocumentList):
        # if there are more keys than just _id in each document
        # then return that as a list of Documents
        # length of a dictionary is just 1 if there is only 1 key
        return DocumentList([d for d in docs if len(d) > 1])

    def handle_upsert(self, chunk_counter: int, chunk_to_update: List[Document]):
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
        logger.debug(format_logging_info(result))

    def _transform_and_upsert(self, batch_index: int, batch: List[Document]):
        batch_to_insert = []

        for chunk in AbstractEngine.chunk_documents(self._transform_chunksize, batch):
            transformed_batch = self._operate(chunk)
            if transformed_batch is not None:
                batch_to_insert += transformed_batch

        self.handle_upsert(batch_index, batch_to_insert)
        self.update_progress(len(batch_to_insert))

        return batch_to_insert

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """
        iterator = self.iterate()

        batch = []

        self.operator.pre_hooks(self.dataset)

        upload_index = 0
        for minibatch in self.api_progress(
            iterator,
            show_progress_bar=self._show_progress_bar,
        ):
            batch += minibatch

            if len(batch) >= self._transform_threshold:
                self._transform_and_upsert(upload_index, batch)
                upload_index += 1
                batch = []

        self.operator.post_hooks(self.dataset)

        self._transform_and_upsert(upload_index, batch)
