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

from typing import Optional, List

from ai_transform.logger import ic, format_logging_info
from ai_transform.dataset.dataset import Dataset
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.utils.document import Document
from ai_transform.types import Filter


class StableEngine(AbstractEngine):
    def __init__(
        self,
        dataset: Dataset = None,
        operator: AbstractOperator = None,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        pull_chunksize: Optional[int] = 3000,
        refresh: bool = True,
        after_id: Optional[List[str]] = None,
        worker_number: int = None,
        total_workers: int = None,
        check_for_missing_fields: bool = True,
        output_to_status: Optional[bool] = False,
        documents: Optional[List[object]] = None,
        limit_documents: Optional[int] = None,
        transform_chunksize: int = 20,
        show_progress_bar: bool = True,
    ):
        super().__init__(
            dataset=dataset,
            operator=operator,
            filters=filters,
            select_fields=select_fields,
            pull_chunksize=pull_chunksize,
            refresh=refresh,
            after_id=after_id,
            worker_number=worker_number,
            total_workers=total_workers,
            check_for_missing_fields=check_for_missing_fields,
            output_to_status=output_to_status,
            documents=documents,
            limit_documents=limit_documents,
        )

        self._transform_chunksize = min(self.pull_chunksize, transform_chunksize)
        self._show_progress_bar = show_progress_bar

    def handle_upsert(self, batch_index: int, batch_to_insert: List[Document]):
        if self.output_to_status:
            # Store in output documents
            self.extend_output_documents([document.to_json() for document in batch_to_insert])
        else:
            # Store in dataset
            # We want to make sure the schema updates
            # on the first chunk upserting
            if batch_index < self.MAX_SCHEMA_UPDATE_LIMITER:
                ingest_in_background = False
            else:
                ingest_in_background = True

            result = self.update_chunk(
                batch_to_insert,
                update_schema=batch_index < self.MAX_SCHEMA_UPDATE_LIMITER,
                ingest_in_background=ingest_in_background,
            )
            ic(result)

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """
        iterator = self.get_iterator()

        self.operator.pre_hooks(self._dataset)
        for batch_index, mega_batch in enumerate(self.api_progress(iterator)):
            batch_to_insert: List[Document] = []

            for mini_batch in AbstractEngine.chunk_documents(self._transform_chunksize, mega_batch):
                transformed_batch = self._operate(mini_batch)
                if transformed_batch is not None:
                    batch_to_insert += transformed_batch

            self.handle_upsert(batch_index, batch_to_insert)

        self.operator.post_hooks(self._dataset)
