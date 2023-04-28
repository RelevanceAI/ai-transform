import logging

from typing import Optional, Sequence, List

from ai_transform.logger import format_logging_info, ic
from ai_transform.dataset.dataset import Dataset
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.utils.document import Document
from ai_transform.types import Filter


class MultiPassEngine(AbstractEngine):
    def __init__(
        self,
        dataset: Dataset = None,
        operators: Sequence[AbstractOperator] = None,
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
            operators=operators,
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
        """
        This functions handles the reinsertion of new transformed data back
        into the dataset. This could happen for two reasons:
            1. For a regular workflow
            2. For outputting to status
        """
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

    def _operate(self, operator: AbstractOperator, mini_batch: List[Document]):
        try:
            # note: do not put an IF inside ths try-except-else loop - the if code will not work
            transformed_batch = operator(mini_batch)
        except Exception as e:
            ic(e)
            ic({"chunk_ids": self._get_chunks_ids(mini_batch)})
        else:
            # if there is no exception then this block will be executed
            # we only update schema on the first chunk
            # otherwise it breaks down how the backend handles
            # schema updates
            self._successful_documents += len(mini_batch)
            return transformed_batch

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """
        for operator_index, operator in enumerate(self.operators):
            operator.pre_hooks(self._dataset)

            iterator = self.get_iterator()

            for batch_index, mega_batch in enumerate(
                self.api_progress(iterator, pass_index=operator_index, n_passes=len(self.operators))
            ):
                batch_to_insert: List[Document] = []

                for mini_batch in AbstractEngine.chunk_documents(self._transform_chunksize, mega_batch):
                    transformed_batch = self._operate(operator, mini_batch)
                    if transformed_batch is not None:
                        batch_to_insert += transformed_batch

                if batch_to_insert:
                    self.handle_upsert(batch_index, batch_to_insert)

            operator.post_hooks(self._dataset)
