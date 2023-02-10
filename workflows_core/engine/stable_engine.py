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

from typing import Optional, List

from workflows_core.dataset.dataset import Dataset
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.document import Document
from workflows_core.types import Filter

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


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
        seed: int = 42,
        output_to_status: Optional[bool] = False,
        documents: Optional[List[object]] = None,
        limit_documents: Optional[int] = None,
        transform_chunksize: int = 20,
        show_progress_bar: bool = True,
    ):
        """
        Parameters
        -----------

        pull_chunksize
            the number of documents that are downloaded

        """
        if not refresh:
            output_field_filters = []
            for output_field in operator._output_fields:
                output_field_filters.append(dataset[output_field].not_exists())
            filters += [{"filter_type": "or", "condition_value": output_field_filters}]

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
            seed=seed,
            output_to_status=output_to_status,
            documents=documents,
            limit_documents=limit_documents,
        )

        self._transform_chunksize = min(self.pull_chunksize, transform_chunksize)
        self._show_progress_bar = show_progress_bar

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """
        if self.documents is None or len(self.documents) == 0:
            # Iterate through dataset
            iterator = self.iterate()
        else:
            # Iterate through passed in documents
            iterator = self.chunk_documents(
                chunksize=min(100, len(self.documents)), documents=self.documents
            )

        successful_chunks = 0
        error_logs = []

        self.update_progress(0)

        self.operator.post_hooks(self._dataset)
        
        for batch_index, mega_batch in enumerate(
            tqdm(
                iterator,
                desc=repr(self.operator),
                disable=(not self._show_progress_bar),
                total=self.num_chunks,
            )
        ):
            batch_to_insert: List[Document] = []

            for mini_batch in AbstractEngine.chunk_documents(
                self._transform_chunksize, mega_batch
            ):
                try:
                    # note: do not put an IF inside ths try-except-else loop - the if code will not work
                    transformed_batch = self.operator(mini_batch)
                    if isinstance(transformed_batch, dict):
                        n_processed_pricing = transformed_batch['n_processed_pricing']
                        transformed_batch = transformed_batch['documents']
                except Exception as e:
                    n_processed_pricing = None
                    chunk_error_log = {
                        "exception": str(e),
                        "traceback": traceback.format_exc(),
                        "chunk_ids": self._get_chunks_ids(mini_batch),
                    }
                    error_logs.append(chunk_error_log)
                    logger.error(mini_batch)
                else:
                    # if there is no exception then this block will be executed
                    # we only update schema on the first chunk
                    # otherwise it breaks down how the backend handles
                    # schema updates
                    successful_chunks += 1
                    batch_to_insert += transformed_batch

            if self.output_to_status:
                # Store in output documents
                self.extend_output_documents(
                    [document.to_json() for document in batch_to_insert]
                )
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
                logger.debug(result)

            # executes after everything wraps up
            if self.job_id:
                self.update_progress(
                    batch_index + 1, 
                    n_processed_pricing=n_processed_pricing
                )

            self._operator.post_hooks(self._dataset)

        self._error_logs = error_logs
        if self.num_chunks > 0:
            self.set_success_ratio(successful_chunks)
            logger.debug({"success_ratio": self._success_ratio})
