import logging
import traceback

from typing import Optional, Sequence, List

from workflows_core.dataset.dataset import Dataset
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.document import Document
from workflows_core.utils.payload_optimiser import get_optimal_chunksize
from workflows_core.types import Filter

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


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
        seed: int = 42,
        output_to_status: Optional[bool] = False,
        documents: Optional[List[object]] = None,
        limit_documents: Optional[int] = None,
        transform_chunksize: int = 20,
        show_progress_bar: bool = True,
        optimise_payloads: bool = False,
    ):
        """
        Parameters
        -----------

        pull_chunksize
            the number of documents that are downloaded

        """
        if not refresh:
            output_field_filters = []
            for operator in operators:
                for output_field in operator._output_fields:
                    output_field_filters.append(dataset[output_field].not_exists())
            filters += [{"filter_type": "or", "condition_value": output_field_filters}]

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
            seed=seed,
            output_to_status=output_to_status,
            documents=documents,
            limit_documents=limit_documents,
            optimise_payloads=optimise_payloads,
        )

        self._num_chunks *= len(operators)
        self._transform_chunksize = min(self.pull_chunksize, transform_chunksize)
        self._show_progress_bar = show_progress_bar

    def apply(self) -> None:
        """
        Returns the ratio of successful chunks / total chunks needed to iterate over the dataset
        """

        self.update_progress(0)

        desc = " -> ".join([repr(operator) for operator in self._operators])
        progress_bar = tqdm(
            range(self.num_chunks),
            desc=desc,
            disable=(not self._show_progress_bar),
            total=self.num_chunks,
        )

        for operator_index, operator in enumerate(self.operators):
            operator.pre_hooks(self._dataset)

            if self.documents is None or len(self.documents) == 0:
                # Iterate through dataset
                dataset_iterator = self.iterate()
            else:
                # Iterate through passed in documents
                dataset_iterator = self.chunk_documents(
                    chunksize=min(100, len(self.documents)), documents=self.documents
                )

            successful_chunks = 0
            error_logs = []
            optimised = False

            for batch_index, mega_batch in enumerate(dataset_iterator):
                transformed_mega_batch: List[Document] = []

                for mini_batch in AbstractEngine.chunk_documents(
                    self._transform_chunksize, mega_batch
                ):
                    try:
                        # note: do not put an IF inside ths try-except-else loop - the if code will not work
                        transformed_batch = operator(mini_batch)
                    except Exception as e:
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
                        if transformed_batch is not None:
                            transformed_mega_batch += transformed_batch

                if self.output_to_status:
                    # Store in output documents
                    self.extend_output_documents(
                        [document.to_json() for document in transformed_mega_batch]
                    )
                else:
                    # Store in dataset
                    # We want to make sure the schema updates
                    # on the first chunk upserting
                    if batch_index < self.MAX_SCHEMA_UPDATE_LIMITER:
                        ingest_in_background = False
                    else:
                        ingest_in_background = True

                    if not optimised:
                        push_chunksize = get_optimal_chunksize(
                            transformed_mega_batch[:50]
                        )

                    for batch_to_insert in self.chunk_documents(
                        push_chunksize, transformed_mega_batch
                    ):
                        result = self.update_chunk(
                            batch_to_insert,
                            update_schema=batch_index < self.MAX_SCHEMA_UPDATE_LIMITER,
                            ingest_in_background=ingest_in_background,
                        )
                        logger.debug(result)

                # executes after everything wraps up
                if self.job_id:
                    progress_index = operator_index + batch_index + 1
                    self.update_progress(progress_index)

                progress_bar.update(1)

            operator.post_hooks(self._dataset)

        self._error_logs = error_logs
        if self.num_chunks > 0:
            self.set_success_ratio(successful_chunks)
            logger.debug({"success_ratio": self._success_ratio})
