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

from typing import Optional, Dict, Any, List, Sequence, NamedTuple

from workflows_core.dataset.dataset import Dataset
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.utils.document import Document
from workflows_core.types import Filter

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


class DenseOutput(NamedTuple):
    dataset_id: str
    documents: Sequence[Dict[str, Any]]


class DenseOutputEngine(AbstractEngine):
    def __init__(
        self,
        input_dataset: Dataset = None,
        output_datasets: Sequence[Dataset] = None,
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

        assert input_dataset.len(filters=filters) == len(
            output_datasets
        ), "You must have an output dataset dataset for every document your input dataset"

        operator.set_postprocess(False)
        self.token = input_dataset.token
        self._store_dataset_relationship(
            input_dataset=input_dataset, output_datasets=output_datasets
        )
        super().__init__(
            dataset=input_dataset,
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

        self._output_datasets = output_datasets
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
            for mini_batch in AbstractEngine.chunk_documents(
                self._transform_chunksize, mega_batch
            ):
                document_mapping = self.operator(mini_batch)
                for dataset_id, documents in document_mapping.items():
                    dataset = Dataset.from_details(dataset_id, self.token)
                    result = dataset.bulk_insert(documents)
                    logger.debug({"dataset_id": dataset_id, "result": result})

            # executes after everything wraps up
            if self.job_id:
                self.update_progress(batch_index + 1)

            self._operator.post_hooks(self._dataset)

        self._error_logs = error_logs
        if self.num_chunks > 0:
            self.set_success_ratio(successful_chunks)
            logger.debug({"success_ratio": self._success_ratio})
