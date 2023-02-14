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

from typing import Optional, List, Sequence

from ai_transform.dataset.dataset import Dataset
from ai_transform.operator.dense_operator import DenseOperator
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.types import Filter

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


class DenseOutputEngine(AbstractEngine):
    def __init__(
        self,
        dataset: Dataset = None,
        operator: DenseOperator = None,
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

        self.token = dataset.token

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
        iterator = self.get_iterator()

        self.update_progress(0)

        output_dataset_ids = []

        self.operator.pre_hooks(self._dataset)

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
                    output_dataset_ids.append(dataset_id)
                    dataset = Dataset.from_details(dataset_id, self.token)
                    result = dataset.bulk_insert(documents)
                    logger.debug({"dataset_id": dataset_id, "result": result})

                # executes after everything wraps up
                self.update_progress(len(mini_batch))

        self.operator.post_hooks(self._dataset)

        output_datasets = self.datasets_from_ids(output_dataset_ids)
        self.operator.store_dataset_relationship(self.dataset, output_datasets)

        self.set_success_ratio()

    def datasets_from_ids(self, dataset_ids: Sequence[str]) -> Sequence[Dataset]:
        return [
            Dataset.from_details(dataset_id, self.token) for dataset_id in dataset_ids
        ]
