import math
import warnings

from typing import Any, List, Optional
from abc import ABC, abstractmethod

from workflows_core.types import Filter
from workflows_core.dataset.dataset import Dataset
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.document import Document


class AbstractEngine(ABC):
    def __init__(
        self,
        dataset: Dataset,
        operator: AbstractOperator,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        chunksize: Optional[int] = 8,
        refresh: bool = True,
        after_id: Optional[List[str]] = None,
        worker_number: int = 0,
    ):
        if select_fields is not None:
            assert all(
                field in dataset.schema
                for field in select_fields
                if field not in {"_id", "insert_date_"}
            ), "Some fields not in dataset schema"

        self._dataset = dataset
        self._select_fields = select_fields
        self._size = dataset.len(filters=filters)
        self.worker_number = worker_number

        if isinstance(chunksize, int):
            assert chunksize > 0, "Chunksize should be a Positive Integer"
            self._chunksize = chunksize
            self._num_chunks = math.ceil(self._size / chunksize)
        else:
            warnings.warn(
                f"`chunksize=None` assumes the operation transforms on the entire dataset at once"
            )
            self._chunksize = self._size
            self._num_chunks = 1

        self._filters = filters
        self._operator = operator

        self._refresh = refresh
        self._after_id = after_id

    @property
    def num_chunks(self) -> int:
        return self._num_chunks

    @property
    def operator(self) -> AbstractOperator:
        return self._operator

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @property
    def chunksize(self) -> int:
        return self._chunksize

    @property
    def size(self) -> int:
        return self._size

    @abstractmethod
    def apply(self) -> Any:
        raise NotImplementedError

    def __call__(self) -> Any:
        self.operator.pre_hooks(self._dataset)
        self.apply()
        self.operator.post_hooks(self._dataset)

    def iterate(
        self,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
    ):
        while True:
            chunk = self._dataset.get_documents(
                self._chunksize,
                filters=filters if filters is not None else self._filters,
                select_fields=select_fields
                if select_fields is not None
                else self._select_fields,
                after_id=self._after_id,
                worker_number=self.worker_number,
            )
            self._after_id = chunk["after_id"]
            if not chunk["documents"]:
                break
            yield chunk["documents"]

    def update_chunk(self, chunk: List[Document]):
        if chunk:
            return self._dataset.update_documents(documents=chunk)
