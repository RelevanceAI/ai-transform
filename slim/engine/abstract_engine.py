import math
from typing import Any, List, Optional
from abc import ABC, abstractmethod

from slim.types import Filter
from slim.dataset import Dataset
from slim.operator import AbstractOperator
from slim.utils import Document


class AbstractEngine(ABC):
    def __init__(
        self,
        dataset: Dataset,
        operator: AbstractOperator,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        chunksize: int = 8,
        refresh: bool = True,
        after_id: Optional[List[str]] = None,
    ):
        if select_fields is not None:
            assert all(
                field in dataset.schema
                for field in select_fields
                if field not in {"_id", "insert_date_"}
            ), "Some fields not in dataset schema"
        self._dataset = dataset
        self._select_fields = select_fields

        assert chunksize > 0, "Chunksize should be a Positive Integer"
        self._chunksize = chunksize

        self._filters = filters
        self._operator = operator

        self._refresh = refresh
        self._after_id = after_id

        self._size = dataset.len(filters=filters)
        self._nb = math.ceil(self._size / chunksize)

    @property
    def nb(self):
        return self._nb

    @property
    def operator(self):
        return self._operator

    @property
    def dataset(self):
        return self._dataset

    @abstractmethod
    def apply(self, operator: AbstractOperator) -> Any:
        raise NotImplementedError

    def __call__(self, operator: AbstractOperator) -> Any:
        raise self.apply(operator)

    def get_chunk(self):
        chunk = self._dataset.get_documents(
            self._chunksize,
            filters=self._filters,
            select_fields=self._select_fields,
            after_id=self._after_id,
        )
        self._after_id = chunk["after_id"]
        return chunk["documents"]

    def update_chunk(self, chunk: List[Document]):
        return self._dataset.update_documents(documents=chunk)
