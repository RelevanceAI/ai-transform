from typing import List, Union

from slim.api.api import API
from slim.types import Document


class Dataset:
    def __init__(self, api: API, dataset_id: str):
        self._api = api
        self._dataset_id = dataset_id

    def __getitem__(self, index: Union[str, int]):
        if isinstance(index, str):
            from slim.dataset.series import Series

            return Series(dataset=self, field=index)
        elif isinstance(index, int):
            return
        else:
            raise NotImplementedError

    @property
    def schema(self):
        return self._api._get_schema(self._dataset_id)

    def create(self):
        return self._api._create_dataset(self._dataset_id)

    def delete(self):
        return self._api._delete_dataset(self._dataset_id)

    def insert_documents(self, documents: List[Document], *args, **kwargs) -> None:
        return self._api._bulk_insert(
            dataset_id=self._dataset_id, documents=documents, *args, **kwargs
        )

    def update_documents(self, documents: List[Document], *args, **kwargs) -> None:
        return self._api._bulk_update(
            dataset_id=self._dataset_id, documents=documents, *args, **kwargs
        )

    def get_documents(self, page_size: int, *args, **kwargs) -> None:
        return self._api._get_where(
            dataset_id=self._dataset_id, page_size=page_size, *args, **kwargs
        )
