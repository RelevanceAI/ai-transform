from typing import Any, Dict, List

from slim.api.api import API
from slim.types import Document, Schema


class Dataset:
    def __init__(self, api: API, dataset_id: str):
        self._api = api
        self._dataset_id = dataset_id

    def __getitem__(self, index: str) -> Any:
        if isinstance(index, str):
            from slim.dataset.series import Series

            return Series(dataset=self, field=index)
        else:
            raise NotImplementedError("index must of type `str` (field in dataset)")

    def __len__(self, *args, **kwargs) -> int:
        return self.get_documents(1, *args, **kwargs)["count"]

    @property
    def schema(self) -> Schema:
        return self._api._get_schema(self._dataset_id)

    def create(self):
        return self._api._create_dataset(self._dataset_id)

    def delete(self):
        return self._api._delete_dataset(self._dataset_id)

    def insert_documents(
        self, documents: List[Document], *args, **kwargs
    ) -> Dict[str, Any]:
        return self._api._bulk_insert(
            dataset_id=self._dataset_id, documents=documents, *args, **kwargs
        )

    def update_documents(
        self, documents: List[Document], *args, **kwargs
    ) -> Dict[str, Any]:
        return self._api._bulk_update(
            dataset_id=self._dataset_id, documents=documents, *args, **kwargs
        )

    def get_documents(self, page_size: int, *args, **kwargs) -> Dict[str, Any]:
        return self._api._get_where(
            dataset_id=self._dataset_id, page_size=page_size, *args, **kwargs
        )

    def len(self, *args, **kwargs):
        """
        Get length of dataset, usually used with filters
        """
        return self._api._get_where(
            dataset_id=self._dataset_id, page_size=1, *args, **kwargs
        )

    def insert_metadata(self, metadata: Dict[str, Any]):
        return self._api._update_metadata(
            dataset_id=self._dataset_id,
            metadata=metadata,
        )

    def update_metadata(self, metadata: Dict[str, Any]):
        metadata = self.get_metadata().update(metadata)
        return self._api._update_metadata(
            dataset_id=self._dataset_id,
            metadata=metadata,
        )

    def get_metadata(self) -> Dict[str, Any]:
        return self._api._get_metadata(dataset_id=self._dataset_id)
