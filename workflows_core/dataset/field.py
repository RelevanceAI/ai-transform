from typing import List, Optional, Union

from workflows_core.types import Filter
from workflows_core.utils.document_list import DocumentList


class Field:
    def __init__(self, dataset, field: str):
        from workflows_core.dataset.dataset import Dataset

        self._dataset: Dataset = dataset
        self._field = field
        if field != "_id":
            self._dtype = dataset.schema.get(field)
        else:
            self._dtype = None
        self._filter_type = self._get_filter_type()

    @property
    def dataset_id(self):
        return self._dataset.dataset_id

    @property
    def _text_field(self):
        return self._field

    def _get_filter_type(self) -> str:
        if self._dtype == "numeric":
            filter_type = "numeric"
        elif self._dtype == "date":
            filter_type = "date"
        elif self._dtype is None:
            filter_type = "ids"
        else:
            filter_type = "exact_match"
        return filter_type

    def __eq__(
        self,
        other: Union[str, float, int, bool, None],
        filter_type: Optional[str] = None,
    ) -> Filter:
        if filter_type is None:
            filter_type = self._filter_type
        return [
            {
                "field": self._field,
                "filter_type": filter_type,
                "condition": "==",
                "condition_value": other,
            }
        ]

    def __lt__(
        self,
        other: Union[str, float, int, bool, None],
        filter_type: Optional[str] = None,
    ) -> Filter:
        if filter_type is None:
            filter_type = self._filter_type
        return [
            {
                "field": self._field,
                "filter_type": filter_type,
                "condition": "<",
                "condition_value": other,
            }
        ]

    def __le__(
        self,
        other: Union[str, float, int, bool, None],
        filter_type: Optional[str] = None,
    ) -> Filter:
        if filter_type is None:
            filter_type = self._filter_type
        return [
            {
                "field": self._field,
                "filter_type": filter_type,
                "condition": "<=",
                "condition_value": other,
            }
        ]

    def __gt__(
        self,
        other: Union[str, float, int, bool, None],
        filter_type: Optional[str] = None,
    ) -> Filter:
        if filter_type is None:
            filter_type = self._filter_type
        return [
            {
                "field": self._field,
                "filter_type": filter_type,
                "condition": ">",
                "condition_value": other,
            }
        ]

    def __ge__(
        self,
        other: Union[str, float, int, bool, None],
        filter_type: Optional[str] = None,
    ) -> Filter:
        if filter_type is None:
            filter_type = self._filter_type
        return [
            {
                "field": self._field,
                "filter_type": filter_type,
                "condition": ">=",
                "condition_value": other,
            }
        ]

    def contains(self, other: str) -> Filter:
        return [
            {
                "field": self._field,
                "filter_type": "contains",
                "condition": "==",
                "condition_value": other,
            }
        ]

    def exists(self) -> Filter:
        return [
            {
                "field": self._field,
                "filter_type": "exists",
                "condition": "==",
                "condition_value": " ",
            }
        ]

    def not_exists(self) -> Filter:
        return [
            {
                "field": self._field,
                "filter_type": "exists",
                "condition": "!=",
                "condition_value": " ",
            }
        ]

    def insert_centroids(self, centroid_documents: DocumentList, alias: str):
        raise NotImplementedError(
            "`insert_centroids` not available for non vector_fields"
        )

    def get_centroids(self):
        raise NotImplementedError(
            "`insert_centroids` not available for non vector_fields"
        )

    def get_keyphrase(self, keyphrase_id: str):
        raise NotImplementedError(
            "`get_keyphrase` not available for non keyphrase_fields"
        )

    def update_keyphrase(self, keyphrase_id: str, update: dict):
        raise NotImplementedError(
            "`update_keyphrase` not available for non keyphrase_fields"
        )

    def delete_keyphrase(self, keyphrase_id: str):
        raise NotImplementedError(
            "`remove_keyphrase` not available for non keyphrase_fields"
        )

    def bulk_update_keyphrases(self, updates: List):
        raise NotImplementedError(
            "`bulk_update_keyphrases` not available for non keyphrase_fields"
        )

    def list_keyphrases(self, page_size: int = 100, page: int = 1, sort: list = None):
        raise NotImplementedError(
            "`list_keyphrases` not available for non keyphrase_fields"
        )


class VectorField(Field):
    def __init__(self, dataset, field: str):
        super().__init__(dataset=dataset, field=field)

    def insert_centroids(self, centroid_documents: DocumentList, alias: str):
        return self._dataset.api._insert_centroids(
            dataset_id=self.dataset_id,
            cluster_centers=centroid_documents,
            vector_fields=[self._text_field],
            alias=alias,
        )

    def get_centroids(self, alias: str):
        return self._dataset.api._get_centroids(
            dataset_id=self.dataset_id,
            vector_fields=[self._text_field],
            alias=alias,
        )


class KeyphraseField(Field):
    def __init__(self, dataset, field: str):
        super().__init__(dataset=dataset, field=field)
        _, text_field, alias, *_ = field.split(".")
        self._keyphrase_text_field = text_field
        self._keyphrase_alias = alias

    def get_keyphrase(self, keyphrase_id: str):
        return self._dataset.api._get_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_text_field,
            alias=self._keyphrase_alias,
            keyphrase_id=keyphrase_id,
        )

    def update_keyphrase(self, keyphrase_id: str, update: dict):
        return self._dataset.api._update_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_text_field,
            alias=self._keyphrase_alias,
            keyphrase_id=keyphrase_id,
            update=update,
        )

    def delete_keyphrase(self, keyphrase_id: str):
        return self._dataset.api._delete_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_text_field,
            alias=self._keyphrase_alias,
            keyphrase_id=keyphrase_id,
        )

    def bulk_update_keyphrases(self, updates: List):
        return self._dataset.api._bulk_update_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_text_field,
            alias=self._keyphrase_alias,
            updates=updates,
        )

    def list_keyphrases(self, page_size: int = 100, page: int = 1, sort: list = None):
        return self._dataset.api._list_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_text_field,
            alias=self._keyphrase_alias,
            page_size=page_size,
            page=page,
            sort=[] if sort is None else sort,
        )
