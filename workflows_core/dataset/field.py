import numpy as np

from typing import Dict, Any, List, Optional, Union

from workflows_core.types import Filter
from workflows_core.utils.document_list import DocumentList
from workflows_core.utils.keyphrase import Keyphrase
from dataclasses import asdict


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
        if "_chunk_" in self._field:
            count = self._field.count(".")
            if count:
                parent_field = self._field.split(".")[0]
            else:
                parent_field = self._field

            return [
                {
                    "chunk": {
                        "path": parent_field,
                        "filters": [{"fieldExists": {"field": self._field}}],
                    }
                }
            ]
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

    def insert_centroids(self, centroid_documents: DocumentList):
        raise NotImplementedError(
            f"`insert_centroids` not available for non-vector fields"
        )

    def label_openai(
        self,
        field: str,
        question_suffix: str,
        accuracy: int = 4,
        cluster_ids: list = None,
        dont_save_summaries: bool = True,
        filters: list = None,
    ):
        raise NotImplementedError(f"`label_openai` not available for non-vector fields")

    def get_centroids(
        self,
        page_size: int = 5,
        page: int = 1,
        cluster_ids: Optional[List] = None,
        include_vector: bool = False,
    ):
        raise NotImplementedError(
            f"`get_centroids` not available for non-vector fields"
        )

    def get_all_centroids(self):
        raise NotImplementedError(
            f"`get_all_centroids` not available for non-vector fields"
        )

    def create_centroid_documents(self, labels: Union[np.ndarray, List[int]]):
        raise NotImplementedError(
            f"`create_centroid_documents` not available for non-vector fields"
        )

    def list_closest_to_center(
        self,
        centroid_vector_fields: List[str],
        cluster_field: str,
        approx: int = 0,
        sum_fields: bool = True,
        page: int = 1,
        similarity_metric: str = "cosine",
        min_score: float = 0,
        include_vector: bool = False,
        include_count: bool = True,
        include_relevance: bool = False,
        page_size: int = 20,
        cluster_properties_filter: Dict[str, Any] = None,
        cluster_ids: List[str] = None,
        filters: List[Filter] = None,
        select_fields: List[str] = None,
    ):
        raise NotImplementedError(
            "`list_closest_to_center` not available for non-vector fields"
        )

    def get_keyphrase(self, keyphrase_id: str):
        raise NotImplementedError(
            f"`get_keyphrase` not available for non-keyphrase fields"
        )

    def update_keyphrase(
        self,
        keyphrase_id: str,
        alias: str,
        keyphrase: str,
        frequency: int = 0,
        ancestors: list = None,
        parents: list = None,
        metadata: dict = None,
        keyphrase_score: float = 0,
        level: int = 0,
    ):
        raise NotImplementedError(
            f"`update_keyphrase` not available for non-keyphrase fields"
        )

    def delete_keyphrase(self, keyphrase_id: str):
        raise NotImplementedError(
            f"`delete_keyphrase` not available for non-keyphrase fields"
        )

    def bulk_update_keyphrases(self, updates: List[Union[Keyphrase, dict]]):
        raise NotImplementedError(
            f"`bulk_update_keyphrases` not available for non-keyphrase fields"
        )

    def list_keyphrases(self, page_size: int = 100, page: int = 1, sort: list = None):
        raise NotImplementedError(
            f"`list_keyphrases` not available for non-keyphrase fields"
        )

    def get_parent(self):
        # Returns None if there is no parent
        for r in self._dataset.list_field_children()["results"]:
            if self._field in r["field_children"]:
                return r["field"]


class ClusterField(Field):
    def __init__(self, dataset, field: str):
        super().__init__(dataset=dataset, field=field)
        _, *middle_fields, alias = field.split(".")
        self._cluster_field = ".".join(middle_fields)
        self._cluster_alias = alias

    def insert_centroids(
        self, centroid_documents: Union[List[Dict[str, Any]], DocumentList]
    ):
        if isinstance(centroid_documents, DocumentList):
            centroid_documents = centroid_documents.to_json()
        return self._dataset.api._insert_centroids(
            dataset_id=self.dataset_id,
            cluster_centers=centroid_documents,
            vector_fields=[self._cluster_field],
            alias=self._cluster_alias,
        )

    def get_centroids(
        self,
        page_size: int = 5,
        page: int = 1,
        cluster_ids: Optional[List] = None,
        include_vector: bool = False,
    ):
        return self._dataset.api._get_centroids(
            dataset_id=self.dataset_id,
            vector_fields=[self._cluster_field],
            alias=self._cluster_alias,
            page_size=page_size,
            page=page,
            cluster_ids=cluster_ids,
            include_vector=include_vector,
        )

    def get_all_centroids(
        self,
        page_size: int = 5,
        cluster_ids: Optional[List] = None,
        include_vector: bool = False,
    ):
        """
        Get all centroids and returns as a dictionary for easy access
        """
        all_centroids = {"results": []}
        page = 1
        while True:
            res = self._dataset.api._get_centroids(
                dataset_id=self.dataset_id,
                vector_fields=[self._cluster_field],
                alias=self._cluster_alias,
                page_size=page_size,
                cluster_ids=cluster_ids,
                include_vector=include_vector,
                page=page,
            )["results"]
            if len(res) == 0:
                break
            else:
                all_centroids["results"] += res
                page += 1
        return all_centroids

    def label_openai(
        self,
        field: str,
        question_suffix: str,
        accuracy: int = 4,
        cluster_ids: list = None,
        dont_save_summaries: bool = True,
        filters: list = None,
    ):
        return self._dataset._api._label_openai(
            dataset_id=self.dataset_id,
            vector_field=self._cluster_field,
            field=field,
            alias=self._cluster_alias,
            question_suffix=question_suffix,
            accuracy=accuracy,
            cluster_ids=cluster_ids if cluster_ids is not None else [],
            dont_save_summaries=dont_save_summaries,
            filters=filters if filters is not None else [],
        )

    def create_centroid_documents(self, labels: List[int]):
        if not isinstance(labels, np.ndarray):
            labels = np.array(labels)

        documents = self._dataset.get_all_documents(select_fields=[self._cluster_field])
        documents = documents["documents"]
        vectors = np.array([document[self._cluster_field] for document in documents])

        n_clusters = len(np.unique(labels))
        centroid_documents = []

        selected_vectors: np.ndarray
        centroid_vector: np.ndarray

        for index in range(n_clusters):
            selected_vectors = vectors[labels == index]
            centroid_vector = selected_vectors.mean(0)

            centroid_document = {
                "_id": f"cluster_{index}",
                f"{self._cluster_field}": centroid_vector.tolist(),
            }
            centroid_documents.append(centroid_document)

        return centroid_documents

    def list_closest_to_center(
        self,
        centroid_vector_fields: List[str],
        cluster_field: str,
        approx: int = 0,
        sum_fields: bool = True,
        page: int = 1,
        similarity_metric: str = "cosine",
        min_score: float = 0,
        include_vector: bool = False,
        include_count: bool = True,
        include_relevance: bool = False,
        page_size: int = 20,
        cluster_properties_filter: Dict[str, Any] = None,
        cluster_ids: List[str] = None,
        filters: List[Filter] = None,
        select_fields: List[str] = None,
    ):
        return self._dataset._api._list_closest_to_center(
            dataset_id=self.dataset_id,
            alias=self._cluster_alias,
            vector_fields=self._cluster_field[0],
            centroid_vector_fields=centroid_vector_fields,
            cluster_field=cluster_field,
            approx=approx,
            sum_fields=sum_fields,
            page=page,
            similarity_metric=similarity_metric,
            min_score=min_score,
            include_vector=include_vector,
            include_count=include_count,
            include_relevance=include_relevance,
            page_size=page_size,
            cluster_properties_filter=cluster_properties_filter,
            cluster_ids=cluster_ids,
            filters=filters,
            select_fields=select_fields,
        )


class KeyphraseField(Field):
    def __init__(self, dataset, field: str):
        super().__init__(dataset=dataset, field=field)
        _, *middle_fields, alias = field.split(".")
        self._keyphrase_field = ".".join(middle_fields)
        self._keyphrase_alias = alias

    def get_keyphrase(self, keyphrase_id: str):
        return self._dataset.api._get_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_field,
            alias=self._keyphrase_alias,
            keyphrase_id=keyphrase_id,
        )

    def update_keyphrase(
        self,
        keyphrase_id: str,
        keyphrase: Union[Keyphrase, str],
        frequency: int = 0,
        ancestors: list = None,
        parents: list = None,
        metadata: dict = None,
        keyphrase_score: float = 0,
        level: int = 0,
    ):
        if isinstance(update, Keyphrase):
            update = asdict(update)
        return self._dataset._api._update_keyphrase(
            dataset_id=self._dataset._dataset_id,
            field=self._keyphrase_field,
            alias=self._keyphrase_alias,
            keyphrase_id=keyphrase_id,
            keyphrase=keyphrase,
            frequency=frequency,
            ancestors=ancestors,
            parents=parents,
            metadata=metadata,
            keyphrase_score=keyphrase_score,
            level=level,
        )

    def delete_keyphrase(self, keyphrase_id: str):
        return self._dataset.api._delete_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_field,
            alias=self._keyphrase_alias,
            keyphrase_id=keyphrase_id,
        )

    def bulk_update_keyphrases(self, updates: List[Union[Keyphrase, dict]]):
        updates_list = []
        for update in updates:
            if isinstance(update, Keyphrase):
                updates_list.append(asdict(update))
            elif isinstance(update, dict):
                updates_list.append(update)
        return self._dataset.api._bulk_update_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_field,
            alias=self._keyphrase_alias,
            updates=updates_list,
        )

    def list_keyphrases(self, page_size: int = 100, page: int = 1, sort: list = None):
        return self._dataset.api._list_keyphrase(
            dataset_id=self.dataset_id,
            field=self._keyphrase_field,
            alias=self._keyphrase_alias,
            page_size=page_size,
            page=page,
            sort=[] if sort is None else sort,
        )
