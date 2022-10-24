from typing import Any, Dict, List, Optional

from workflows_core.api.api import API
from workflows_core.types import Filter, Schema
from workflows_core.utils import document

from workflows_core.utils.json_encoder import json_encoder
from workflows_core.dataset.field import Field, VectorField


class Dataset:
    def __init__(self, api: API, dataset_id: str):
        self._api = api
        self._dataset_id = dataset_id

    def __getitem__(self, index: str) -> Field:
        if isinstance(index, str):
            if "_vector_" in index:
                return VectorField(dataset=self, field=index)
            else:
                return Field(dataset=self, field=index)
        else:
            raise NotImplementedError("index must of type `str` (field in dataset)")

    def __len__(self, *args, **kwargs) -> int:
        return self.get_documents(1, *args, **kwargs)["count"]

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    @property
    def schema(self) -> Schema:
        return self._api._get_schema(self._dataset_id)

    @property
    def api(self) -> API:
        return self._api

    def health(self) -> Dict[str, Any]:
        return self._api._get_health(self._dataset_id)

    def create(self):
        return self._api._create_dataset(self._dataset_id)

    def delete(self):
        return self._api._delete_dataset(self._dataset_id)

    def insert_documents(
        self,
        documents: List[document.Document],
        use_json_encoder: bool = True,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        for index, document in enumerate(documents):
            if hasattr(document, "to_dict"):
                documents[index] = document.to_dict()
        if use_json_encoder:
            documents = json_encoder(documents)
        return self._api._bulk_insert(
            dataset_id=self._dataset_id, documents=documents, *args, **kwargs
        )

    def update_documents(
        self,
        documents: List[document.Document],
        use_json_encoder: bool = True,
        insert_date: bool = True,
        ingest_in_background: bool = True,
    ) -> Dict[str, Any]:
        for index, document in enumerate(documents):
            if hasattr(document, "to_dict"):
                documents[index] = document.to_dict()
        if use_json_encoder:
            documents = json_encoder(documents)
        return self._api._bulk_update(
            dataset_id=self._dataset_id,
            documents=documents,
            insert_date=insert_date,
            ingest_in_background=ingest_in_background,
        )

    def get_documents(
        self,
        page_size: int,
        filters: Optional[List[Filter]] = None,
        sort: Optional[list] = None,
        select_fields: Optional[List[str]] = None,
        include_vector: bool = True,
        random_state: int = 0,
        is_random: bool = False,
        after_id: Optional[List] = None,
        worker_number: int = 0,
    ) -> Dict[str, Any]:
        res = self._api._get_where(
            dataset_id=self._dataset_id,
            page_size=page_size,
            filters=filters,
            sort=sort,
            select_fields=select_fields,
            include_vector=include_vector,
            random_state=random_state,
            is_random=is_random,
            after_id=after_id,
            worker_number=worker_number,
        )
        res["documents"] = [document.Document(d) for d in res["documents"]]
        return res

    def len(self, *args, **kwargs):
        """
        Get length of dataset, usually used with filters
        """
        return self._api._get_where(
            dataset_id=self._dataset_id, page_size=1, *args, **kwargs
        )["count"]

    def insert_metadata(self, metadata: Dict[str, Any]):
        return self._api._update_dataset_metadata(
            dataset_id=self._dataset_id,
            metadata=metadata,
        )

    def update_metadata(self, metadata: Dict[str, Any]):
        old_metadata = self.get_metadata()["results"]
        metadata.update(old_metadata)
        return self._api._update_dataset_metadata(
            dataset_id=self._dataset_id,
            metadata=metadata,
        )

    def get_metadata(self) -> Dict[str, Any]:
        return self._api._get_metadata(dataset_id=self._dataset_id)
