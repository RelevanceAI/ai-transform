import time
import logging

from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Union

from ai_transform.api.api import API
from ai_transform.api.helpers import process_token
from ai_transform.types import Filter, Schema, GroupBy, Metric
from ai_transform.errors import MaxRetriesError
from ai_transform.dataset.field import Field, KeyphraseField, ClusterField
from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList
from ai_transform.logger import ic
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class Dataset:
    def __init__(self, api: API, dataset_id: str):
        self._api = api
        self._dataset_id = dataset_id

    @property
    def api(self) -> API:
        return self._api

    @classmethod
    def from_details(cls: "Dataset", dataset_id: str, token: str) -> "Dataset":
        return cls(API(process_token(token)), dataset_id)

    @property
    def token(self):
        return self.api.credentials.token

    def __getitem__(self, index: str) -> Field:
        if isinstance(index, str):
            if index.startswith(("_cluster_", "_cluster_otm_")) or "_cluster_id_" in index:
                return ClusterField(dataset=self, field=index)
            elif "_keyphrase_" in index:
                return KeyphraseField(dataset=self, field=index)
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
        return self.api._get_schema(self._dataset_id)

    def health(self) -> Dict[str, Any]:
        return self.api._get_health(self._dataset_id)

    def create(self):
        return self.api._create_dataset(self._dataset_id)

    def delete(self):
        return self.api._delete_dataset(self._dataset_id)

    def bulk_insert(
        self, documents: Union[List[Document], DocumentList], insert_chunksize: int = 20, max_workers: int = 2, **kwargs
    ):
        def chunk_documents_with_kwargs(documents):
            for i in range(len(documents) // insert_chunksize + 1):
                yield {"documents": documents[i * insert_chunksize : (i + 1) * insert_chunksize], **kwargs}

        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = executor.map(lambda kw: self.insert_documents(**kw), chunk_documents_with_kwargs(documents))

        results = {"inserted": 0, "failed_documents": []}
        for result in futures:
            results["inserted"] += result["inserted"]
            results["failed_documents"] += result["failed_documents"]

        return results

    def insert_documents(self, documents: Union[List[Document], DocumentList], *args, **kwargs) -> Dict[str, Any]:
        if hasattr(documents, "to_json"):
            documents = documents.to_json()
        else:
            for index in range(len(documents)):
                if hasattr(documents[index], "to_json"):
                    documents[index] = documents[index].to_json()
        return self.api._bulk_insert(dataset_id=self._dataset_id, documents=documents, *args, **kwargs)

    def update_documents(
        self,
        documents: Union[List[Document], DocumentList],
        insert_date: bool = True,
        ingest_in_background: bool = True,
        update_schema: bool = True,
    ) -> Dict[str, Any]:
        if hasattr(documents, "to_json"):
            documents = documents.to_json()
        else:
            for index in range(len(documents)):
                if hasattr(documents[index], "to_json"):
                    documents[index] = documents[index].to_json()
        return self.api._bulk_update(
            dataset_id=self._dataset_id,
            documents=documents,
            insert_date=insert_date,
            ingest_in_background=ingest_in_background,
            update_schema=update_schema,
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
        res = self.api._get_where(
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
        res["documents"] = DocumentList(res["documents"])
        return res

    def delete_documents(self, filters: Optional[List[Filter]]) -> Dict[str, Any]:
        res = self.api._delete_where(dataset_id=self._dataset_id, filters=filters)
        return res

    def get_all_documents(
        self,
        page_size: int = 64,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        sort: Optional[list] = None,
        include_vector: bool = True,
        random_state: int = 0,
        is_random: bool = False,
        after_id: Optional[List] = None,
        worker_number: int = 0,
        max_retries: int = 3,
        retry_delay: int = 2,
    ) -> Dict[str, Any]:
        documents = []
        retry_count = 0
        while True:
            try:
                chunk = self.get_documents(
                    page_size=page_size,
                    filters=filters,
                    select_fields=select_fields,
                    after_id=after_id,
                    worker_number=worker_number,
                    sort=sort,
                    include_vector=include_vector,
                    random_state=random_state,
                    is_random=is_random,
                )
            except ConnectionError as e:
                ic(e)
                retry_count += 1
                time.sleep(retry_delay)

                if retry_count >= max_retries:
                    raise MaxRetriesError("max number of retries exceeded")

            except JSONDecodeError as e:
                ic(e)
                retry_count += 1
                time.sleep(retry_delay)

                if retry_count >= max_retries:
                    raise MaxRetriesError("max number of retries exceeded")

            else:
                after_id = chunk["after_id"]
                if not chunk["documents"]:
                    break
                documents += chunk["documents"]
                retry_count = 0

        res = {}
        res["documents"] = DocumentList(documents)
        return res

    def len(self, *args, **kwargs):
        """
        Get length of dataset, usually used with filters
        """
        return self.api._get_where(dataset_id=self._dataset_id, page_size=1, *args, **kwargs)["count"]

    def insert_metadata(self, metadata: Dict[str, Any]):
        return self.api._update_dataset_metadata(dataset_id=self._dataset_id, metadata=metadata)

    def update_metadata(self, metadata: Dict[str, Any]):
        old_metadata: dict = self.get_metadata()["results"]

        def merge_dicts(dict1, dict2):
            """Recursively merges dict2 into dict1"""
            if not isinstance(dict1, dict) or not isinstance(dict2, dict):
                return dict2
            for k in dict2:
                if k in dict1:
                    dict1[k] = merge_dicts(dict1[k], dict2[k])
                else:
                    dict1[k] = dict2[k]
            return dict1

        return self.api._update_dataset_metadata(
            dataset_id=self._dataset_id, metadata=merge_dicts(old_metadata, metadata)
        )

    def get_metadata(self) -> Dict[str, Any]:
        return self.api._get_metadata(dataset_id=self._dataset_id)

    def insert_local_medias(self, file_paths: List[str]) -> List[str]:
        presigned_urls = self.api._get_file_upload_urls(self.dataset_id, files=file_paths)
        urls = []
        for index, file_path in enumerate(file_paths):
            url = presigned_urls["files"][index]["url"]
            upload_url = presigned_urls["files"][index]["upload_url"]
            with open(file_path, "rb") as fn_byte:
                media_content = bytes(fn_byte.read())
            urls.append(url)
            response = self.api._upload_media(presigned_url=upload_url, media_content=media_content)
            assert response.status_code == 200
        return urls

    def facets(self, fields: List[str], data_interval: str = "monthly", page_size: int = 10000, asc: bool = False):
        return self.api._facets(
            dataset_id=self.dataset_id, fields=fields, data_interval=data_interval, page_size=page_size, asc=asc
        )

    def set_field_children(
        self,
        fieldchildren_id: str,
        field: str,
        field_children: List[str],
        metadata: Optional[Dict[str, Any]] = None,
        recursive: bool = True,
    ):
        self.api._set_field_children(
            dataset_id=self._dataset_id,
            fieldchildren_id=fieldchildren_id,
            field=field,
            field_children=field_children,
            metadata=metadata,
        )
        if recursive:
            parent_fields = self[field].list_field_parents()
            for parent_field in parent_fields:
                self[parent_field].add_field_children(
                    field_children=field_children, fieldchildren_id=fieldchildren_id, recursive=recursive
                )

    def list_field_children(self, page: int = 1, page_size: int = 10000, sort=None):
        return self.api._list_field_children(dataset_id=self._dataset_id, page=page, page_size=page_size, sort=sort)

    def delete_field_children(self, fieldchildren_id: str):
        return self.api._delete_field_children(dataset_id=self._dataset_id, fieldchildren_id=fieldchildren_id)

    def aggregate(
        self,
        page_size: str = 20,
        page: str = 1,
        asc: str = False,
        groupby: List[GroupBy] = None,
        metrics: List[Metric] = None,
        sort: List[Any] = None,
        dataset_ids: List[str] = None,
        filters: List[Filter] = None,
    ):
        return self.api._aggregate(
            dataset_id=self._dataset_id,
            page_size=page_size,
            page=page,
            asc=asc,
            aggregation_query=dict(
                groupby=[] if groupby is None else groupby,
                metrics=[] if metrics is None else metrics,
                sort=[] if sort is None else sort,
            ),
            dataset_ids=dataset_ids,
            filters=filters,
        )

    def get_settings(self):
        return self.api._get_dataset_settings(self.dataset_id)

    def update_settings(self, settings: Dict[str, Any]):
        return self.api._upsert_dataset_settings(dataset_id=self.dataset_id, settings=settings)
