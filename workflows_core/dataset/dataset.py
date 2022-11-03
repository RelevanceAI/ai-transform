import time
import logging
import requests

from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Union

from workflows_core.api.api import API
from workflows_core.types import Filter, Schema
from workflows_core.errors import MaxRetriesError
from workflows_core.dataset.field import Field, VectorField
from workflows_core.utils.document import Document
from workflows_core.utils.document_list import DocumentList


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


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
        self, documents: Union[List[Document], DocumentList], *args, **kwargs
    ) -> Dict[str, Any]:
        if hasattr(documents, "to_json"):
            documents = documents.to_json()
        else:
            for index in range(len(documents)):
                if hasattr(documents[index], "to_json"):
                    documents[index] = documents[index].to_json()
        return self._api._bulk_insert(
            dataset_id=self._dataset_id, documents=documents, *args, **kwargs
        )

    def update_documents(
        self,
        documents: Union[List[Document], DocumentList],
        insert_date: bool = True,
        ingest_in_background: bool = True,
        update_schema: bool=True
    ) -> Dict[str, Any]:
        if hasattr(documents, "to_json"):
            documents = documents.to_json()
        else:
            for index in range(len(documents)):
                if hasattr(documents[index], "to_json"):
                    documents[index] = documents[index].to_json()
        return self._api._bulk_update(
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
        res["documents"] = DocumentList(res["documents"])
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
        retry_delay: int = 2
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
                logger.error(e)
                retry_count += 1
                time.sleep(retry_delay)

                if retry_count >= max_retries:
                    raise MaxRetriesError("max number of retries exceeded")

            except JSONDecodeError as e:
                logger.error(e)
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

    def insert_local_medias(self, file_paths: List[str]) -> List[str]:
        presigned_urls = self._api._get_file_upload_urls(
            self.dataset_id,
            files=file_paths,
        )
        urls = []
        for index, file_path in enumerate(file_paths):
            url = presigned_urls["files"][index]["url"]
            upload_url = presigned_urls["files"][index]["upload_url"]
            with open(file_path, "rb") as fn_byte:
                media_content = bytes(fn_byte.read())
            urls.append(url)
            response = self._api._upload_media(
                presigned_url=upload_url,
                media_content=media_content,
            )
            assert response.status_code == 200
        return urls
