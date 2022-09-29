import requests

from typing import Any, Dict, List, Optional

from slim.types import Credentials, Document, FieldTransformer, Filter, Schema


class API:
    def __init__(self, credentials: Credentials) -> None:
        self._credentials = credentials
        self._base_url = f"https://api.{self._credentials.region}.relevance.ai/latest"
        self._headers = dict(
            Authorization=f"{self._credentials.project}:{self._credentials.api_key}"
        )

    def _list_datasets(self):
        return requests.get(
            url=self._base_url + "/datasets/list", headers=self._headers
        ).json()

    def _create_dataset(
        self, dataset_id: str, schema: Optional[Schema] = None, upsert: bool = True
    ) -> Any:
        return requests.post(
            url=self._base_url + f"/datasets/create",
            headers=self._headers,
            json=dict(
                id=dataset_id,
                schema=schema,
                upsert=upsert,
            ),
        ).json()

    def _delete_dataset(self, dataset_id: str) -> Any:
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/delete",
            headers=self._headers,
        ).json()

    def _get_schema(self, dataset_id: str) -> Schema:
        return requests.get(
            url=self._base_url + f"/datasets/{dataset_id}/schema",
            headers=self._headers,
        ).json()

    def _bulk_insert(
        self,
        dataset_id: str,
        documents: List[Document],
        insert_date: bool = True,
        overwrite: bool = True,
        update_schema: bool = True,
        wait_for_update: bool = True,
        field_transformers: List[FieldTransformer] = None,
        ingest_in_background: bool = False,
    ) -> Any:
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/documents/bulk_insert",
            headers=self._headers,
            json=dict(
                documents=documents,
                insert_date=insert_date,
                overwrite=overwrite,
                update_schema=update_schema,
                field_transformers=[]
                if field_transformers is None
                else field_transformers,
                ingest_in_background=ingest_in_background,
                wait_for_update=wait_for_update,
            ),
        ).json()

    def _bulk_update(
        self,
        dataset_id: str,
        documents: List[Document],
        insert_date: bool = True,
        ingest_in_background: bool = True,
    ):
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/documents/bulk_update",
            headers=self._headers,
            json=dict(
                updates=documents,
                insert_date=insert_date,
                ingest_in_background=ingest_in_background,
            ),
        ).json()

    def _get_where(
        self,
        dataset_id: str,
        page_size: int,
        filters: Optional[List[Filter]] = None,
        sort: Optional[list] = None,
        select_fields: Optional[List[str]] = None,
        include_vector: bool = True,
        random_state: int = 0,
        is_random: bool = False,
        after_id: Optional[List] = None,
    ):
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/documents/get_where",
            headers=self._headers,
            json=dict(
                select_fields=[] if select_fields is None else select_fields,
                page_size=page_size,
                sort=[] if sort is None else sort,
                include_vector=include_vector,
                filters=[] if filters is None else filters,
                random_state=random_state,
                is_random=is_random,
                after_id=[] if after_id is None else after_id,
            ),
        ).json()

    def _update_metadata(self, dataset_id: str, metadata: Dict[str, Any]):
        """
        Edit and add metadata about a dataset. Notably description, data source, etc
        """
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/metadata",
            headers=self._headers,
            json=dict(
                dataset_id=dataset_id,
                metadata=metadata,
            ),
        ).json()

    def _get_metadata(self, dataset_id: str) -> Dict[str, Any]:
        return requests.get(
            url=self._base_url + f"/datasets/{dataset_id}/metadata",
            headers=self._headers,
        )
