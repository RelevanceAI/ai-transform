import uuid
import requests

from typing import Any, Dict, List, Optional

from workflows_core.utils import document
from workflows_core.types import Credentials, FieldTransformer, Filter, Schema


class API:
    def __init__(self, credentials: Credentials) -> None:
        self._credentials = credentials
        self._base_url = (
            f"https://api-{self._credentials.region}.stack.relevance.ai/latest"
        )
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
        documents: List[document.Document],
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
        documents: List[document.Document],
        insert_date: bool = True,
        ingest_in_background: bool = True,
    ) -> Any:
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
        worker_number: int = 0,
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
                worker_number=worker_number,
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
        ).json()

    def _insert_centroids(
        self,
        dataset_id: str,
        cluster_centers: List[document.Document],
        vector_fields: List[str],
        alias: str,
    ):
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/cluster/centroids/insert",
            headers=self._headers,
            json=dict(
                dataset_id=dataset_id,
                cluster_centers=cluster_centers,
                vector_fields=vector_fields,
                alias=alias,
            ),
        ).json()

    def _get_centroids(
        self,
        dataset_id: str,
        vector_fields: List[str],
        alias: str,
        page_size: int = 5,
        page: int = 1,
        cluster_ids: Optional[List] = None,
        include_vector: bool = False,
    ):
        return requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/cluster/centroids/documents",
            headers=self._headers,
            json=dict(
                cluster_ids=[] if cluster_ids is None else cluster_ids,
                vector_fields=vector_fields,
                alias=alias,
                page_size=page_size,
                page=page,
                include_vector=include_vector,
            ),
        ).json()

    def _set_workflow_status(
        self,
        workflow_id: str,
        metadata: Dict[str, Any],
        workflow_name: str,
        additional_information: str = "",
        status: str = "inprogress",
        send_email: bool = True,
    ):
        if status not in {"inprogress", "complete", "failed"}:
            raise ValueError(
                "state should be one of `['inprogress', 'complete', 'failed']`"
            )
        return requests.post(
            url=self._base_url + f"/workflows/{workflow_id}/status",
            headers=self._headers,
            json=dict(
                metadata={},  # TODO: why is this empty
                status=status,
                workflow_name=workflow_name,
                additional_information=additional_information,
                send_email=send_email,
            ),
        ).json()

    def _set_field_children(
        self,
        dataset_id: str,
        fieldchildren_id: str,
        field: str,
        field_children: List[str],
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        fieldchildren_id: The name of the workflow or operation

        field: the input field of the operation

        field_children: a list of output fields, taken from the most nested level
        i.e. ["_sentiment_.text_field.alias"]

        metadata: extra parameters associated with operation
        i.e. n_clusters, n_init, softmax_temperature, etc...
        """
        return requests.post(
            url=self._base_url
            + f"/datasets/{dataset_id}/field_children/{str(uuid.uuid4())}/update",
            headers=self._headers,
            json=dict(
                field=field,
                field_children=field_children,
                category=fieldchildren_id,
                metadata={} if metadata is None else metadata,
            ),
        ).json()

    def _get_health(self, dataset_id: str):
        return requests.get(
            url=self._base_url + f"/datasets/{dataset_id}/monitor/health",
            headers=self._headers,
        ).json()

    def _get_workflow_status(self, workflow_id: str):
        return requests.post(
            url=self._base_url + f"/workflows/{workflow_id}/get",
            headers=self._headers,
        ).json()
