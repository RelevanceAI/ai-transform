import requests
import time
import uuid
import logging
from functools import wraps
from typing import Any, Dict, List, Optional
from workflows_core.utils import document
from workflows_core.types import Credentials, FieldTransformer, Filter, Schema
from workflows_core import __version__

logger = logging.getLogger(__name__)

def get_response(response):
    # get a json response
    # if errors - print what the response contains
    try:
        return response.json()
    except Exception as e:
        logger.error({"error": e})
        try:
            # Log this somewhere if it errors
            logger.error(response.content)
        except Exception as no_content_e:
            # in case there's no content
            logger.error(no_content_e)
        finally:
            # we still want to raise the right error for retrying
            # continue to raise exception so that any retry logic still holds
            raise e

# We implement retry as a function for several reasons 
# first - we can get a

def retry(num_of_retries=3, timeout=5):
    """
    Allows the function to retry upon failure. 
    Args:
        num_of_retries: The number of times the function should retry
        timeout: The number of seconds to wait between each retry
    """
    num_of_retries = 3
    timeout = 2

    def _retry(func):
        @wraps(func)
        def function_wrapper(*args, **kwargs):
            for i in range(num_of_retries):
                try:
                    return func(*args, **kwargs)
                # Using general error to avoid any possible error dependencies.
                except ConnectionError as error:
                    time.sleep(timeout)
                    logger.debug("Retrying...")
                    if i == num_of_retries - 1:
                        raise error
                    continue
                break
        return function_wrapper
    return _retry

class API:
    def __init__(self, credentials: Credentials, job_id: str=None, name: str=None) -> None:
        self._credentials = credentials
        self._base_url = (
            f"https://api-{self._credentials.region}.stack.relevance.ai/latest"
        )
        self._headers = dict(
            Authorization=f"{self._credentials.project}:{self._credentials.api_key}",
            workflows_core_version=__version__,
        )
        if job_id is not None:
            self._headers.update(workflows_core_job_id=job_id)
        if name is not None:
            self._headers.update(workflows_core_name=name)

    @retry()
    def _list_datasets(self):
        response = requests.get(
            url=self._base_url + "/datasets/list", headers=self._headers
        )
        return get_response(response)

    @retry()
    def _create_dataset(
        self, dataset_id: str, schema: Optional[Schema] = None, upsert: bool = True
    ) -> Any:
        return requests.post(
            url=self._base_url + f"/datasets/create",
            headers=self._headers,
            json=dict(id=dataset_id, schema=schema, upsert=upsert),
        ).json()

    @retry()
    def _delete_dataset(self, dataset_id: str) -> Any:
        response = requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/delete", headers=self._headers
        )
        return get_response(response)

    @retry()
    def _get_schema(self, dataset_id: str) -> Schema:
        response = requests.get(
            url=self._base_url + f"/datasets/{dataset_id}/schema", headers=self._headers
        )
        return get_response(response)

    @retry()
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
        response = requests.post(
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
        )
        return get_response(response)

    @retry()
    def _bulk_update(
        self,
        dataset_id: str,
        documents: List[document.Document],
        insert_date: bool = True,
        ingest_in_background: bool = True,
        update_schema: bool=True
    ) -> Any:
        response = requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/documents/bulk_update",
            headers=self._headers,
            json=dict(
                updates=documents,
                insert_date=insert_date,
                ingest_in_background=ingest_in_background,
                update_schema=update_schema
            ),
        )
        return get_response(response)

    @retry()
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
        logger.debug(self._headers)
        response = requests.post(
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
        )
        return get_response(response)

    @retry()
    def _update_dataset_metadata(self, dataset_id: str, metadata: Dict[str, Any]):
        """
        Edit and add metadata about a dataset. Notably description, data source, etc
        """
        response = requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/metadata",
            headers=self._headers,
            json=dict(dataset_id=dataset_id, metadata=metadata),
        )
        return get_response(response)

    @retry()
    def _get_metadata(self, dataset_id: str) -> Dict[str, Any]:
        response = requests.get(
            url=self._base_url + f"/datasets/{dataset_id}/metadata",
            headers=self._headers,
        )
        return get_response(response)

    @retry()
    def _insert_centroids(
        self,
        dataset_id: str,
        cluster_centers: List[document.Document],
        vector_fields: List[str],
        alias: str,
    ):
        response = requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/cluster/centroids/insert",
            headers=self._headers,
            json=dict(
                dataset_id=dataset_id,
                cluster_centers=cluster_centers,
                vector_fields=vector_fields,
                alias=alias,
            ),
        )
        return get_response(response)

    @retry()
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
        response = requests.post(
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
        )
        return get_response(response)

    @retry()
    def _set_workflow_status(
        self,
        job_id: str,
        workflow_name: str,
        additional_information: str = "",
        metadata: Dict[str, Any] = None,
        status: str = "inprogress",
        send_email: bool = True,
        worker_number: int = None,
    ):
        # add edge case for API
        if job_id == "":
            return
        if status not in {"inprogress", "complete", "failed"}:
            raise ValueError(
                "state should be one of `['inprogress', 'complete', 'failed']`"
            )
        if metadata is None:
            metadata = {}
        if worker_number is None:
            return requests.post(
                url=self._base_url + f"/workflows/{job_id}/status",
                headers=self._headers,
                json=dict(
                    metadata=metadata,
                    status=status,
                    workflow_name=workflow_name,
                    additional_information=additional_information,
                    send_email=send_email,
                ),
            ).json()
        else:
            response = requests.post(
                url=self._base_url + f"/workflows/{job_id}/status",
                headers=self._headers,
                json=dict(
                    metadata=metadata,
                    status=status,
                    workflow_name=workflow_name,
                    additional_information=additional_information,
                    send_email=send_email,
                    worker_number=worker_number,
                ),
            )
            return get_response(response)

    @retry()
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
        response = requests.post(
            url=self._base_url
            + f"/datasets/{dataset_id}/field_children/{str(uuid.uuid4())}/update",
            headers=self._headers,
            json=dict(
                field=field,
                field_children=field_children,
                category=fieldchildren_id,
                metadata={} if metadata is None else metadata,
            ),
        )
        return get_response(response)

    @retry()
    def _get_health(self, dataset_id: str):
        response = requests.get(
            url=self._base_url + f"/datasets/{dataset_id}/monitor/health",
            headers=self._headers,
        )
        return get_response(response)

    @retry()
    def _get_workflow_status(self, job_id: str):
        response = requests.post(
            url=self._base_url + f"/workflows/{job_id}/get", headers=self._headers
        )
        return get_response(response)

    @retry()
    def _update_workflow_metadata(self, job_id: str, metadata: Dict[str, Any]):
        response = requests.post(
            url=self._base_url + f"/workflows/{job_id}/metadata",
            headers=self._headers,
            json=dict(metadata=metadata),
        )
        return get_response(response)

    @retry()
    def _get_file_upload_urls(self, dataset_id: str, files: List[str]):
        response = requests.post(
            url=self._base_url + f"/datasets/{dataset_id}/get_file_upload_urls",
            headers=self._headers,
            json=dict(files=files),
        )
        return get_response(response)

    @retry()
    def _upload_media(self, presigned_url: str, media_content: bytes):
        response = requests.put(presigned_url, data=media_content)
        return response

    @retry()
    def _trigger(
        self,
        dataset_id: str,
        params: dict,
        workflow_id: str,
        notebook_path: str = None,
        instance_type: str = None,
        host_type: str = None,
    ):
        """
        trigger a workflow
        """
        return requests.post(
            url=self._base_url + f"/workflows/trigger",
            headers=self._headers,
            json=dict(
                params=params,
                dataset_id=dataset_id,
                workflow_id=workflow_id,
                notebook_path=notebook_path,
                instance_type=instance_type,
                host_type=host_type
            ),
        ).json()
    
    @retry()
    def _progress(
        self,
        workflow_id: str,
        worker_number: int=0,
        step=0,
        n_processed: int=0,
        n_total: int=0
    ):
        """
        Tracks Workflow Progress
        """
        params = dict(
            worker_number=worker_number,
            step=step,
            n_processed=n_processed,
            n_total=n_total
        )
        # print the params to see what is happening here
        logger.debug("adding progress...")
        logger.debug(params)
        response = requests.post(
            url=self._base_url + f"/workflows/{workflow_id}/progress",
            headers=self._headers,
            json=params
        )
        return get_response(
            response
        )
