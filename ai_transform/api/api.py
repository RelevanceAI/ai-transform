import os
import time
import uuid
import logging
import requests

from requests.models import Response
from functools import wraps

from typing import Any, Dict, List, Optional, Literal, Callable

from ai_transform.logger import format_logging_info
from ai_transform.utils import document
from ai_transform.types import Credentials, FieldTransformer, Filter, Schema
from ai_transform.api.wrappers import request_wrapper

from ai_transform import __version__
from ai_transform.logger import ic


LOG_REQUESTS = bool(os.getenv("LOG_REQUESTS"))
if LOG_REQUESTS:
    # Get the current Unix timestamp as a string
    timestamp = str(int(time.time()))

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(f"{timestamp}_request_logs.log")],
    )


def to_curl(request: requests.PreparedRequest):
    command = "curl -X {method} '{url}'".format(method=request.method, url=request.url)

    for header, value in request.headers.items():
        if header.lower() == "authorization":
            value = "MASKED"
        command += " -H '{header}: {value}'".format(header=header, value=value)

    if request.body:
        command += " -d '{data}'".format(data=request.body)

    return command


def log_request(request: requests.PreparedRequest):
    curl_command = to_curl(request)
    logging.debug(curl_command)


def log_response(response: requests.Response):
    logging.debug("Response Headers: %s", response.headers)
    logging.debug("Response Content: %s\n", response.text)


def get_response(response: requests.Response) -> Dict[str, Any]:
    # get a json response
    # if errors - print what the response contains
    if response.status_code == 200:
        try:
            return response.json()
        except Exception as e:
            ic(e)
            ic(format_logging_info({"x-trace-id": response.headers["x-trace-id"]}))
            raise e
    else:
        datum = {"error": response.content.decode("utf-8")}
        if "x-trace-id" in response.headers:
            datum["x-trace-id"] = response.headers["x-trace-id"]

        try:
            # Log this somewhere if it errors
            ic(format_logging_info(datum))
        except Exception as no_content_e:
            # in case there's no content
            ic(no_content_e)
            # we still want to raise the right error for retrying
            # continue to raise exception so that any retry logic still holds
            raise no_content_e


# We implement retry as a function for several reasons
# first - we can get a
def retry(num_of_retries: int = 3, timeout: int = 30, retry_func: Callable = None):
    """
    Allows the function to retry upon failure.
    Args:
        num_of_retries: The number of times the function should retry
        timeout: The number of seconds to wait between each retry
    """

    def _retry(func):
        @wraps(func)
        def function_wrapper(*args, **kwargs):
            return request_wrapper(
                func,
                args,
                kwargs,
                num_retries=num_of_retries,
                timeout=timeout,
                exponential_backoff=2,
                retry_func=retry_func,
            )

        return function_wrapper

    return _retry


class API:
    def __init__(self, credentials: Credentials, job_id: str = None, name: str = None) -> None:
        self._credentials = credentials
        self._base_url = f"https://api-{self.credentials.region}.stack.tryrelevance.com/latest"
        self._headers = dict(
            Authorization=f"{self.credentials.project}:{self.credentials.api_key}", ai_transform_version=__version__
        )
        if job_id is not None:
            self.headers.update(ai_transform_job_id=job_id)
        if name is not None:
            self.headers.update(ai_transform_name=name)

        self.session = requests.Session()

    @property
    def credentials(self) -> Credentials:
        return self._credentials

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @retry()
    def _request(self, method: Literal["GET", "POST"], suffix: str, *args, **kwargs) -> Response:
        request = requests.Request(method=method, url=self.base_url + suffix, headers=self.headers, *args, **kwargs)
        prepared_request = request.prepare()

        if LOG_REQUESTS:
            log_request(prepared_request)

        response = self.session.send(prepared_request)

        if LOG_REQUESTS:
            log_response(response)

        return response

    def get(self, suffix: str, *args, **kwargs) -> Response:
        return self._request(method="GET", suffix=suffix, *args, **kwargs)

    def post(self, suffix: str, *args, **kwargs) -> Response:
        return self._request(method="POST", suffix=suffix, *args, **kwargs)

    def _list_datasets(self):
        response = self.get(suffix="/datasets/list")
        return get_response(response)

    def _create_dataset(
        self, dataset_id: str, schema: Optional[Schema] = None, upsert: bool = True, expire: bool = False
    ) -> Any:
        response = self.post(
            suffix=f"/datasets/create", json=dict(id=dataset_id, schema=schema, upsert=upsert, expire=expire)
        )
        return get_response(response)

    def _delete_dataset(self, dataset_id: str) -> Any:
        response = self.post(suffix=f"/datasets/{dataset_id}/delete")
        return get_response(response)

    def _get_schema(self, dataset_id: str) -> Schema:
        response = self.get(suffix=f"/datasets/{dataset_id}/schema")
        return get_response(response)

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
        response = self.post(
            suffix=f"/datasets/{dataset_id}/documents/bulk_insert",
            json=dict(
                documents=documents,
                insert_date=insert_date,
                overwrite=overwrite,
                update_schema=update_schema,
                field_transformers=[] if field_transformers is None else field_transformers,
                ingest_in_background=ingest_in_background,
                wait_for_update=wait_for_update,
            ),
        )
        return get_response(response)

    def _bulk_update(
        self,
        dataset_id: str,
        documents: List[document.Document],
        insert_date: bool = True,
        ingest_in_background: bool = True,
        update_schema: bool = True,
    ) -> Any:
        response = self.post(
            suffix=f"/datasets/{dataset_id}/documents/bulk_update",
            json=dict(
                updates=documents,
                insert_date=insert_date,
                ingest_in_background=ingest_in_background,
                update_schema=update_schema,
            ),
        )
        return get_response(response)

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
        response = self.post(
            suffix=f"/datasets/{dataset_id}/documents/get_where",
            json=dict(
                select_fields=[] if select_fields is None else select_fields,
                page_size=min(9999, page_size),
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

    def _delete_where(self, dataset_id: str, filters: Optional[List[Filter]] = None):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/documents/delete_where",
            json=dict(filters=[] if filters is None else filters),
        )
        return get_response(response)

    def _update_dataset_metadata(self, dataset_id: str, metadata: Dict[str, Any]):
        """
        Edit and add metadata about a dataset. Notably description, data source, etc
        """
        response = self.post(
            suffix=f"/datasets/{dataset_id}/metadata", json=dict(dataset_id=dataset_id, metadata=metadata)
        )
        return get_response(response)

    def _get_metadata(self, dataset_id: str) -> Dict[str, Any]:
        response = self.get(suffix=f"/datasets/{dataset_id}/metadata")
        return get_response(response)

    def _insert_centroids(
        self, dataset_id: str, cluster_centers: List[document.Document], vector_fields: List[str], alias: str
    ):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/cluster/centroids/insert",
            json=dict(dataset_id=dataset_id, cluster_centers=cluster_centers, vector_fields=vector_fields, alias=alias),
        )
        return get_response(response)

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
        response = self.post(
            suffix=f"/datasets/{dataset_id}/cluster/centroids/documents",
            json=dict(
                cluster_ids=[] if cluster_ids is None else cluster_ids,
                vector_fields=vector_fields,
                alias=alias,
                page_size=min(9999, page_size),
                page=page,
                include_vector=include_vector,
            ),
        )
        return get_response(response)

    def _set_workflow_status(
        self,
        job_id: str,
        workflow_name: str,
        additional_information: str = "",
        metadata: Dict[str, Any] = None,
        status: str = "inprogress",
        send_email: bool = True,
        worker_number: int = None,
        output: Dict[str, Any] = None,
        email: Dict[str, Any] = None,
        user_errors: str = None,
    ):
        # add edge case for API
        if job_id == "":
            return
        if status not in {"inprogress", "complete", "failed"}:
            raise ValueError("state should be one of `['inprogress', 'complete', 'failed']`")
        parameters = dict(
            status=status,
            workflow_name=workflow_name,
            additional_information=additional_information,
            send_email=send_email,
        )
        # metadata can't be an empty dictionary as it overwrites
        if metadata is not None and metadata != {}:
            parameters["metadata"] = metadata

        if worker_number is not None:
            parameters["worker_number"] = worker_number

        if output:
            parameters["output"] = output

        if user_errors:
            parameters["user_errors"] = user_errors

        if email:
            # adding some assertions here for better developer experience
            assert isinstance(email, dict)
            assert "secondary_cta" in email
            assert "url" in email["secondary_cta"]
            assert "text" in email["secondary_cta"]

            parameters["email"] = email

        ic(parameters)

        response = self.post(suffix=f"/workflows/{job_id}/status", json=parameters)
        return get_response(response)

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
        params = dict(
            field=field,
            field_children=field_children,
            category=fieldchildren_id,
            metadata={} if metadata is None else metadata,
        )
        ic(params)
        response = self.post(suffix=f"/datasets/{dataset_id}/field_children/{str(uuid.uuid4())}/update", json=params)
        return get_response(response)

    def _delete_field_children(self, dataset_id: str, fieldchildren_id: str):
        response = self.post(suffix=f"/datasets/{dataset_id}/field_children/{fieldchildren_id}/delete")
        return get_response(response)

    def _list_field_children(self, dataset_id: str, page: int = 1, page_size: int = 10000, sort=None):
        parameters = {"page": page, "page_size": page_size}

        if sort:
            parameters["sort"] = sort

        response = self.post(suffix=f"/datasets/{dataset_id}/field_children/list", json=parameters)
        return get_response(response)

    def _get_health(self, dataset_id: str):
        response = self.get(suffix=f"/datasets/{dataset_id}/monitor/health")
        return get_response(response)

    def _get_workflow_status(self, job_id: str):
        response = self.post(suffix=f"/workflows/{job_id}/get")
        return get_response(response)

    def _update_workflow_metadata(self, job_id: str, metadata: Dict[str, Any]):
        response = self.post(suffix=f"/workflows/{job_id}/metadata", json=dict(metadata=metadata))
        return get_response(response)

    def _get_file_upload_urls(self, dataset_id: str, files: List[str]):
        response = self.post(suffix=f"/datasets/{dataset_id}/get_file_upload_urls", json=dict(files=files))
        return get_response(response)

    def _get_temp_file_upload_url(self):
        """Use this for temporary file uploads.
        returns: {'download_url': ..., 'upload_url': ...}
        """
        response = self.post(suffix=f"/services/get_temporary_file_upload_url")
        return get_response(response)

    def _upload_temporary_media(self, presigned_url: str, media_content: bytes):
        return requests.put(presigned_url, headers={"x-amz-tagging": "Expire=true"}, data=media_content)

    def _upload_media(self, presigned_url: str, media_content: bytes):
        # dont use get response since response cannot be json decoded
        return requests.put(presigned_url, data=media_content)

    def _trigger(
        self,
        dataset_id: str,
        params: dict,
        workflow_id: str,
        notebook_path: str = None,
        instance_type: str = None,
        host_type: str = None,
        version: str = "production_version",
    ):
        """
        trigger a workflow
        """

        data = dict(params=params, dataset_id=dataset_id, workflow_id=workflow_id, version=version)
        if notebook_path is not None:
            data["notebook_path"] = notebook_path
        if instance_type is not None:
            data["instance_type"] = instance_type
        if host_type is not None:
            data["host_type"] = host_type
        return self.post(suffix=f"/workflows/trigger", json=data).json()

    def _trigger_polling_workflow(
        self,
        dataset_id: str,
        input_field: str,
        output_field: str,
        job_id: str,
        workflow_name: str,
        # set 95% coverage in case of edge cases like workflow only working
        # on certain proportion of dataset
        minimum_coverage: float = 0.95,
        max_time: float = 120,
        sleep_timer: float = 10,
        workflow_id="poll",
        version="production_version",
    ):
        """
        Trigger the polling workflow
        """
        return self._trigger(
            dataset_id=dataset_id,
            params=dict(
                dataset_id=dataset_id,
                input_field=input_field,
                output_field=output_field,
                minimum_coverage=minimum_coverage,
                max_time=max_time,
                sleep_timer=sleep_timer,
                parent_job_id=job_id,
                parent_job_name=workflow_name,
            ),
            workflow_id=workflow_id,
            version=version,
            notebook_path="",
            instance_type="batch",
            host_type="batch",
        )

    def _update_workflow_progress(
        self,
        workflow_id: str,
        worker_number: int = 0,
        step: str = "Workflow",
        n_processed: int = 0,
        n_total: int = 0,
        n_processed_pricing: Optional[int] = None,  # optional parameter
    ):
        """
        Tracks Workflow Progress
        """
        if worker_number is None:
            worker_number = 0

        if n_processed_pricing:
            params["n_processed_pricing "] = n_processed_pricing

        params = dict(worker_number=worker_number, step=step, n_processed=n_processed, n_total=n_total)

        ic("adding progress...")
        ic(params)

        response = self.post(suffix=f"/workflows/{workflow_id}/progress", json=params)
        return get_response(response)

    def _update_workflow_pricing(
        self,
        workflow_id: str,
        worker_number: int = 0,
        step: str = "Workflow",
        n_processed_pricing: Optional[int] = None,
    ):
        """
        Pricing endpoint is really part of progress endpoint but this is being
        abstracted away for now due to the fact that the pricing is actually
        something outside of progress.
        """
        if worker_number is None:
            worker_number = 0

        params = dict(worker_number=worker_number, step=step, n_processed_pricing=n_processed_pricing)
        ic("adding progress...")
        ic(params)
        response = self.post(suffix=f"/workflows/{workflow_id}/progress", json=params)
        return get_response(response)

    def _append_tags(self, dataset_id: str, field: str, tags_to_add: List[str], filters: List[Filter]):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/tags/append",
            json=dict(field=field, tags_to_add=tags_to_add, filters=filters),
        )
        return get_response(response)

    def _delete_tags(self, dataset_id: str, field: str, tags_to_delete: List[str], filters: List[Filter]):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/tags/delete",
            json=dict(field=field, tags_to_delete=tags_to_delete, filters=filters),
        )
        return get_response(response)

    def _merge_tags(self, dataset_id: str, field: str, tags_to_merge: Dict[str, str], filters: List[Filter]):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/tags/merge",
            json=dict(field=field, tags_to_merge=tags_to_merge, filters=filters),
        )
        return get_response(response)

    def _bulk_update_keyphrase(self, dataset_id: str, field: str, alias: str, updates: List):
        """
        Update keyphrases
        """
        response = self.post(
            suffix=f"/datasets/{dataset_id}/fields/{field}.{alias}/keyphrase/bulk_update", json=dict(updates=updates)
        )
        return get_response(response)

    def _bulk_delete_keyphrase(self, dataset_id: str, field: str, alias: str, ids: List[str]):
        """
        Update keyphrases
        """
        response = self.post(
            suffix=f"/datasets/{dataset_id}/fields/{field}.{alias}/keyphrase/bulk_delete", json=dict(ids=ids)
        )
        return get_response(response)

    def _get_keyphrase(self, dataset_id: str, field: str, alias: str, keyphrase_id: str):
        """
        Get keyphrase
        """
        if isinstance(keyphrase_id, str) and keyphrase_id != "":
            response = self.get(suffix=f"/datasets/{dataset_id}/fields/{field}.{alias}/keyphrase/{keyphrase_id}/get")
            return get_response(response)

    def _delete_keyphrase(self, dataset_id: str, field: str, keyphrase_id: str, alias: str):
        """
        Deleting Keyphrases
        """
        response = self.post(suffix=f"/datasets/{dataset_id}/fields/{field}.{alias}/keyphrase/{keyphrase_id}/delete")
        return get_response(response)

    def _update_keyphrase(
        self,
        dataset_id: str,
        field: str,
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
        # missing update contents here?
        """
        Update keyphrases
        """
        params = {
            "_id": keyphrase_id,
            "text": keyphrase,
            "frequency": frequency,
            "keyphrase_score": keyphrase_score,
            "level": level,
        }
        if ancestors is not None:
            params["ancestors"] = ancestors
        if parents is not None:
            params["parents"] = parents
        if metadata is not None:
            params["metadata"] = metadata
        response = self.post(
            suffix=f"/datasets/{dataset_id}/fields/{field}.{alias}/keyphrase/{keyphrase_id}/update", json=params
        )
        return get_response(response)

    def _list_keyphrase(
        self, dataset_id: str, field: str, alias: str, page: int = 0, page_size: int = 100, sort: list = None
    ):
        """
        List keyphrases
        """
        params = {"page": page, "page_size": min(9999, page_size)}
        if sort is not None:
            params["sort"] = sort
        response = self.post(suffix=f"/datasets/{dataset_id}/fields/{field}.{alias}/keyphrase/list", json=params)
        return get_response(response)

    def _facets(
        self,
        dataset_id: str,
        fields: List[str],
        data_interval: str = "monthly",
        page_size: int = 1000,
        asc: bool = False,
    ):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/facets",
            json=dict(fields=fields, data_interval=data_interval, page_size=min(9999, page_size), asc=asc),
        )
        return get_response(response)

    def _upsert_dataset_settings(self, dataset_id: str, settings: Optional[Dict[str, Any]] = None):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/settings", json=dict(settings={} if settings is None else settings)
        )
        return get_response(response)

    def _get_dataset_settings(self, dataset_id: str):
        response = self.get(suffix=f"/datasets/{dataset_id}/settings")
        return get_response(response)

    def _create_deployable(self, dataset_id: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        response = self.post(
            suffix="/deployables/create",
            json=dict(dataset_id=dataset_id, configuration={} if config is None else config),
        )
        return get_response(response)

    def _share_dashboard(self, deployable_id: str):
        response = self.post(suffix=f"/deployablegroups/{deployable_id}/share")
        return get_response(response)

    def _unshare_dashboard(self, deployable_id: str):
        response = self.post(suffix=f"/deployablegroups/{deployable_id}/private")
        return get_response(response)

    def _get_deployable(self, deployable_id: str):
        response = self.get(suffix=f"/deployables/{deployable_id}/get")
        return get_response(response)

    def _delete_deployable(self, deployable_id: str):
        response = self.post(suffix=f"/deployables/delete", json=dict(id=deployable_id))
        return get_response(response)

    def _list_deployables(self, page_size: int):
        response = self.get(suffix="/deployables/list", params=dict(page_size=min(9999, page_size)))
        return get_response(response)

    def _label_openai(
        self,
        dataset_id: str,
        vector_field: str,
        field: str,
        alias: str,
        question_suffix: str,
        accuracy: int = 4,
        cluster_ids: list = None,
        dont_save_summaries: bool = True,
        filters: list = None,
    ):
        params = {
            "vector_fields": [vector_field],
            # legacy parameter
            "centroid_vector_fields": [vector_field],
            "alias": alias,
            "dataset_id": dataset_id,
            "cluster_ids": cluster_ids,
            "dont_save_summaries": dont_save_summaries,
            "questions": [
                {
                    "cluster_ids": cluster_ids,
                    "config": {
                        "accuracy": accuracy,
                        "examples": [],
                        "field": field,
                        "question_suffix": question_suffix,
                    },
                }
            ],
        }
        if filters is not None:
            params["filters"] = filters
        response = self.post(suffix=f"/datasets/{dataset_id}/cluster/centroids/labels/create", json=params)
        return get_response(response)

    def _aggregate(
        self,
        dataset_id: str,
        page_size: str = 20,
        page: str = 1,
        asc: str = False,
        aggregation_query: Dict[str, List[Dict[str, Any]]] = None,
        dataset_ids: List[str] = None,
        filters: List[Filter] = None,
    ):
        response = self.post(
            suffix=f"/datasets/{dataset_id}/aggregate",
            json=dict(
                filters=[] if filters is None else filters,
                aggregation_query=aggregation_query,
                page_size=min(9999, page_size),
                page=page,
                asc=asc,
                dataset_ids=[] if dataset_ids is None else dataset_ids,
                dataset_id=dataset_id,
            ),
        )
        return get_response(response)

    def _list_closest_to_center(
        self,
        dataset_id: str,
        vector_fields: List[str],
        alias: str,
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
        response = self.post(
            suffix=f"/datasets/{dataset_id}/cluster/centroids/list_closest_to_center",
            json=dict(
                vector_fields=vector_fields,
                alias=alias,
                approx=approx,
                sum_fields=sum_fields,
                page=page,
                similarity_metric=similarity_metric,
                min_score=min_score,
                include_vector=include_vector,
                include_count=include_count,
                include_relevance=include_relevance,
                page_size=min(9999, page_size),
                cluster_properties_filter=cluster_properties_filter if cluster_properties_filter is not None else {},
                filters=filters if filters is not None else [],
                cluster_ids=cluster_ids if cluster_ids is not None else [],
                select_fields=select_fields if select_fields is not None else [],
            ),
        )
        return get_response(response)

    def _list_project_keys(self):
        response = self.get(suffix="/projects/keys/list")
        return get_response(response)

    def _get_project_key(self, key: str, token: str):
        response = self.post(suffix="/projects/keys/get", json=dict(key=key, token=token))
        return get_response(response)

    def _set_project_key(self, key: str, value: str):
        response = self.post(suffix="/projects/keys/set", json=dict(key=key, value=value))
        return get_response(response)

    def _delete_project_key(self, key: str):
        response = self.post(suffix="/projects/keys/delete", json=dict(key=key))
        return get_response(response)

    def _update_version_aliases(self, development_version: str, production_version: str):
        response = self.post(
            suffix="/workflows/types/version_aliases/update",
            json={"aliases": {"development_version": development_version, "production_version": production_version}},
        )
        return get_response(response)

    def _openai_completion(
        self, workflows_admin_token: str, body: dict, suffix: str = "/admin/proxy/openai/v1/completions"
    ):
        response = self.post(suffix=suffix, json={"token": workflows_admin_token, "body": body})
        return get_response(response)

    def _proxy_openai(self, workflows_admin_token: str, endpoint: str, body: dict):
        response = self.post(
            suffix="/admin/proxy/openai" + endpoint, json={"token": workflows_admin_token, "body": body}
        )
        return get_response(response)
