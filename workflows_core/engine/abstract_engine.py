from json import JSONDecodeError
import math
import time
import logging
import warnings

from typing import Any, List, Optional
from abc import ABC, abstractmethod

from workflows_core.types import Filter
from workflows_core.dataset.dataset import Dataset
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.document_list import DocumentList
from workflows_core.errors import MaxRetriesError
from workflows_core.utils import set_seed

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)


class AbstractEngine(ABC):
    MAX_SCHEMA_UPDATE_LIMITER: int = 1

    def __init__(
        self,
        dataset: Dataset = None,
        operator: AbstractOperator = None,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        pull_chunksize: Optional[int] = 3000,
        refresh: bool = True,
        after_id: Optional[List[str]] = None,
        worker_number: int = None,
        total_workers: int = None,
        check_for_missing_fields: bool = True,
        seed: int = 42,
        output_to_status: Optional[bool] = False,
        documents: Optional[List[object]] = None,
        limit_documents: Optional[int] = None,
    ):
        set_seed(seed)
        if select_fields is not None:
            # We set this to a warning so that workflows that are adding
            # onto an existing field don't need this. For example - adding tags
            # to existing tags. If existing tags don't exist - it shouldn't break
            # the whole workflow. This allows for multiple workflows to be run in parallel
            # without worrying about breaking things.
            if check_for_missing_fields:
                assert all(
                    field in dataset.schema
                    for field in select_fields
                    if field not in {"_id", "insert_date_"}
                ), f"Some fields not in dataset schema - namely {select_fields}. If this is not desired behavior, set check_for_missing_fields=False."
            else:
                for field in select_fields:
                    if field not in ["_id", "insert_date_"]:
                        if field not in dataset.schema:
                            warnings.warn(f"Not all fields were found. Missing {field}")

        self._dataset = dataset
        self._select_fields = select_fields
        self.worker_number = worker_number
        self.total_workers = total_workers
        if filters is None:
            filters = []
        filters += self._get_workflow_filter()

        self._limit_documents = limit_documents

        self._size = (
            dataset.len(filters=filters)
            if self._limit_documents is None
            else self._limit_documents
        )

        if isinstance(pull_chunksize, int):
            assert pull_chunksize > 0, "Chunksize should be a Positive Integer"
            self._pull_chunksize = pull_chunksize
        else:
            warnings.warn(
                f"`chunksize=None` assumes the operation transforms on the entire dataset at once"
            )
            self._pull_chunksize = self._size

        # If limiting documents, make sure pull chunk size is not larger than document count
        if (
            self._limit_documents is not None
            and self._limit_documents < self._pull_chunksize
        ):
            self._pull_chunksize = self.limit_documents

        self._num_chunks = math.ceil(self._size / self._pull_chunksize)

        if filters is None:
            self._filters = []
        else:
            self._filters = filters

        self._output_to_status = output_to_status  # Whether we should output_to_status
        self._output_documents = []  # document store for output

        # Empty unless documents passed into run on instead of dataset
        self._documents = DocumentList(documents)
        if len(self._documents) > 0:
            # Force output to status if running on documents
            self._output_to_status = True

        self._operator = operator

        self._refresh = refresh
        self._after_id = after_id

        self._success_ratio = None
        self._error_logs = None

    @property
    def num_chunks(self) -> int:
        return self._num_chunks

    @property
    def operator(self) -> AbstractOperator:
        return self._operator

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @property
    def pull_chunksize(self) -> int:
        return self._pull_chunksize

    @property
    def size(self) -> int:
        return self._size

    @property
    def limit_documents(self) -> int:
        return self._limit_documents

    @property
    def documents(self) -> DocumentList:
        return self._documents

    @property
    def output_to_status(self) -> bool:
        return self._output_to_status

    @property
    def output_documents(self) -> bool:
        return self._output_documents

    def extend_output_documents(self, documents: DocumentList):
        self._output_documents.extend(documents)
        return

    @abstractmethod
    def apply(self) -> None:
        raise NotImplementedError

    def __call__(self) -> Any:
        self.operator.pre_hooks(self._dataset)
        self.apply()
        self.operator.post_hooks(self._dataset)

    def _get_workflow_filter(self, field: str = "_id"):
        # Get the required workflow filter as an environment variable
        # WORKER_NUMBER is passed into execute function
        # total number of workers must be greater than 1 for data sharding to work
        if self.worker_number is not None and self.total_workers is not None:
            if self.total_workers > 1:
                return [
                    {
                        "matchModulo": {
                            "field": field,
                            "modulo": self.total_workers,
                            "value": self.worker_number,
                        }
                    }
                ]
        return []

    def iterate(
        self,
        filters: Optional[List[Filter]] = None,
        select_fields: Optional[List[str]] = None,
        max_retries: int = 5,
        sort: Optional[list] = None,
        include_vector: bool = True,
        random_state: int = 0,
        is_random: bool = False,
    ):
        if filters is None:
            filters = self._filters

        filters += self._get_workflow_filter()

        if select_fields is None:
            select_fields = self._select_fields

        retry_count = 0
        documents_processed = 0

        while True:
            try:
                # Adjust chunksize to get correct amount of documents
                if (
                    self.limit_documents is None
                    or documents_processed + self.pull_chunksize < self.limit_documents
                ):
                    pull_chunksize = self._pull_chunksize
                else:
                    pull_chunksize = self.limit_documents - documents_processed

                chunk = self._dataset.get_documents(
                    page_size=pull_chunksize,
                    filters=filters,
                    select_fields=select_fields,
                    after_id=self._after_id,
                    worker_number=self.worker_number,
                    sort=sort,
                    include_vector=include_vector,
                    random_state=random_state,
                    is_random=is_random,
                )
            except (ConnectionError, JSONDecodeError) as e:
                logger.error(e)
                retry_count += 1
                time.sleep(1)

                if retry_count >= max_retries:
                    raise MaxRetriesError("max number of retries exceeded")
            else:
                self._after_id = chunk["after_id"]
                if not chunk["documents"]:
                    break

                yield chunk["documents"]
                retry_count = 0
                # If document limit is hit, break the loop
                documents_processed += chunk["count"]
                if (
                    self.limit_documents is not None
                    and documents_processed >= self.limit_documents
                ):
                    break

    @staticmethod
    def chunk_documents(chunksize: int, documents: DocumentList):
        num_chunks = len(documents) // chunksize + 1
        for i in range(num_chunks):
            start = i * chunksize
            end = (i + 1) * chunksize
            chunk = documents[start:end]
            if len(chunk) > 0:
                yield chunk

    def update_chunk(
        self,
        chunk: DocumentList,
        max_retries: int = 3,
        ingest_in_background: bool = True,
        update_schema: bool = False,
    ):
        if chunk:
            for _ in range(max_retries):
                try:
                    update_json = self._dataset.update_documents(
                        documents=chunk,
                        ingest_in_background=ingest_in_background,
                        update_schema=update_schema,
                    )
                except Exception as e:
                    logger.error(e)
                else:
                    return update_json

            raise MaxRetriesError("max number of retries exceeded")

    def update_progress(self, n_processed: int):
        """
        Parameters:
        job_id - the job ID
        name - the name of the job
        n_processed - the name of what is processed
        """
        # Update the progress of the workflow
        return self.dataset.api._update_workflow_progress(
            workflow_id=self.job_id,
            worker_number=self.worker_number,
            step=self.name,
            n_processed=min(n_processed * self.pull_chunksize, self._size),
            n_total=self._size,
        )

    #####################################3
    # The following attributes are set by the workflow
    # and provides the update progress functionality
    # required for engines
    @property
    def job_id(self):
        if hasattr(self, "_job_id"):
            return self._job_id
        return

    @job_id.setter
    def job_id(self, value):
        self._job_id = value

    @property
    def name(self):
        if hasattr(self, "_name"):
            return self._name
        return

    @name.setter
    def name(self, value):
        self._name = value
