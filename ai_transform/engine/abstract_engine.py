import time
import logging
import warnings

from json import JSONDecodeError
from typing import Any, List, Optional, Sequence, Iterator
from abc import ABC, abstractmethod

from tqdm.auto import tqdm

from ai_transform.logger import format_logging_info, ic
from ai_transform.types import Filter
from ai_transform.dataset.dataset import Dataset
from ai_transform.operator.abstract_operator import AbstractOperator

from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList

from ai_transform.errors import MaxRetriesError


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
        output_to_status: Optional[bool] = False,
        documents: Optional[List[object]] = None,
        operators: Sequence[AbstractOperator] = None,
        limit_documents: Optional[int] = None,
    ):
        if select_fields is not None:
            # We set this to a warning so that workflows that are adding
            # onto an existing field don't need this. For example - adding tags
            # to existing tags. If existing tags don't exist - it shouldn't break
            # the whole workflow. This allows for multiple workflows to be run in parallel
            # without worrying about breaking things.
            if check_for_missing_fields:
                assert all(
                    field in dataset.schema for field in select_fields if field not in {"_id", "insert_date_"}
                ), f"Some fields not in dataset schema - namely {select_fields}. If this is not desired behavior, set check_for_missing_fields=False."
            else:
                for field in select_fields:
                    if field not in ["_id", "insert_date_"]:
                        if field not in dataset.schema:
                            warnings.warn(f"Not all fields were found. Missing {field}")

        self._dataset = dataset

        if select_fields is not None:
            fields_to_add = []
            for field in select_fields:
                if "_chunk_" in field:
                    chunk_index = field.index("_chunk_") + len("_chunk_")
                    chunk_field = field[:chunk_index]
                    fields_to_add += [chunk_field]
            select_fields += fields_to_add
            select_fields = list(set(select_fields))
        else:
            select_fields = []

        self._select_fields = select_fields

        self.worker_number = worker_number
        self.total_workers = total_workers

        self._limit_documents = limit_documents

        if isinstance(pull_chunksize, int):
            assert pull_chunksize > 0, "Chunksize should be a Positive Integer"
            self._pull_chunksize = pull_chunksize
        else:
            warnings.warn(f"`chunksize=None` assumes the operation transforms on the entire dataset at once")
            self._pull_chunksize = self._size

        # If limiting documents, make sure pull chunk size is not larger than document count
        if self._limit_documents is not None and self._limit_documents < self._pull_chunksize:
            self._pull_chunksize = self.limit_documents

        self._output_to_status = output_to_status  # Whether we should output_to_status
        self._output_documents = []  # document store for output

        # Empty unless documents passed into run on instead of dataset
        self._documents = DocumentList(documents)
        if len(self._documents) > 0:
            # Force output to status if running on documents
            self._output_to_status = True

        if operator is not None:
            self._operator = operator
            self._operators = [operator]
        else:
            self._operator = None
            self._operators = operators

        if filters is None:
            filters = []
        assert isinstance(filters, list), "Filters must be applied as a list of Dictionaries"

        if not refresh:
            filters += self._get_refresh_filter(select_fields, dataset)
        filters += self._get_workflow_filter()

        self._filters = filters

        if self.documents:
            self._size = len(documents)
        else:
            self._size = dataset.len(filters=filters) if self._limit_documents is None else self._limit_documents

        self._refresh = refresh
        self._after_id = after_id

        self._successful_documents = 0
        self._success_ratio = None

        self._job_id = None
        self._workflow_name = None

    @property
    def operator(self) -> AbstractOperator:
        return self._operator

    @property
    def operators(self) -> Sequence[AbstractOperator]:
        return self._operators

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
    def documents(self) -> List[Document]:
        return self._documents

    @property
    def output_to_status(self) -> bool:
        return self._output_to_status

    @property
    def output_documents(self) -> bool:
        return self._output_documents

    @property
    def success_ratio(self) -> float:
        if self._success_ratio is None:
            success_ratio = 1
        else:
            success_ratio = self._success_ratio
        return success_ratio

    @property
    def job_id(self):
        return self._job_id

    @property
    def name(self):
        return self._workflow_name

    def extend_output_documents(self, documents: List[Document]):
        self._output_documents.extend(documents)

    @abstractmethod
    def apply(self) -> None:
        raise NotImplementedError

    def __call__(self) -> Any:
        if self.size != 0:
            self.apply()
        self.set_success_ratio()

    def _operate(self, mini_batch):
        try:
            # note: do not put an IF inside ths try-except-else loop - the if code will not work
            transformed_batch = self.operator(mini_batch)
        except Exception as e:
            ic(e)
            ic({"chunk_ids": self._get_chunks_ids(mini_batch)})
        else:
            # if there is no exception then this block will be executed
            # we only update schema on the first chunk
            # otherwise it breaks down how the backend handles
            # schema updates
            self._successful_documents += len(mini_batch)
            return transformed_batch

    def _get_refresh_filter(self, select_fields: List[str], dataset: Dataset):
        # initialize the refresh filter container
        refresh_filters = {"filter_type": "or", "condition_value": []}

        # initialize where the filters are going
        input_field_filters = []
        output_field_filters = {"filter_type": "or", "condition_value": []}

        # We want documents where all select_fields exists
        # as these are needed for operator ...
        for field in select_fields:
            input_field_filters += dataset[field].exists()

        # ... and where any of its output_fields dont exist
        for operator in self.operators:
            if operator.output_fields is not None:
                for output_field in operator.output_fields:
                    output_field_filters["condition_value"] += dataset[output_field].not_exists()

        # We construct this as:
        #
        #   input_field1 and input_field2 and (not output_field1 or not output_field2)
        #
        # This use case here is for two input fields and two output fields
        # tho this extends to arbitrarily many.
        refresh_filters["condition_value"] = input_field_filters
        refresh_filters["condition_value"] += [output_field_filters]

        # Wrap in list at end
        return [refresh_filters]

    def _get_workflow_filter(self, field: str = "_id"):
        # Get the required workflow filter as an environment variable
        # WORKER_NUMBER is passed into execute function
        # total number of workers must be greater than 1 for data sharding to work
        if self.worker_number is not None and self.total_workers is not None:
            if self.total_workers > 1:
                return [{"matchModulo": {"field": field, "modulo": self.total_workers, "value": self.worker_number}}]
        return []

    def get_iterator(self) -> Iterator:
        if self.documents is None or len(self.documents) == 0:
            # Iterate through dataset
            iterator = self.iterate()
        else:
            # Iterate through passed in documents
            iterator = self.chunk_documents(chunksize=min(100, len(self.documents)), documents=self.documents)
        return iterator

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
                if self.limit_documents is None or documents_processed + self.pull_chunksize < self.limit_documents:
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
                ic(e)
                retry_count += 1
                time.sleep(1)

                if retry_count >= max_retries:
                    raise MaxRetriesError("max number of retries exceeded")
            else:
                self._after_id = chunk["after_id"]
                if not chunk["documents"]:
                    break

                documents = chunk["documents"]
                documents = self._filter_for_non_empty_list(documents)
                if documents:
                    yield documents

                retry_count = 0
                # If document limit is hit, break the loop
                documents_processed += chunk["count"]
                if self.limit_documents is not None and documents_processed >= self.limit_documents:
                    break

    @staticmethod
    def chunk_documents(chunksize: int, documents: List[Document]):
        num_chunks = len(documents) // chunksize + 1
        for i in range(num_chunks):
            start = i * chunksize
            end = (i + 1) * chunksize
            chunk = documents[start:end]
            if len(chunk) > 0:
                yield chunk

    def update_chunk(
        self,
        chunk: List[Document],
        max_retries: int = 3,
        ingest_in_background: bool = True,
        update_schema: bool = False,
    ):
        if chunk:
            for _ in range(max_retries):
                try:
                    update_json = self._dataset.update_documents(
                        documents=chunk, ingest_in_background=ingest_in_background, update_schema=update_schema
                    )
                except Exception as e:
                    ic(e)
                else:
                    return update_json

            raise MaxRetriesError("max number of retries exceeded")

    def api_progress(
        self,
        iterator: Iterator,
        show_progress_bar: bool = True,
        n_total: int = None,
        n_passes: int = 1,
        pass_index: int = 0,
    ) -> Iterator:
        assert n_passes >= 1, "`n_passes` must be strictly positive and greater than 0"
        assert pass_index >= 0, "`pass_index` must be strictly positive"

        if n_total is None:
            n_total = self.size

        total = n_total * n_passes
        inital_value = pass_index * n_total
        self.update_progress(n_processed=inital_value, n_total=total)

        desc = " -> ".join([repr(operator) for operator in self.operators])

        tqdm_bar = tqdm(range(total), desc=desc, disable=(not show_progress_bar), total=total)
        tqdm_bar.update(inital_value)

        total_so_far = 0
        for batch in iterator:
            yield batch
            api_n_processed = total_so_far + len(batch) + pass_index * n_total
            self.update_progress(n_processed=api_n_processed, n_total=total)
            total_so_far += len(batch)
            tqdm_bar.update(len(batch))

    def update_progress(self, n_processed: int, n_total: int = None):
        """
        n_process: int
            the number of documents that have been processed:

        n_total: int
            the total number of documets to be processed
        """
        if self.job_id:
            if n_total is None:
                n_total = self.size

            return self.dataset.api._update_workflow_progress(
                workflow_id=self.job_id,
                worker_number=self.worker_number,
                step=self.name,
                n_processed=n_processed,
                n_total=n_total,
            )

    def update_engine_props(self, job_id: str, workflow_name: str):
        self._job_id = job_id
        self._workflow_name = workflow_name
        self.dataset.api.headers.update(ai_transform_job_id=job_id, ai_transform_name=workflow_name)

    def set_success_ratio(self) -> None:
        if self.size:
            denominator = self.size * len(self.operators)
            self._success_ratio = self._successful_documents / denominator
        else:
            self._success_ratio = 1
        ic({"success_ratio": self._success_ratio})

    @staticmethod
    def _filter_for_non_empty_list(documents: List[Document]) -> List[Document]:
        # if there are more keys than just _id in each document
        # then return that as a list of Documents
        # length of a dictionary is just 1 if there is only 1 key
        return DocumentList([document for document in documents if len(document.keys()) > 1])

    @staticmethod
    def _get_chunks_ids(documents: List[Document]) -> List[Document]:
        return [document["_id"] for document in documents]
