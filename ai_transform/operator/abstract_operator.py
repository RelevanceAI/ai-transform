import json
import logging
import warnings
import numpy as np

from copy import deepcopy
from abc import ABC, abstractmethod

from typing import Any, Dict, List, Optional, Union

from ai_transform.dataset.dataset import Dataset
from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList

logger = logging.getLogger(__file__)


def are_vectors_similar(vector_1, vector_2):
    element_wise_diff = abs(np.array(vector_1)) - abs(np.array(vector_2))
    sums = np.sum(element_wise_diff)
    return sums > 0


def is_different(field: str, value1: Any, value2: Any) -> bool:
    """
    An all purpose function that checks if two values are different
    """
    # TODO: Implement a better fix for chunks - but this will do for now
    if isinstance(value1, list) and isinstance(value2, list):
        return any(
            is_different(field, chunk1_value, chunk2_value)
            for chunk1_value, chunk2_value in zip(value1, value2)
        )

    # check if its a vector field but only if it ends with it
    elif (
        field.endswith(("_vector_", "_chunkvector_"))
        and isinstance(value1, list)
        and isinstance(value2, list)
    ):
        return are_vectors_similar(value1, value2)

    elif isinstance(value1, dict) and isinstance(value2, dict):
        return json.dumps(value1, sort_keys=True) != json.dumps(value2, sort_keys=True)

    else:
        return value1 != value2


def get_document_diff(old_document: Document, new_document: Document) -> Document:
    pp_document = Document()
    new_fields = new_document.keys()
    old_fields = old_document.keys()
    for field in new_fields:
        old_value = old_document.get(field, None)
        new_value = new_document.get(field, None)
        value_diff = is_different(field, old_value, new_value)

        is_list = isinstance(new_value, list)
        if is_list:
            contains_chunks = all([isinstance(value, dict) for value in new_value])
        else:
            contains_chunks = False

        is_chunk_field = is_list and contains_chunks

        if (
            field not in old_fields or value_diff or field == "_id" or is_chunk_field
        ) and field not in pp_document.keys():
            pp_document[field] = new_value

    if len(pp_document.keys()) > 1:
        return pp_document


class AbstractOperator(ABC):
    def __init__(
        self,
        input_fields: Optional[List[str]] = None,
        output_fields: Optional[Union[Dict[str, str], List[str]]] = None,
        enable_postprocess: Optional[bool] = True,
    ):

        if input_fields is not None and output_fields is not None:
            if any(input_field in output_fields for input_field in input_fields):
                detected_fields = [
                    input_field
                    for input_field in input_fields
                    if input_field in output_fields
                ]
                warnings.warn(
                    f"Some input fields are present in the output fields, namely {str(detected_fields)}"
                )
                for field in detected_fields:
                    output_fields.remove(field)

        if input_fields is not None:
            assert isinstance(
                input_fields, list
            ), "`input_fields` must be of type list or dict"
            for field_index, input_field in enumerate(input_fields):
                assert isinstance(
                    input_field, str
                ), f"input_field at index {field_index} of `input_fields` is not of type string"

        if output_fields is not None:
            assert isinstance(
                output_fields, list
            ), "`output_fields`  must be of type list or dict"
            for field_index, output_field in enumerate(output_fields):
                assert isinstance(
                    output_field, str
                ), f"output_field at index {field_index} of `output_fields` is not of type string"

        self._input_fields = input_fields
        self._output_fields = output_fields
        self._enable_postprocess = enable_postprocess
        self._n_processed_pricing = None

    def toggle_postprocess(self):
        self._enable_postprocess ^= True

    def set_postprocess(self, state: bool):
        self._enable_postprocess = state

    @property
    def input_fields(self):
        return self._input_fields

    @property
    def output_fields(self):
        return self._output_fields

    @property
    def update_field_children(self):
        return self.input_fields is not None and self.output_fields is not None

    @abstractmethod
    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Every Operator needs a transform function
        """
        raise NotImplementedError

    def __repr__(self):
        return str(type(self).__name__)

    def __call__(self, old_documents: DocumentList) -> DocumentList:
        new_documents = deepcopy(old_documents)
        new_documents = self.transform(new_documents)
        if new_documents is not None and self._enable_postprocess:
            new_documents = self.postprocess(new_documents, old_documents)
        return new_documents

    @staticmethod
    def postprocess(new_batch: DocumentList, old_batch: DocumentList) -> DocumentList:
        """
        Removes fields from `new_batch` that are present in the `old_keys` list.
        Necessary to avoid bloating the upload payload with unnecesary information.
        """
        batch = []
        for old_document, new_document in zip(old_batch, new_batch):
            document_diff = get_document_diff(old_document, new_document)
            if document_diff:
                batch.append(document_diff)
        return DocumentList(batch)

    # Adding this for backwards compatibility
    _postprocess = postprocess

    def transform_for_playground(
        self,
        documents: DocumentList,
        job_id: str,
        authorization_token: str,
        additional_information: str = "",
        workflow_name: str = "Workflow",
        metadata: dict = None,
        status: str = "complete",
        send_email: bool = False,
        worker_number: int = None,
    ):
        """
        Transform and upload an object
        """
        from ai_transform.api.client import Client

        output = self.transform(documents=documents)
        client = Client(authorization_token)
        return client.api._set_workflow_status(
            job_id=job_id,
            workflow_name=workflow_name,
            additional_information=additional_information,
            metadata=metadata,
            status=status,
            send_email=send_email,
            worker_number=worker_number,
            output=output,
        )

    def pre_hooks(self, dataset: Dataset):
        pass

    def post_hooks(self, dataset: Dataset):
        pass

    @property
    def is_operator_based_pricing(self):
        return self._n_processed_pricing is not None and self._n_processed_pricing > 0

    @property
    def n_processed_pricing(self):
        if self._n_processed_pricing is not None:
            return self._n_processed_pricing
        else:
            return 0

    @n_processed_pricing.setter
    def n_processed_pricing(self, value):
        self._n_processed_pricing = value
