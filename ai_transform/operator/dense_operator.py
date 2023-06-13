import logging

from abc import abstractmethod

from typing import Dict, Sequence

from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList

logger = logging.getLogger(__file__)


DatasetID = str
DenseOperatorOutput = Dict[DatasetID, Sequence[Document]]


BAD_OPERATOR_MESSAGE = (
    "please ensure the output of the operator is a dict that is a mapping of dataset_ids to documents"
)


class DenseOperator(AbstractOperator):
    def __call__(self, old_documents: DocumentList) -> DenseOperatorOutput:
        datum = self.transform(old_documents)
        if not isinstance(datum, dict):
            raise ValueError(BAD_OPERATOR_MESSAGE)
        for _, documents in datum.items():
            if not isinstance(documents, Sequence):
                raise ValueError(BAD_OPERATOR_MESSAGE)
        return datum

    @abstractmethod
    def transform(self, documents: DocumentList) -> DenseOperatorOutput:
        raise NotImplementedError
