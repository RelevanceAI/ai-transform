import logging

from abc import abstractmethod

from typing import Dict, Sequence

from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.document import Document
from workflows_core.utils.document_list import DocumentList

logger = logging.getLogger(__file__)


DatasetID = str
DenseOperatorOutput = Dict[DatasetID, Sequence[Document]]


BAD_OPERATOR_MESSAGE = "please ensure the output of the operator is a dict that is a mapping of dataset_ids to documents"


class DenseOperator(AbstractOperator):
    def __call__(self, old_documents: DocumentList) -> DenseOperatorOutput:
        datum = self.transform(old_documents)
        assert isinstance(datum, dict), BAD_OPERATOR_MESSAGE
        for _, documents in datum.items():
            assert isinstance(documents, Sequence)
        return datum

    @abstractmethod
    def transform(self, documents: DocumentList) -> DenseOperatorOutput:
        raise NotImplementedError
