import pandas as pd
import pyarrow as pa

from ray.data.block import Block

from copy import deepcopy
from abc import ABC, abstractmethod

from typing import List
from core.utils import Document, DocumentUtils


class AbstractOperator(ABC, DocumentUtils):
    @abstractmethod
    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Every Operator needs a transform function
        """
        raise NotImplementedError

    def __call__(self, old_documents: List[Document]) -> List[Document]:
        new_documents = deepcopy(old_documents)
        self.transform(new_documents)
        new_documents = AbstractOperator._postprocess(new_documents, old_documents)
        return new_documents

    @staticmethod
    def _postprocess(
        new_batch: List[Document], old_batch: List[Document]
    ) -> List[Document]:
        """
        Removes fields from `new_batch` that are present in the `old_keys` list.
        Necessary to avoid bloating the upload payload with unnecesary information.
        """
        batch = []
        for old_document, new_document in zip(old_batch, new_batch):
            pp_document = Document()
            new_fields = new_document.keys()
            old_fields = old_document.keys()
            for field in new_fields:
                old_value = old_document.get(field, None)
                new_value = new_document.get(field, None)
                value_diff = old_value != new_value
                if field not in old_fields or value_diff or field == "_id":
                    pp_document.set(field, new_value)
            batch.append(pp_document)

        return batch


class AbstractRayOperator(AbstractOperator):
    def __call__(self, batch: pd.DataFrame) -> Block:
        batch = pd.json_normalize(batch.to_dict("records"))
        old = batch.copy()
        new = self.transform(batch)
        new = self._postprocess(new, old)
        return pa.Table.from_pandas(new)

    @staticmethod
    def _postprocess(new: pd.DataFrame, old: pd.DataFrame):
        import pdb

        pdb.set_trace()
        return new
