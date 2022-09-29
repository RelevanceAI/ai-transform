from abc import ABC, abstractmethod

from typing import Any, List
from slim.types import Document


class AbstractOperator(ABC):
    @abstractmethod
    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Every Operator needs a transform function
        """
        raise NotImplementedError

    def __call__(self, documents: List[Document]) -> List[Document]:
        return self.transform(documents)
