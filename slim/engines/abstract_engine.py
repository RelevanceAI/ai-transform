from typing import Any
from abc import ABC, abstractmethod

from slim.dataset.dataset import Dataset
from slim.operator.abstract_operator import AbstractOperator


class AbstractEngine(ABC):
    @abstractmethod
    def __call__(self, dataset: Dataset, operator: AbstractOperator) -> Any:
        raise NotImplementedError
