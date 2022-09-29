from slim.dataset.dataset import Dataset
from slim.engines.abstract_engine import AbstractEngine
from slim.operator.abstract_operator import AbstractOperator


class AbstractWorkflow:
    def __init__(
        self, dataset: Dataset, engine: AbstractEngine, operator: AbstractOperator
    ):
        self._dataset = dataset
        self._operator = operator
        self._engine = engine

    def run(self):
        self._engine(self._dataset, self._operator)
