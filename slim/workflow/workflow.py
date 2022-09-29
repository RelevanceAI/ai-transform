from slim.dataset.dataset import Dataset
from slim.engines.abstract_engine import AbstractEngine
from slim.operator.abstract_operator import AbstractOperator


class Workflow:
    def __init__(
        self, dataset: Dataset, engine: AbstractEngine, operator: AbstractOperator
    ):
        self._dataset = dataset
        self._engine = engine
        self._operator = operator
