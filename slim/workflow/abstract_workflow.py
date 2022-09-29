from slim.dataset.dataset import Dataset
from slim.engines.abstract_engine import AbstractEngine
from slim.operator.abstract_operator import AbstractOperator


class AbstractWorkflow:
    def __init__(self, engine: AbstractEngine):
        self._engine = engine

    @property
    def engine(self):
        return self._engine

    def run(self):
        self._engine.apply()
