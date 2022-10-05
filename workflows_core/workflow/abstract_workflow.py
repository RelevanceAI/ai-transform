from workflows_core.dataset.dataset import Dataset

from workflows_core.engine.abstract_engine import AbstractEngine


class AbstractWorkflow:
    def __init__(self, engine: AbstractEngine):
        self._engine = engine

    @property
    def engine(self) -> AbstractEngine:
        return self._engine

    @property
    def dataset(self) -> Dataset:
        return self.engine.dataset

    @property
    def operator(self):
        return self.engine.operator

    def run(self):
        self.engine()
        return
