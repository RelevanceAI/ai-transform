from typing import Any

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

    def pre_hook(self, *args, **kwargs) -> Any:
        """
        Optional can be implemented for extra functionality pre workflow
        """
        return

    def post_hook(self, *args, **kwargs) -> Any:
        """
        Optional can be implemented for extra functionality post workflow
        """
        return

    def run(self):
        self.pre_hook()
        self.engine.apply()
        self.post_hook()
        return
