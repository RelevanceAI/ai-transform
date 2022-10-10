from ast import operator
from workflows_core.dataset.dataset import Dataset

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.workflow.context_manager import WorkflowContextManager
from workflows_core.operator.abstract_operator import AbstractOperator


class AbstractWorkflow:
    def __init__(self, engine: AbstractEngine):
        self._engine = engine

    def __repr__(self):
        return str(type(self).__name__)

    @property
    def engine(self) -> AbstractEngine:
        return self._engine

    @property
    def dataset(self) -> Dataset:
        return self.engine.dataset

    @property
    def operator(self) -> AbstractOperator:
        return self.engine.operator

    def run(self):
        with WorkflowContextManager(
            workflow_name=repr(self),
            engine=self.engine,
            dataset=self.dataset,
            operator=self.operator,
        ):
            self.engine()
        return
