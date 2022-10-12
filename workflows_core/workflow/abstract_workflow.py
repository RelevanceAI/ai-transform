from ast import operator
from workflows_core.dataset.dataset import Dataset

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.workflow.context_manager import WorkflowContextManager
from workflows_core.operator.abstract_operator import AbstractOperator


class AbstractWorkflow:
    def __init__(self, workflow_id: str, engine: AbstractEngine, **kwargs):
        self._engine = engine
        self._workflow_id = workflow_id
        self._kwargs = kwargs
        self._api = engine.dataset.api

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
            workflow_id=self._workflow_id,
            engine=self.engine,
            dataset=self.dataset,
            operator=self.operator,
            **self._kwargs
        ):
            self.engine()
        return

    def get_status(self):
        return self._api._get_workflow_status(self._workflow_id)
