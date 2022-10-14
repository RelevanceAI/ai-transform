import uuid
import warnings
from typing import Optional
from workflows_core.dataset.dataset import Dataset

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.workflow.context_manager import WorkflowContextManager
from workflows_core.operator.abstract_operator import AbstractOperator


class AbstractWorkflow:
    def __init__(
        self,
        engine: AbstractEngine,
        job_id: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        self._name = "Workflow" if name is None else name
        self._engine = engine

        if job_id is None:
            job_id = str(uuid.uuid4())
            warnings.warn(f"No job id supplied, using {job_id}")

        self._workflow_id = job_id

        self._kwargs = kwargs
        self._api = engine.dataset.api

    @property
    def name(self):
        return self._name

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
            workflow_name=self._name,
            workflow_id=self._workflow_id,
            engine=self.engine,
            dataset=self.dataset,
            operator=self.operator,
            **self._kwargs,
        ):
            self.engine()
        return

    def get_status(self):
        return self._api._get_workflow_status(self._workflow_id)


Workflow = AbstractWorkflow
