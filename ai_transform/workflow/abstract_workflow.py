import uuid
import logging
import warnings

from typing import Any, List, Dict, Optional

from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.workflow.context_manager import WorkflowContextManager
from ai_transform.operator.abstract_operator import AbstractOperator

logger = logging.getLogger(__name__)


class Workflow:
    def __init__(
        self,
        engine: AbstractEngine,
        job_id: Optional[str] = None,
        name: Optional[str] = "Workflow",
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: str = "",
        send_email: bool = True,
        success_threshold: float = 0.8,
        email: dict = None,
        **kwargs,  # TODO: Update workflows, This for deprecated arguments
    ):

        if job_id is None:
            job_id = str(uuid.uuid4())
            warnings.warn(f"No job id supplied, using {job_id}")

        self._name = name
        self._engine = engine
        self._job_id = job_id

        self.engine.update_engine_props(job_id=job_id, workflow_name=name)

        self._metadata = {} if metadata is None else metadata
        self._additional_information = additional_information
        self._send_email = send_email

        self._success_threshold = success_threshold
        self._email = email

    @property
    def success_threshold(self):
        return self._success_threshold

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
    def api(self):
        return self.dataset.api

    @property
    def job_id(self):
        return self._job_id

    @property
    def metadata(self):
        return self._metadata

    @property
    def additional_information(self):
        return self._additional_information

    @property
    def send_email(self):
        return self._send_email

    @property
    def email(self):
        return self._email

    @property
    def operators(self) -> List[AbstractOperator]:
        if isinstance(self.engine.operator, AbstractOperator):
            return [self.engine.operator]
        else:
            return self.engine.operators

    def run(self):
        with WorkflowContextManager(
            workflow_name=self.name,
            job_id=self.job_id,
            additional_information=self.additional_information,
            send_email=self.send_email,
            email=self.email,
            success_threshold=self.success_threshold,
            operators=self.operators,
            metadata=self.metadata,
            engine=self.engine,
            dataset=self.dataset,
        ):
            self.engine()

    def get_status(self):
        return self.api._get_workflow_status(self._job_id)

    def update_metadata(self, metadata: Dict[str, Any]):
        return self.api._update_workflow_metadata(self._job_id, metadata=metadata)


AbstractWorkflow = Workflow
