import uuid
import warnings

from structlog import get_logger
from typing import Any, Dict, Optional
from workflows_core.dataset.dataset import Dataset

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.errors import WorkflowFailedError
from workflows_core.workflow.context_manager import WorkflowContextManager
from workflows_core.operator.abstract_operator import AbstractOperator


logger = get_logger(__file__)


class Workflow:
    def __init__(
        self,
        engine: AbstractEngine,
        job_id: Optional[str] = None,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: Optional[str] = None,
        send_email: Optional[bool] = None,
        success_threshold: float = 0.5,
    ):
        self._engine = engine
        self._success_threshold = success_threshold
        self._api = engine.dataset.api
        self._metadata = metadata

        if job_id is None:
            self._job_id = str(uuid.uuid4())
            warnings.warn(f"No job id supplied, using {job_id}")
        else:
            self._job_id = job_id

        if name is None:
            self._name = "Workflow"
        else:
            self._name = name

        if additional_information is None:
            self._additional_information = ""
        else:
            self._additional_information = additional_information

        if send_email is None:
            self._send_email = True
        else:
            self._send_email = send_email

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
        try:
            with WorkflowContextManager(
                workflow_name=self._name,
                job_id=self._job_id,
                engine=self.engine,
                dataset=self.dataset,
                operator=self.operator,
                metadata=self._metadata,
                additional_information=self._additional_information,
                send_email=self._send_email,
            ):
                self.engine()
                success_ratio = self.engine._success_ratio
                if success_ratio < self._success_threshold:
                    raise WorkflowFailedError(
                        f"Workflow ran successfully on {100 * success_ratio:.2f}% of documents, less than the required {100 * self._success_threshold:.2f}% threshold"
                    )
        except WorkflowFailedError as e:
            logger.exception(e, stack_info=True)

        return

    def get_status(self):
        return self._api._get_workflow_status(self._job_id)

    def update_metadata(self, metadata: Dict[str, Any]):
        return self._api._update_workflow_metadata(self._job_id, metadata=metadata)


AbstractWorkflow = Workflow
