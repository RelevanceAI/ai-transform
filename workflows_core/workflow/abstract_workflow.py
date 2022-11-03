import uuid
import logging
import warnings

from typing import Any, Dict, Optional
from workflows_core.dataset.dataset import Dataset

from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.errors import WorkflowFailedError
from workflows_core.workflow.context_manager import WorkflowContextManager
from workflows_core.operator.abstract_operator import AbstractOperator


logger = logging.getLogger(__name__)


class Workflow:
    def __init__(
        self,
        engine: AbstractEngine,
        job_id: Optional[str] = None,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: str = "",
        send_email: bool = True,
        success_threshold: float = 0.5,
    ):
        self._name = "Workflow" if name is None else name
        self._engine = engine

        if job_id is None:
            job_id = str(uuid.uuid4())
            warnings.warn(f"No job id supplied, using {job_id}")

        self._job_id = job_id

        # Update the header
        engine.dataset.api._headers.update(
            workflows_core_job_id=job_id,
            workflows_core_name=name,
        )

        engine.job_id = job_id
        engine.name = name

        self._api = engine.dataset.api
        self._metadata = metadata
        self._additional_information = additional_information
        self._send_email = send_email

        self._success_threshold = success_threshold

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
                if success_ratio is not None and success_ratio < self._success_threshold:
                    WORKFLOW_FAIL_MESSAGE = f"Workflow ran successfully on {100 * success_ratio:.2f}% of documents, less than the required {100 * self._success_threshold:.2f}% threshold"
                    self._api._set_workflow_status(
                        job_id=self._job_id,
                        workflow_name=self._name,
                        additional_information=WORKFLOW_FAIL_MESSAGE,
                        status="failed",
                        send_email=self._send_email,
                        metadata={"error": WORKFLOW_FAIL_MESSAGE},
                        worker_number=self.engine.worker_number
                    )
                    raise WorkflowFailedError(
                        f"Workflow ran successfully on {100 * success_ratio:.2f}% of documents, less than the required {100 * self._success_threshold:.2f}% threshold"
                    )
        except WorkflowFailedError as e:
            logger.error(e)

        return

    def get_status(self):
        return self._api._get_workflow_status(self._job_id)

    def update_metadata(self, metadata: Dict[str, Any]):
        return self._api._update_workflow_metadata(self._job_id, metadata=metadata)


AbstractWorkflow = Workflow
