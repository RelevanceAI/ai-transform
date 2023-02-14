import uuid
import logging
import traceback
import warnings

from typing import Any, List, Dict, Optional, Union, Sequence
from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.errors import WorkflowFailedError
from ai_transform.workflow.context_manager import WorkflowContextManager
from ai_transform.operator.abstract_operator import AbstractOperator

logger = logging.getLogger(__name__)

WORKFLOW_FAIL_MESSAGE = (
    "Workflow processed {:.2f}%"
    + " of documents. This is less than the success threshold of {:.2f}%"
)


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
        # this is bugged
        mark_as_complete_after_polling: bool = False,
        email: dict = None,
    ):
        self._name = name
        self._engine = engine

        if job_id is None:
            job_id = str(uuid.uuid4())
            warnings.warn(f"No job id supplied, using {job_id}")

        self._job_id = job_id

        # Update the header
        self._engine.dataset.api._headers.update(
            ai_transform_job_id=job_id,
            ai_transform_name=name,
        )

        self._engine.job_id = job_id
        self._engine.name = name

        self._api = engine.dataset.api
        self._metadata = metadata
        self._additional_information = additional_information
        self._send_email = send_email

        self._success_threshold = success_threshold
        self._mark_as_complete_after_polling = mark_as_complete_after_polling
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
    def operators(self) -> List[AbstractOperator]:
        if isinstance(self.engine.operator, AbstractOperator):
            return [self.engine.operator]
        else:
            return self.engine.operators

    def run(self):
        try:
            with WorkflowContextManager(
                workflow_name=self._name,
                job_id=self._job_id,
                engine=self.engine,
                dataset=self.dataset,
                operators=self.operators,
                metadata=self._metadata,
                additional_information=self._additional_information,
                send_email=self._send_email,
                email=self._email,
                mark_as_complete_after_polling=self._mark_as_complete_after_polling,
            ):
                self.engine()
                success_ratio = self.engine._success_ratio
                if success_ratio is None:
                    success_ratio = 1

                if success_ratio is not None and success_ratio < self.success_threshold:
                    fail_message = WORKFLOW_FAIL_MESSAGE.format(
                        100 * success_ratio,
                        100 * self.success_threshold,
                    )
                    self._api._set_workflow_status(
                        job_id=self._job_id,
                        workflow_name=self._name,
                        additional_information=fail_message,
                        status="failed",
                        send_email=self._send_email,
                        metadata={"error": fail_message},
                        worker_number=self.engine.worker_number,
                        email=self._email,
                    )
                    raise WorkflowFailedError(fail_message)

        except WorkflowFailedError as e:
            logger.exception(e)

    def get_status(self):
        return self._api._get_workflow_status(self._job_id)

    def update_metadata(self, metadata: Dict[str, Any]):
        return self._api._update_workflow_metadata(self._job_id, metadata=metadata)


AbstractWorkflow = Workflow
