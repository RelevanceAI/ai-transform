import os
import pprint
import logging

from inspect import Traceback
from typing import Any, Dict, List, Optional

from ai_transform.helpers import format_logging_info
from ai_transform.api.api import API
from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.operator.abstract_operator import AbstractOperator

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
)

logger = logging.getLogger(__file__)


class WorkflowContextManager(API):

    FAILED = "failed"
    COMPLETE = "complete"
    IN_PROGRESS = "inprogress"

    def __init__(
        self,
        workflow_name: str,
        job_id: str,
        engine: AbstractEngine,
        dataset: Dataset,
        operators: List[AbstractOperator],
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: str = "",
        send_email: bool = True,
        email: dict = None,
        mark_as_complete_after_polling: bool = False,
    ) -> None:
        super().__init__(dataset.api.credentials, job_id, workflow_name)

        self._engine = engine
        self._dataset = dataset
        self._dataset_id = dataset.dataset_id

        self._operators = operators
        self._workflow_name = workflow_name
        self._job_id = job_id

        self._metadata = metadata
        self._additional_information = additional_information
        self._send_email = send_email
        self._email = email
        self._mark_as_complete_after_polling = mark_as_complete_after_polling

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @property
    def field_children_metadata(self) -> Dict[str, str]:
        script_path = os.getenv("script_path", "")
        workflow_id = script_path.split("/")[0]
        return {
            "job_id": self._job_id,
            "workflow_id": workflow_id,
        }

    def __enter__(self):
        """
        The workflow is in progress
        """
        self._set_field_children_recursively()
        self._set_status(
            status=self.IN_PROGRESS, worker_number=self._engine.worker_number
        )
        return

    def _set_field_children_recursively(self):
        for operator in self._operators:
            if operator.update_field_children:
                for input_field in operator.input_fields:

                    metadata = {}
                    metadata.update(self.field_children_metadata)
                    if isinstance(operator.output_fields, dict):
                        metadata.update(operator.output_fields)

                    output_fields = list(operator.output_fields)

                    res = self.dataset[input_field].add_field_children(
                        field_children=output_fields,
                        fieldchildren_id=self._job_id,
                        metadata=metadata,
                        recursive=True,
                    )
                    logger.debug(format_logging_info(res))

    def _handle_workflow_fail(
        self, exc_type: type, exc_value: BaseException, traceback: Traceback
    ):
        self._set_status(status=self.FAILED, worker_number=self._engine.worker_number)
        logger.exception(exc_value)
        self._update_workflow_metadata(
            job_id=self._job_id,
            metadata={},
        )
        return False

    def _handle_workflow_complete(
        self, exc_type: type, exc_value: BaseException, traceback: Traceback
    ):
        # Workflow must have run successfully
        if self._mark_as_complete_after_polling:
            # TODO: trigger a polling job while keeping this one in progress
            # When triggering this poll job - we can send the job ID
            result = self._trigger_polling_workflow(
                dataset_id=self._dataset_id,
                input_field=self._operators[0].input_fields[0],
                output_field=self._operators[-1].output_fields[-1],
                job_id=self._job_id,
                workflow_name=self._workflow_name,
            )
            logger.debug(format_logging_info({"trigger_poll_id": result}))
        else:
            self._set_status(
                status=self.COMPLETE,
                worker_number=self._engine.worker_number,
                output=self._engine.output_documents,
            )

        return True

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: Traceback):
        if exc_type is not None:
            return self._handle_workflow_fail(exc_type, exc_value, traceback)
        else:
            return self._handle_workflow_complete(exc_type, exc_value, traceback)

    def _set_status(
        self,
        status: str,
        worker_number: int = None,
        output: List[object] = None,
    ):
        """
        Set the status of the workflow
        """
        result = self._set_workflow_status(
            status=status,
            job_id=self._job_id,
            metadata={} if self._metadata is not None else self._metadata,
            workflow_name=self._workflow_name,
            additional_information=self._additional_information,
            send_email=self._send_email,
            email=self._email,
            worker_number=worker_number,
            output=[] if output is None else output,
        )
        from ai_transform import __version__

        logger.debug(
            format_logging_info(
                {
                    "status": status,
                    "job_id": self._job_id,
                    "workflow_name": self._workflow_name,
                    "worker_number": worker_number,
                    "result": result,
                    "ai_transform_version": __version__,
                }
            )
        )
        return result
