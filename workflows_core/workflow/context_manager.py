import os
import pprint
import logging

from inspect import Traceback
from typing import Any, Dict, List, Optional

from workflows_core.helpers import format_logging_info
from workflows_core.api.api import API
from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.operator.abstract_operator import AbstractOperator

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
        super().__init__(dataset.api._credentials, job_id, workflow_name)

        self._engine = engine
        self._dataset = dataset
        self._dataset_id = dataset.dataset_id

        update_field_children = False
        for operator in operators:
            if (
                operator._input_fields is not None
                and operator._output_fields is not None
            ):
                update_field_children = True
                break

        self._operators = operators
        self._update_field_children = update_field_children

        self._workflow_name = workflow_name
        self._job_id = job_id

        self._metadata = metadata
        self._additional_information = additional_information
        self._send_email = send_email
        self._email = email
        self._mark_as_complete_after_polling = mark_as_complete_after_polling

    def __enter__(self):
        """
        The workflow is in progress
        """
        self._set_status(
            status=self.IN_PROGRESS, worker_number=self._engine.worker_number
        )

        if self._update_field_children:
            for operator in self._operators:
                for input_field in operator._input_fields:
                    self.set_field_children(
                        input_field=input_field, output_fields=operator._output_fields
                    )
                
        self._dataset.api._update_workflow_progress(
            workflow_id=self._job_id,
            worker_number=self._engine.worker_number,
            step=self._workflow_name,
            n_processed=0,
            n_total=self._engine._size,
        )
        return

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: Traceback):
        if exc_type is not None:
            self._set_status(
                status=self.FAILED, worker_number=self._engine.worker_number
            )

            logger.exception(exc_value)

            self._update_workflow_metadata(
                job_id=self._job_id,
                metadata=dict(
                    _error_=dict(
                        exc_value=pprint.pformat(exc_value),
                    ),
                ),
            )
            return False
        else:
            # Workflow must have run successfully
            self._calculate_pricing()
            if self._mark_as_complete_after_polling:
                # TODO: trigger a polling job while keeping this one in progress
                # When triggering this poll job - we can send the job ID
                result = self._trigger_polling_workflow(
                    dataset_id=self._dataset_id,
                    input_field=self._operators[0]._input_fields[0],
                    output_field=self._operators[-1]._output_fields[0],
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
        
    def _calculate_pricing(self):
        n_processed_pricing = 0
        for operator in self._operators:
            n_processed_pricing += operator.n_processed_pricing

        # Set default pricing to the the number of documents
        if n_processed_pricing == 0:
            n_processed_pricing = self._engine.size

        self._dataset.api._update_workflow_pricing(
            workflow_id=self._job_id,
            step=self._workflow_name,
            worker_number=self._engine.worker_number,
            n_processed_pricing=n_processed_pricing,
        )


    def set_field_children(self, input_field: str, output_fields: list):
        # Implement the config ID and authorization token
        # Receive these from the env variables in production - do not touch
        script_path = os.getenv("script_path", "")
        metadata = {"job_id": self._job_id, "workflow_id": script_path.split("/")[0]}
        return self._set_field_children(
            dataset_id=self._dataset_id,
            fieldchildren_id=self._workflow_name.lower().replace("workflow", ""),
            field=input_field,
            field_children=output_fields,
            metadata=metadata,
        )

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
        from workflows_core import __version__

        logger.debug(
            "\n"
            + format_logging_info(
                {
                    "status": status,
                    "job_id": self._job_id,
                    "workflow_name": self._workflow_name,
                    "worker_number": worker_number,
                    "result": result,
                    "workflows_core_version": __version__,
                }
            )
        )
        return result
