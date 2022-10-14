import logging

from inspect import Traceback
from typing import Any, Dict, Optional

from workflows_core.api.api import API
from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.operator.abstract_operator import AbstractOperator

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
)

logger = logging.getLogger(__file__)

class WorkflowContextManager(API):

    FAILED = "failed"
    COMPLETE = "complete"
    IN_PROGRESS = "inprogress"

    def __init__(
        self,
        workflow_name: str,
        workflow_id: str,
        engine: AbstractEngine,
        dataset: Dataset,
        operator: AbstractOperator,
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: str = "",
        send_email: bool = True,
    ) -> None:
        super().__init__(dataset.api._credentials)

        self._engine = engine
        self._operator = operator
        self._dataset = dataset
        self._dataset_id = dataset.dataset_id

        self._update_field_children = (
            self._operator._input_fields is not None
            and self._operator._output_fields is not None
        )
        self._workflow_name = workflow_name
        self._workflow_id = workflow_id

        self._metadata = metadata
        self._additional_information = additional_information
        self._send_email = send_email

    def __enter__(self):
        """
        The workflow is in progress
        """
        if self._workflow_id is not None:
            self._set_status(status=self.IN_PROGRESS)
        return

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: Traceback):

        if self._update_field_children:
            for input_field in self._operator._input_fields:
                self._set_field_children(
                    dataset_id=self._dataset_id,
                    fieldchildren_id=self._workflow_name.lower().replace(
                        "workflow", ""
                    ),
                    field=input_field,
                    field_children=self._operator._output_fields,
                )

        if self._workflow_id is not None:
            if exc_type is not None:
                logger.exception("Exception")
                self._set_status(status=self.FAILED)
                return False
            else:
                # Workflow must have run successfully
                self._set_status(status=self.COMPLETE)
                return True
        else:
            # If not workflow id in env, we simply exit
            return True

    def _set_status(self, status: str):
        """
        Set the status of the workflow
        """ 
        return self._set_workflow_status(
            status=status,
            workflow_id=self._workflow_id,
            metadata={} if self._metadata is not None else self._metadata,
            workflow_name=self._workflow_name,
            additional_information=self._additional_information,
            send_email=self._send_email,
        )
