from ast import operator
import os

from inspect import Traceback
from typing import Any, Dict, Optional

from workflows_core.api.api import API
from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.operator.abstract_operator import AbstractOperator


class WorkflowContextManager(API):

    FAILED = "failed"
    COMPLETED = "completed"
    IN_PROGRESS = "inprogress"

    def __init__(
        self,
        workflow_name: str,
        engine: AbstractEngine,
        dataset: Dataset,
        operator: AbstractOperator,
    ) -> None:
        super().__init__(dataset.api._credentials)

        self._workflow_name = workflow_name
        self._engine = engine
        self._operator = operator
        self._dataset = dataset
        self._dataset_id = dataset.dataset_id

        self._update_field_children = (
            self._operator._input_fields is not None
            and self._operator._output_fields is not None
        )

        workflow_id = os.getenv("WORKFLOW_ID")
        self._workflow_id = (
            workflow_id if workflow_id != "" and workflow_id is not None else None
        )

    def __enter__(self):
        """
        The workflow is in progress
        """
        if self._workflow_id is not None:
            self._set_status(status=self.IN_PROGRESS)
        return

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: Traceback):

        if self._update_field_children:
            self._set_field_children(
                self._dataset_id,
                self._workflow_name.lower().replace("workflow", ""),
                self._operator._input_fields,
                self._operator._output_fields,
            )

        if self._workflow_id is not None:
            if exc_type is not None:
                # Handle the except, let user know etc...
                self._set_status(status=self.FAILED)
                return False
            else:
                # Workflow must have run successfully
                self._set_status(status=self.COMPLETED)
                return True
        else:
            # If not workflow id in env, we simply exit
            return True

    def _set_status(
        self,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: str = "",
    ):
        """
        Set the status of the workflow
        """
        return self._set_workflow_status(
            workflow_id=self._workflow_id,
            metadata={} if metadata is not None else metadata,
            workflow_name=self._workflow_name,
            additional_information=additional_information,
            status=status,
        )
