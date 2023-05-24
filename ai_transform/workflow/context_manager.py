import os

from inspect import Traceback
from typing import Dict, Any, List

from ai_transform.api.api import API
from ai_transform.types import Credentials
from ai_transform.dataset import dataset
from ai_transform.operator import abstract_operator
from ai_transform.engine import abstract_engine
from ai_transform.logger import ic


WORKFLOW_PROCESSED_MESSAGE = "Workflow processed {:.2f}%" + " of documents. "

WORKFLOW_FAIL_MESSAGE = WORKFLOW_PROCESSED_MESSAGE + "This is less than the success threshold of {:.2f}%"


class WorkflowContextManager:

    FAILED = "failed"
    COMPLETE = "complete"
    IN_PROGRESS = "inprogress"

    def __init__(
        self,
        workflow_name: str,
        job_id: str,
        additional_information: str = "",
        send_email: bool = True,
        email: Dict[str, Any] = None,
        success_threshold: float = 0.8,
        # The arguments below can vary depending on if workflow is ran as
        # a Simple Workflow or a Regular one. They are initialized to None.
        credentials: Credentials = None,
        dataset: dataset.Dataset = None,
        operators: List[abstract_operator.AbstractOperator] = None,
        engine: abstract_engine.AbstractEngine = None,
        metadata: Dict[str, Any] = None,
        output: dict = None,
    ):

        self.credentials = credentials
        self.engine = engine
        self.dataset = dataset

        if self.dataset is not None:
            self.api = self.dataset.api
        else:
            self.api = API(self.credentials)

        self.workflow_name = workflow_name
        self.operators = operators
        self.job_id = job_id
        self.additional_information = additional_information
        self.send_email = send_email
        self.email = email
        self.success_threshold = success_threshold

        if output is not None:
            assert isinstance(output, dict), "When specifying an `output` object, please make sure it of type `dict`"
        self.output = output

        from ai_transform import __version__

        self.metadata = metadata if metadata is not None else {}
        self.metadata["ai_transform_version"] = __version__

        self._n_processed_pricing = None

    @property
    def worker_number(self) -> int:
        if self.engine is not None:
            return self.engine.worker_number
        else:
            return 0

    @property
    def output_documents(self) -> List[Dict[str, Any]]:
        if self.engine is not None:
            return self.engine.output_documents
        else:
            return None

    @property
    def field_children_metadata(self) -> Dict[str, str]:
        script_path = os.getenv("script_path", "")
        workflow_id = script_path.split("/")[0]
        return {"job_id": self.job_id, "workflow_id": workflow_id}

    def _set_field_children_recursively(self):
        for operator in self.operators:
            if operator.update_field_children:
                for input_field in operator.input_fields:

                    metadata = {}
                    metadata.update(self.field_children_metadata)
                    if isinstance(operator.output_fields, dict):
                        metadata.update(operator.output_fields)

                    output_fields = list(operator.output_fields)

                    res = self.dataset[input_field].add_field_children(
                        field_children=output_fields, fieldchildren_id=self.job_id, metadata=metadata, recursive=True
                    )
                    ic(res)

    def _get_output_to_status_obj(self):
        if self.output is not None:
            output = self.output
            if self.output_documents is not None:
                output["documents"] = self.output_documents
        else:
            output = self.output_documents
        return output

    def set_workflow_status(self, status: str, user_errors: str = None):
        result = self.api._set_workflow_status(
            status=status,
            job_id=self.job_id,
            metadata=self.metadata,
            workflow_name=self.workflow_name,
            additional_information=self.additional_information,
            send_email=self.send_email,
            email=self.email,
            worker_number=self.worker_number,
            user_errors=user_errors,
            output=self._get_output_to_status_obj(),
        )
        ic(result)
        return result

    def __enter__(self) -> "WorkflowContextManager":
        if self.operators is not None:
            self._set_field_children_recursively()
        self.set_workflow_status(status=self.IN_PROGRESS)
        return self

    def _handle_workflow_fail(
        self, exc_type: type, exc_value: BaseException, traceback: Traceback, user_errors: str = None
    ):
        ic(exc_value)
        self.set_workflow_status(status=self.FAILED, user_errors=user_errors)
        return False

    def _handle_workflow_complete(self):
        self.set_workflow_status(status=self.COMPLETE)
        return True

    def _calculate_pricing(self):
        n_processed_pricing = 0
        is_automatic = True

        if self.operators is not None:
            for operator in self.operators:
                if operator.is_operator_based_pricing:
                    n_processed_pricing += operator.n_processed_pricing
                    is_automatic = False

        if is_automatic:
            # Use document-based pricing if not manually specified
            if getattr(self, "engine", None) is not None:
                return self._calculate_n_processed_pricing_from_size()
            else:
                return self._calculate_n_processed_pricing_from_timer()
        else:
            return n_processed_pricing

    def _calculate_n_processed_pricing_from_size(self):
        return self.engine.size

    def _calculate_n_processed_pricing_from_timer(self):
        from ai_transform import _TIMER

        return _TIMER.stop()

    def set_workflow_pricing(self, n_processed_pricing: float):
        self._n_processed_pricing = n_processed_pricing

    def update_workflow_pricing(self, n_processed_pricing: float):
        return self.api._update_workflow_pricing(
            workflow_id=self.job_id,
            step=self.workflow_name,
            worker_number=self.worker_number,
            n_processed_pricing=n_processed_pricing,
        )

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: Traceback):
        regular_workflow_failed = False
        user_errors = None

        if self.engine is not None:
            self.addtional_message = WORKFLOW_PROCESSED_MESSAGE.format(100 * self.engine.success_ratio)

            regular_workflow_failed = self.engine.success_ratio < self.success_threshold

            if regular_workflow_failed:
                user_errors = WORKFLOW_FAIL_MESSAGE.format(
                    100 * self.engine.success_ratio, 100 * self.success_threshold
                )

        if exc_type is not None or regular_workflow_failed:
            return self._handle_workflow_fail(exc_type, exc_value, traceback, user_errors)
        else:
            n_processed_pricing = self._n_processed_pricing or self._calculate_pricing()
            if n_processed_pricing is not None:
                self.update_workflow_pricing(n_processed_pricing)
            return self._handle_workflow_complete()

    def get_status(self):
        return self.api._get_workflow_status(self.job_id)
