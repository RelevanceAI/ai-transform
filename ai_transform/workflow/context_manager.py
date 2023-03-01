import os
import logging

from inspect import Traceback
from typing import Dict, Any, List

from ai_transform.api.api import API
from ai_transform.types import Credentials
from ai_transform.dataset import dataset
from ai_transform.operator import abstract_operator
from ai_transform.engine import abstract_engine
from ai_transform.helpers import format_logging_info

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
)

logger = logging.getLogger(__file__)


WORKFLOW_FAIL_MESSAGE = (
    "Workflow processed {:.2f}%"
    + " of documents. This is less than the success threshold of {:.2f}%"
)


class WorkflowContextManager:

    FAILED = "failed"
    COMPLETE = "complete"
    IN_PROGRESS = "inprogress"

    def __init__(
        self,
        workflow_name: str,
        job_id: str,
        additional_information: str,
        send_email: bool,
        email: Dict[str, Any],
        success_threshold: float,
        credentials: Credentials = None,
        operators: List[abstract_operator.AbstractOperator] = None,
        metadata: Dict[str, Any] = None,
        engine: abstract_engine.AbstractEngine = None,
        dataset: dataset.Dataset = None,
    ):

        self.credentials = credentials
        self.dataset = dataset
        self.engine = engine
        self.dataset_id = self.dataset_id
        self.api = self.dataset.api or API(self.credentials)

        self.workflow_name = workflow_name
        self.operators = operators
        self.job_id = job_id
        self.additional_information = additional_information
        self.send_email = send_email
        self.email = email
        self.success_threshold = success_threshold

        from ai_transform import __version__

        self.metadata = metadata if metadata is not None else {}
        self.metadata["ai_transform_version"] = __version__

    @property
    def field_children_metadata(self) -> Dict[str, str]:
        script_path = os.getenv("script_path", "")
        workflow_id = script_path.split("/")[0]
        return {
            "job_id": self.job_id,
            "workflow_id": workflow_id,
        }

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
                        field_children=output_fields,
                        fieldchildren_id=self.job_id,
                        metadata=metadata,
                        recursive=True,
                    )
                    logger.debug(format_logging_info(res))

    def set_workflow_status(self, status: str):
        result = self.api._set_workflow_status(
            status=status,
            job_id=self.job_id,
            metadata=self.metadata,
            workflow_name=self.workflow_name,
            additional_information=self.additional_information,
            send_email=self.send_email,
            email=self.email,
            worker_number=self.engine.worker_number,
            output=self.engine.output_documents,
        )
        logger.debug(format_logging_info(result))
        return result

    def __enter__(self):
        if self.operators is not None:
            self._set_field_children_recursively()
        return self.set_workflow_status(status=self.IN_PROGRESS)

    def _handle_workflow_fail(
        self, exc_type: type, exc_value: BaseException, traceback: Traceback
    ):
        logger.exception(exc_value)
        self.set_workflow_status(status=self.FAILED)
        return False

    def _handle_workflow_complete(self):
        self.set_workflow_status(status=self.COMPLETE)
        return True

    def _calculate_pricing(self):
        n_processed_pricing = 0
        is_automatic = True

        for operator in self.operators:
            if operator.is_operator_based_pricing:
                n_processed_pricing += operator.n_processed_pricing
                is_automatic = False

        if is_automatic:
            return self._calculate_n_processed_pricing_from_timer()
        else:
            return None

    def _calculate_n_processed_pricing_from_timer(self):
        from ai_transform import _TIMER

        return _TIMER.stop()

    def update_workflow_pricing(self, n_processed_pricing: float):
        return self.api._update_workflow_pricing(
            workflow_id=self.job_id,
            step=self.workflow_name,
            worker_number=self.engine.worker_number,
            n_processed_pricing=n_processed_pricing,
        )

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: Traceback):
        workflow_failed = self.engine.success_ratio < self.success_threshold

        if exc_type is not None or workflow_failed:
            return self._handle_workflow_fail(exc_type, exc_value, traceback)
        else:
            n_processed_pricing = self._calculate_pricing()
            if n_processed_pricing is not None:
                self.update_workflow_pricing(n_processed_pricing)
            return self._handle_workflow_complete()
