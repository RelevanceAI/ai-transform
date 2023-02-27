import os
import logging

from inspect import Traceback
from typing import Dict

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

    def __init__(self, workflow) -> None:
        from ai_transform.workflow.abstract_workflow import Workflow

        self._workflow: Workflow = workflow

    @property
    def api(self):
        return self.dataset.api

    @property
    def workflow_name(self):
        return self._workflow.name

    @property
    def engine(self):
        return self._workflow.engine

    @property
    def dataset(self):
        return self._workflow.dataset

    @property
    def dataset_id(self):
        return self._workflow.dataset.dataset_id

    @property
    def operators(self):
        return self._workflow.operators

    @property
    def job_id(self):
        return self._workflow.job_id

    @property
    def metadata(self):
        from ai_transform import __version__

        self._workflow.metadata["ai_transform_version"] = __version__
        return self._workflow.metadata

    @property
    def additional_information(self):
        return self._workflow.additional_information

    @property
    def send_email(self):
        return self._workflow.send_email

    @property
    def email(self):
        return self._workflow.email

    @property
    def success_threshold(self):
        return self._workflow.success_threshold

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
