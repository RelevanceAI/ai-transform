import time

from pydantic import Field

from ai_transform.api.client import Client
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.workflow.abstract_workflow import Workflow
from ai_transform.config import BaseConfig


class SimpleWorkflowConfig(BaseConfig):
    input_field: str = Field("test_input_field", description="The field you want to are using to transform on")
    output_field: str = Field("test_output_field", description="The output field")
    minimum_coverage: float = Field(
        0.95, description="The minimum amount of coverage of the output field relative to the input field."
    )
    max_time: float = Field(6000, description="THe maximum amount of time to allow for this to poll.")
    sleep_timer: float = Field(10, description="How long to wait before each poll")
    parent_job_id: str = Field(
        None,
        description="If supplied - it will update the status of the workflow as complete only once the workflow has complete.",
    )
    parent_job_name: str = Field(
        None,
        description="If supplied - it will update the status of the workflow as complete only once the workflow has complete.",
    )


class TestWorkflow:
    def test_workflow(self, test_paid_engine: AbstractEngine):
        workflow_name = "test_workflow"
        workflow = Workflow(name=workflow_name, engine=test_paid_engine, job_id="test_job1")
        res = workflow.run()
        assert res is None

        status = workflow.get_status()
        assert status["steps"][workflow_name]["n_processed_pricing"] == 20

    def test_workflow_no_refresh(self, test_paid_engine_no_refresh: AbstractEngine):
        workflow_name = "test_workflow"
        workflow = Workflow(name=workflow_name, engine=test_paid_engine_no_refresh, job_id="test_job2")
        res = workflow.run()
        assert res is None

        status = workflow.get_status()
        assert status["steps"][workflow_name]["n_processed_pricing"] == 20


class TestSimpleWorkflow:
    def test_simple_workflow_simple_case(self, test_client: Client, test_simple_workflow_token: str):
        config = SimpleWorkflowConfig.read_token(test_simple_workflow_token)

        x = 0
        with test_client.SimpleWorkflow(
            workflow_name="Simple Workflow",
            job_id=config.job_id,
            additional_information=config.additional_information,
            send_email=config.send_email,
        ):
            x += 1
        assert x == 1

    def test_simple_workflow(self, test_client: Client):
        simple_workflow_dataset = test_client.Dataset("test-simple-workflow-dataset", expire=True)
        simple_workflow_dataset.insert_documents([{"_id": "0", "value": 0}])

        workflow_name = "Simple Workflow"
        time_sleep_value = 10

        with test_client.SimpleWorkflow(workflow_name=workflow_name, job_id="test-simple-workflow") as workflow:
            time.sleep(time_sleep_value)
            simple_workflow_dataset.update_documents([{"_id": "0", "value": 1}], ingest_in_background=False)

        status = workflow.get_status()
        assert status["status"] == "complete"
        assert status["steps"][workflow_name]["n_processed_pricing"] >= time_sleep_value

        documents = simple_workflow_dataset.get_all_documents()["documents"]
        assert documents[0]["value"] == 1
