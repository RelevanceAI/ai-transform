from workflows_core.api.client import Client
from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.workflow.abstract_workflow import Workflow


from workflows_core.config import BaseConfig
from pydantic import Field


class SimpleWorkflowConfig(BaseConfig):
    input_field: str = Field(
        "test_input_field",
        description="The field you want to are using to transform on",
    )
    output_field: str = Field(
        "test_output_field",
        description="The output field",
    )
    minimum_coverage: float = Field(
        0.95,
        description="The minimum amount of coverage of the output field relative to the input field.",
    )
    max_time: float = Field(
        6000, description="THe maximum amount of time to allow for this to poll."
    )
    sleep_timer: float = Field(
        10,
        description="How long to wait before each poll",
    )
    parent_job_id: str = Field(
        None,
        description="If supplied - it will update the status of the workflow as complete only once the workflow has complete.",
    )
    parent_job_name: str = Field(
        None,
        description="If supplied - it will update the status of the workflow as complete only once the workflow has complete.",
    )


class TestWorkflow:
    def test_workflow(self, test_engine: AbstractEngine):
        workflow = Workflow(
            name="test_workflow",
            engine=test_engine,
            job_id="test_job",
        )
        res = workflow.run()
        assert res is None


class TestSimpleWorkflow:
    def test_simple_workflow(
        self, test_client: Client, test_simple_workflow_token: str
    ):
        config = SimpleWorkflowConfig.read_token(test_simple_workflow_token)

        x = 0
        with test_client.SimpleWorkflow(
            workflow_name="Simple Workflow", **config.dict()
        ):
            x += 1

        assert x == 1
