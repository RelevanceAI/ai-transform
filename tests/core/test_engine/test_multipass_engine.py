from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.multipass_engine import MultiPassEngine

from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.workflow.abstract_workflow import AbstractWorkflow


class TestStableEngine:
    def test_multipass_engine_abstract(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        engine = MultiPassEngine(
            dataset=full_dataset,
            operators=[
                test_operator,
                test_operator,
            ],
        )
        workflow = AbstractWorkflow(
            name="workflow_test123",
            engine=engine,
            job_id="test_job123",
        )
        workflow.run()
        assert True
