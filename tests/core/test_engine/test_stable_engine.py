from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.engine.small_batch_stable_engine import SmallBatchStableEngine

from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.workflow.abstract_workflow import AbstractWorkflow


class TestStableEngine:
    def test_stable_engine(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        engine = StableEngine(full_dataset, test_operator, worker_number=0)
        workflow = AbstractWorkflow(
            name="workflow_test123",
            engine=engine,
            job_id="test_job123",
        )
        workflow.run()
        assert True

    def test_small_batch_stable_engine(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        engine = SmallBatchStableEngine(full_dataset, test_operator)
        workflow = AbstractWorkflow(
            name="workflow_test123",
            engine=engine,
            job_id="test_job123",
        )
        workflow.run()
        assert True
