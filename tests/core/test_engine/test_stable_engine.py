from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.engine.small_batch_stable_engine import SmallBatchStableEngine

from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.workflow.abstract_workflow import Workflow


class TestStableEngine:
    def test_stable_engine(self, full_dataset: Dataset, test_operator: AbstractOperator):
        engine = StableEngine(full_dataset, test_operator, worker_number=0)
        workflow = Workflow(name="workflow_test123", engine=engine, job_id="test_job123")
        workflow.run()
        assert engine.success_ratio == 1

    def test_small_batch_stable_engine(self, full_dataset: Dataset, test_operator: AbstractOperator):
        engine = SmallBatchStableEngine(full_dataset, test_operator)
        workflow = Workflow(name="workflow_test123", engine=engine, job_id="test_job123")
        workflow.run()
        assert engine.success_ratio == 1

    def test_stable_engine_filters(self, partial_dataset: Dataset, test_operator: AbstractOperator):
        engine = StableEngine(partial_dataset, test_operator, select_fields=["sample_1_label"])
        workflow = Workflow(name="workflow_test123", engine=engine, job_id="test_job123")
        workflow.run()
        assert engine.success_ratio == 1
