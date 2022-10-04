from core.dataset.dataset import Dataset
from core.engine.stable_engine import StableEngine

from core.operator.abstract_operator import AbstractOperator
from core.workflow.abstract_workflow import AbstractWorkflow


class TestStableEngine:
    def test_engine_abstract(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        engine = StableEngine(full_dataset, test_operator)
        workflow = AbstractWorkflow(engine)
        workflow.run()
        assert True
