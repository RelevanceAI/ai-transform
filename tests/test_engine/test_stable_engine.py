from core.dataset import Dataset
from core.operator import AbstractOperator
from core.engine import StableEngine
from core.workflow import AbstractWorkflow


class TestStableEngine:
    def test_engine_abstract(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        engine = StableEngine(full_dataset, test_operator)
        workflow = AbstractWorkflow(engine)
        workflow.run()
        assert True
