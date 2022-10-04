from slim.dataset import Dataset
from slim.operator import AbstractOperator
from slim.engine import StableEngine
from slim.workflow import AbstractWorkflow


class TestStableEngine:
    def test_engine_abstract(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        engine = StableEngine(full_dataset, test_operator)
        workflow = AbstractWorkflow(engine)
        workflow.run()
        assert True
