from typing import Any

from ai_transform.dataset.dataset import Dataset
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.engine.abstract_engine import AbstractEngine


class TestAbstractEngine:
    def test_engine(self, full_dataset: Dataset, test_operator: AbstractOperator):
        class ExampleEngine(AbstractEngine):
            def apply(self) -> Any:

                iterator = self.iterate()
                for chunk in iterator:
                    new_batch = self.operator(chunk)
                    self.update_chunk(new_batch)

                return

        engine = ExampleEngine(full_dataset, test_operator)

        assert engine.num_chunks > 0
        assert isinstance(engine.operator, AbstractOperator)
        assert len(ExampleEngine.__abstractmethods__) == 0

    def test_engine_abstract(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        class ExampleEngine(AbstractEngine):
            pass

        try:
            engine = ExampleEngine(full_dataset, test_operator)
        except:
            assert True

    def test_engine_select_fields(
        self, full_dataset: Dataset, test_operator: AbstractOperator
    ):
        class ExampleEngine(AbstractEngine):
            def apply(self) -> Any:
                return

        engine = ExampleEngine(full_dataset, test_operator, select_fields=["_id"])
        assert True
