from typing import Any
from slim.dataset import Dataset
from slim.operator import AbstractOperator
from slim.engine import AbstractEngine


class TestAbstractEngine:
    def test_engine(self, full_dataset: Dataset, test_operator: AbstractOperator):
        class ExampleEngine(AbstractEngine):
            def apply(self) -> Any:

                for _ in range(self.nb):
                    batch = self.get_chunk()
                    new_batch = self.operator(batch)
                    self.update_chunk(new_batch)

                return

        engine = ExampleEngine(full_dataset, test_operator)
        assert True

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

        engine = ExampleEngine(full_dataset, test_operator, select_fields=[""])
