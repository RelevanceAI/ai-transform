from typing import Any

from ai_transform.api.client import Client
from ai_transform.dataset.dataset import Dataset
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.engine.abstract_engine import AbstractEngine
from ai_transform.engine.stable_engine import StableEngine

from ai_transform.utils.example_documents import mock_documents


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

        assert isinstance(engine.operator, AbstractOperator)
        assert len(ExampleEngine.__abstractmethods__) == 0

    def test_engine_abstract(self, full_dataset: Dataset, test_operator: AbstractOperator):
        class ExampleEngine(AbstractEngine):
            pass

        try:
            engine = ExampleEngine(full_dataset, test_operator)
        except:
            assert True

    def test_engine_select_fields(self, full_dataset: Dataset, test_operator: AbstractOperator):
        class ExampleEngine(AbstractEngine):
            def apply(self) -> Any:
                return

        engine = ExampleEngine(full_dataset, test_operator, select_fields=["_id", "_chunk_.label"])
        assert "_chunk_" in engine._select_fields

    def test_engine_select_fields(self, test_client: Client, test_dataset_id: str, test_operator: AbstractOperator):
        engine = StableEngine(
            test_client.Dataset(test_dataset_id), test_operator, transform_chunksize=2, documents=mock_documents(20)
        )
        engine()

        assert len(engine.output_documents) == 20

        test_client.delete_dataset(test_dataset_id)
