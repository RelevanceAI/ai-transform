from typing import Any

from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils import mock_documents


class TestEnginePlayground:
    def test_engine_playground(
        self,
        full_dataset: Dataset,
        test_operator: AbstractOperator,
    ):

        engine = StableEngine(
            full_dataset,
            test_operator,
            documents=mock_documents(1000),
        )

        assert engine.output_to_status

        engine.apply()

        assert engine.output_documents
        for document in engine.output_documents:
            assert "new_field" in document
            assert document["new_field"] == 3
