import json
import random

from copy import deepcopy
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.example_documents import mock_documents


class TestDocumentDiff:
    def test_diff(self):
        old_documents = [
            {"example_vector_": [random.random() for _ in range(5)]} for _ in range(5)
        ]
        new_documents = deepcopy(old_documents)
        for document in new_documents:
            document["label"] = "yes"

        diff = AbstractOperator._postprocess(new_documents, old_documents)

        expected = json.dumps({"label": "yes"})
        assert all(json.dumps(document.to_json()) == expected for document in diff)

    def test_no_diff(self):
        documents = mock_documents()
        diff = AbstractOperator._postprocess(documents, documents)
        assert not diff
