from copy import deepcopy
import json
import random

from workflows_core.operator.abstract_operator import AbstractOperator


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
