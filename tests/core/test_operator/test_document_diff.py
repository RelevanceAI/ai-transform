import json
import random

from copy import deepcopy
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.example_documents import (
    mock_documents,
    generate_random_label,
    generate_random_vector,
)


class TestDocumentDiff:
    def test_diff(self):
        old_documents = mock_documents(10)
        new_documents = deepcopy(old_documents)

        expected_diff = []

        for document in new_documents:
            new_chunk = {
                "label": generate_random_label(),
                "label_chunkvector_": generate_random_vector(),
            }
            expected_diff.append({"_id": document["_id"], "_chunk_": [new_chunk]})
            document["_chunk_"].append(new_chunk)

        diff = AbstractOperator._postprocess(new_documents, old_documents)

        assert all(
            json.dumps(document.to_json(), sort_keys=True)
            == json.dumps(expected_document, sort_keys=True)
            for document, expected_document in zip(diff, expected_diff)
        )

    def test_no_diff(self):
        documents = [{"value": 10}]
        diff = AbstractOperator._postprocess(documents, documents)
        assert not diff

    def test_chunk_diff(self):
        old_documents = [
            {"example_vector_": [random.random() for _ in range(5)]} for _ in range(5)
        ]
        new_documents = deepcopy(old_documents)
        for document in new_documents:
            document["label"] = "yes"

        diff = AbstractOperator._postprocess(new_documents, old_documents)

        expected = json.dumps({"label": "yes"})
        assert all(json.dumps(document.to_json()) == expected for document in diff)
