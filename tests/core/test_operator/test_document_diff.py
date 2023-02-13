import json
import random

from copy import deepcopy
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.document import Document
from workflows_core.utils.example_documents import (
    mock_documents,
    generate_random_label,
    generate_random_vector,
)


class TestDocumentDiff:
    def test_diff1(self):
        old_documents = mock_documents(3)
        new_documents = deepcopy(old_documents)

        expected_diff = []

        for document in new_documents:
            new_chunk = {
                "label": generate_random_label(),
                "label_chunkvector_": generate_random_vector(),
            }
            expected_diff.append({"_id": document["_id"], "_chunk_": [new_chunk]})
            document["_chunk_"].append(new_chunk)

        diff = AbstractOperator._postprocess(new_documents, old_documents).to_json()
        diff = list(sorted(diff, key=lambda x: x["_id"]))
        expected_diff = list(sorted(expected_diff, key=lambda x: x["_id"]))

        assert json.dumps(diff, sort_keys=True) == json.dumps(
            expected_diff, sort_keys=True
        )

    def test_update_diff(self):
        old_documents = [Document({"label": "yes"}) for _ in range(5)]
        new_documents = deepcopy(old_documents)
        for document in new_documents:
            document["label"] = "no"

        diff = AbstractOperator._postprocess(new_documents, old_documents)
        expected_diff = json.dumps({"label": "no"})

        assert all(json.dumps(document.to_json()) == expected_diff for document in diff)

    def test_no_diff(self):
        documents = [Document({"value": 10})]
        diff = AbstractOperator._postprocess(documents, documents)
        assert not diff

    def test_chunk_diff(self):
        old_documents = [
            Document({"example_vector_": [random.random() for _ in range(5)]})
            for _ in range(5)
        ]
        new_documents = deepcopy(old_documents)
        for document in new_documents:
            document["label"] = "yes"

        diff = AbstractOperator._postprocess(new_documents, old_documents)
        expected_diff = json.dumps({"label": "yes"})

        assert all(json.dumps(document.to_json()) == expected_diff for document in diff)
