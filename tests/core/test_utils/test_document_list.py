import json
import random
import string
from copy import deepcopy
from ai_transform.utils.document_list import DocumentList


class TestDocumentList:
    def test_deepcopy(self, test_documents: DocumentList):
        copy_test_documents = deepcopy(test_documents)
        copy_test_documents["field3"] = 4
        copy_test_documents["field1.field2"] = 4

        assert all(
            test_document.get("field3") != copy_test_document["field3"]
            for test_document, copy_test_document in zip(
                test_documents, copy_test_documents
            )
        )
        assert all(
            test_document.get("field1.field") != copy_test_document["field1.field2"]
            for test_document, copy_test_document in zip(
                test_documents, copy_test_documents
            )
        )

    def test_serializer(self, test_documents: DocumentList):
        serialized = test_documents.to_json()
        assert json.dumps(serialized)


class TestDocumentListTagOperations:
    label_field = "label"
    tag_field = "_surveytag_.text"

    def test_remove_tags(self, test_tag_documents: DocumentList):
        tag_values = set()
        for document in test_tag_documents:
            tag_values.update(list(map(lambda x: x["label"], document[self.tag_field])))
        random_tag = random.choice(list(tag_values))

        test_tag_documents.remove_tag(
            f"{self.tag_field}.{self.label_field}", random_tag
        )
        for document in test_tag_documents:
            assert all(
                tag[self.label_field] != random_tag for tag in document[self.tag_field]
            )

    def test_sort_tags(self, test_tag_documents: DocumentList):
        test_tag_documents.sort_tags(
            f"{self.tag_field}.{self.label_field}"
        )  # string case
        test_tag_documents.sort_tags(f"{self.tag_field}.value")  # numeric case
        assert True


class TestDocumentChunkOperations:
    def test_get_chunk_values_as_list(self, test_documents: DocumentList):
        results = test_documents.get_chunks_as_flat("_chunk_", "label")
        for r in results:
            assert isinstance(r, str), f"Not a string, {r}"

    def test_set_chunk_values(self, test_documents: DocumentList):
        LETTERS: list = (
            string.ascii_letters + string.ascii_uppercase + string.ascii_lowercase
        )
        LETTERS = LETTERS * 10
        random_values = LETTERS[: len(test_documents)]
        test_documents.set_chunks_from_flat(
            chunk_field="_chunk_", field="test_label", values=random_values
        )
        results = test_documents.get_chunks_as_flat("_chunk_", "test_label")
        assert results == [x for x in random_values]

    def test_set_chunk_values(self, test_documents: DocumentList):
        chunk_values = [[n] for n in range(len(test_documents))]
        test_documents.set_chunk_values(
            "_chunk_", "'_cluster_id_.default", chunk_values
        )
        for i, document in enumerate(test_documents):
            assert document["_chunk_.0._cluster_id_.default"] == i
