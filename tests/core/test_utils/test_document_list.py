import json

from copy import deepcopy
import random

from workflows_core.utils.document_list import DocumentList


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
    remove_field = "label"
    tag_field = "_surveytag_.text"

    def test_remove_tags(self, test_tag_documents: DocumentList):
        tag_values = set()
        for document in test_tag_documents:
            tag_values.update(list(map(lambda x: x["label"], document[self.tag_field])))
        random_tag = random.choice(list(tag_values))

        test_tag_documents.remove_tag(
            f"{self.tag_field}.{self.remove_field}", random_tag
        )
        for document in test_tag_documents:
            assert all(
                tag[self.remove_field] != random_tag for tag in document[self.tag_field]
            )

    def test_sort_tags(self, test_tag_documents: DocumentList):
        test_tag_documents.sort_tags(f"{self.tag_field}.{self.remove_field}")
        assert True
