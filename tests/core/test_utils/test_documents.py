from copy import deepcopy

from workflows_core.utils.documents import DocumentList


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
        serialized = test_documents.serialize()
        assert True
