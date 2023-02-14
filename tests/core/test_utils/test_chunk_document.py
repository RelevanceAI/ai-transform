from copy import deepcopy
from ai_transform.utils import DocumentList


class TestDocumentChunk:
    def test_list_chunks(self, test_documents: DocumentList):
        doc = test_documents[0]
        result = doc.list_chunks()
        assert len(result) > 0
        assert "_chunk_" in result

    def test_get_chunk(self, test_documents: DocumentList):
        doc = test_documents[0]
        labels = doc.get_chunk(chunk_field="_chunk_", field="label")
        assert len(labels) == 1
        assert isinstance(labels[0], str)

    def test_set_chunk(self, test_documents: DocumentList):
        doc = test_documents[0]
        new_labels = ["new_value"]
        doc.set_chunk(chunk_field="_chunk_", field="set-label", values=new_labels)
        assert doc.get_chunk("_chunk_", "set-label") == new_labels
