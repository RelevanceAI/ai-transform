from slim import Client
from slim.dataset.dataset import Dataset
from slim.utils import mock_documents


class TestDataset:
    def test_schema(self, test_dataset: Dataset):
        documents = mock_documents(100)
        keys = documents[0].keys()
        test_dataset.insert_documents(documents)
        schema = test_dataset.schema
        assert all([key in schema for key in keys])

    def test_insert(self, test_dataset: Dataset):
        documents = mock_documents(100)
        result = test_dataset.insert_documents(documents)
        assert result["inserted"] == 100
