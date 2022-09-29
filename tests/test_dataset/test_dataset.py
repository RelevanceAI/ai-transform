import random
from slim import Client
from slim.dataset.dataset import Dataset
from slim.utils import mock_documents


class TestDataset:
    def test_schema(self, full_dataset: Dataset):
        documents = mock_documents(5)
        keys = documents[0].keys()
        schema = full_dataset.schema
        assert all([key in schema for key in keys])

    def test_create_delete(self, empty_dataset: Dataset):
        empty_dataset.delete()
        empty_dataset.create()
        assert True

    def test_insert(self, empty_dataset: Dataset):
        documents = mock_documents(100)
        result = empty_dataset.insert_documents(documents)
        assert result["inserted"] == 100

    def test_series(self, full_dataset: Dataset):
        schema = full_dataset.schema
        series = full_dataset[random.choice(list(schema.keys()))]
        assert True
