from slim import Client
from slim.dataset.dataset import Dataset
from slim.utils.documents import mock_documents


class TestDataset:
    def test_insert(self, test_dataset: Dataset):
        documents = mock_documents(100)
        result = test_dataset.insert_documents(documents)
        assert result["inserted"] == 100
