import time
import random

from ai_transform.api.client import Client
from ai_transform.utils.example_documents import mock_documents


class TestClient:
    def test_init_dataset(self, test_client: Client, test_dataset_id: str):
        test_client.create_dataset(test_dataset_id)
        test_client.delete_dataset(test_dataset_id)
        assert True

    def test_create_dataset(self, test_client: Client, test_dataset_id: str):
        dataset = test_client.Dataset(test_dataset_id)
        test_client.delete_dataset(dataset._dataset_id)
        assert True

    def test_delete_where(self, test_client: Client, test_dataset_id: str):
        dataset = test_client.Dataset(test_dataset_id)

        n_test_docs = 20
        n_to_delete = 10

        documents = mock_documents(n_test_docs)
        _ids = list(set([document["_id"] for document in random.choices(documents, k=n_to_delete)]))

        dataset.insert_documents(documents)
        dataset.delete_documents(filters=dataset["_id"] == _ids)

        time.sleep(2)

        all_documents = dataset.get_all_documents()["documents"]
        all_ids = [document["_id"] for document in all_documents]

        assert len(all_documents) == (n_test_docs - len(_ids))
        assert all([_id not in all_ids for _id in _ids])

    def test_no_auth_client(self):
        client = Client(token="this:token:doesn't:work", authenticate=False)
        assert True
