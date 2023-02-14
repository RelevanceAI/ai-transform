from ai_transform.api.client import Client


class TestClient:
    def test_init_dataset(self, test_client: Client, test_dataset_id: str):
        res = test_client.create_dataset(test_dataset_id)
        res = test_client.delete_dataset(test_dataset_id)
        assert True

    def test_create_dataset(self, test_client: Client, test_dataset_id: str):
        dataset = test_client.Dataset(test_dataset_id)
        res = test_client.delete_dataset(dataset._dataset_id)
        assert True
