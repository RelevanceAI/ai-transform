from slim import Client


class TestClient:
    def test_create_dataset(self, test_client: Client):
        res = test_client.create_dataset("test_dataset")
        assert True
