from slim import Client


class TestClient:
    def test_init(self, test_token: str):
        test_client = Client(test_token)
        assert True
