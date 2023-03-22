from ai_transform.api.client import Client


class TestEndpoints:
    def test_generic_request(self, test_client: Client):
        test_client.api.post("/auth/users/list")
        test_client.api.get("/health")
        assert True
