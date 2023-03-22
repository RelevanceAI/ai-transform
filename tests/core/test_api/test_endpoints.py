from ai_transform.api.client import Client


class TestEndpoints:
    def test_generic_request(self, test_client: Client):
        resp = test_client.api.post("/auth/users/list")
        resp = test_client.api.get("/health")
        assert resp.status_code == 200
        assert resp.status_code == 200
