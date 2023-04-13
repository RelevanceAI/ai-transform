import pytest
import os
from ai_transform.api.client import Client


class TestEndpoints:
    def test_generic_request(self, test_client: Client):
        resp = test_client.api.post("/auth/users/list")
        resp = test_client.api.get("/health")
        assert resp.status_code == 200
        assert resp.status_code == 200


@pytest.mark.skip(reason="Don't hit openai")
def test_proxy_openai(test_client: Client):
    body = {
        "model": f"gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "You are a translator."},
            {
                "role": "assistant",
                "content": """Example response:
{"detected_language" : The detected language, "translated_text": The translated text}
""",
            },
            {"role": "user", "content": "Example"},
        ],
        "temperature": 0,
        "n": 1,
        "presence_penalty": 0,
        "frequency_penalty": 0,
    }
    response = test_client._api._proxy_openai(
        workflows_admin_token=os.environ["WORKFLOW_ADMIN_TOKEN"], body=body, endpoint="/v1/chat/completions"
    )
    assert "choices" in response
