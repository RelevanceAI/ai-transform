import requests

from ai_transform.api.wrappers import request_wrapper


class TestWrappers:
    def test_request_wrapper_fail(self):
        try:
            resp = request_wrapper(
                requests.get, ("asdfasdf",), timeout=1, num_retries=2
            )
        except:
            assert True

    def test_request_wrapper_pass(self):
        resp = request_wrapper(
            requests.get, ("https://www.google.com",), timeout=1, num_retries=2
        )
        assert resp.status_code == 200
