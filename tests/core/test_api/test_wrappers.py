import requests

from ai_transform.api.wrappers import request_wrapper
from contextlib import redirect_stdout, redirect_stderr
import io
import sys


class TestWrappers:
    def test_request_wrapper_fail(self):
        resp = request_wrapper(
            requests.post,
            args=["https://www.google.com"],
            num_retries=1,
            timeout=1,
        )

        assert "status_code" in resp._content.decode()
        assert "message" in resp._content.decode()

    def test_request_wrapper_fail_2(self):
        # ####
        f = io.StringIO()
        u = io.StringIO()

        resp = request_wrapper(
            requests.post,
            args=("https://www.google.com",),
            kwargs={"json": {"value": 10}},
            num_retries=1,
            timeout=1,
        )

        assert "status_code" in resp._content.decode()
        assert "message" in resp._content.decode()

    def test_request_wrapper_pass(self):
        resp = request_wrapper(
            requests.get, ("https://www.google.com",), timeout=1, num_retries=2
        )
        if resp is None:
            raise ValueError("Resp should not be None")
        assert resp.status_code == 200
