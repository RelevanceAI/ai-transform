import requests

from ai_transform.api.wrappers import request_wrapper
from contextlib import redirect_stdout, redirect_stderr
import io
import sys


class TestWrappers:
    def test_request_wrapper_fail_with_error_in_logs(self):
        f = io.StringIO()
        saved_stdout = sys.stdout
        sys.stdout = f
        resp = request_wrapper(
            requests.post,
            args=["https://www.google.com"],
            num_retries=1,
            timeout=1,
            output_to_stdout=True,
        )
        output = f.getvalue()
        sys.stdout = saved_stdout
        assert "status_code" in output
        assert "message" in output

    def test_request_wrapper_fail_2(self):
        f = io.StringIO()
        u = io.StringIO()

        with redirect_stdout(f), redirect_stderr(u):
            resp = request_wrapper(
                requests.post,
                args=("https://www.google.com",),
                kwargs={"json": {"value": 10}},
                num_retries=1,
                output_to_stdout=True,
                timeout=1,
            )

        assert "status_code" in str(u.getvalue()) + str(f.getvalue())
        assert "message" in str(u.getvalue()) + str(f.getvalue())

    def test_request_wrapper_pass(self):
        resp = request_wrapper(
            requests.get, ("https://www.google.com",), timeout=1, num_retries=2
        )
        if resp is None:
            raise ValueError("Resp should not be None")
        assert resp.status_code == 200
