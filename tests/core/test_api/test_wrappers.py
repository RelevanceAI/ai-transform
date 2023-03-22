import requests

from ai_transform.api.wrappers import request_wrapper
from contextlib import redirect_stdout, redirect_stderr
import io
import sys

class TestWrappers:
    def test_request_wrapper_fail(self):
        # ####
        f = io.StringIO()
        u = io.StringIO()

        # with redirect_stdout(f):
        with redirect_stdout(u):
            try:
                resp = request_wrapper(
                    requests.post, 
                    args=["https://www.google.com"],
                    num_retries=1,
                    timeout=1,
                    raise_errors=True
                )
            except Exception as e:
                print(e)
                pass
        
        out = f.getvalue()
        err = u.getvalue()
        assert "status_code" in str(out) + str(err)
        assert "message" in str(out) + str(err)

    def test_request_wrapper_pass(self):
        resp = request_wrapper(
            requests.get, ("https://www.google.com",), timeout=1, num_retries=2
        )
        assert resp.status_code == 200
