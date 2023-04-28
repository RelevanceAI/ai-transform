import io
import sys
import json
import requests

from ai_transform.api.wrappers import request_wrapper
from contextlib import redirect_stdout, redirect_stderr


class TestWrappers:
    def test_request_wrapper_fail_with_error_in_logs(self):
        f = io.StringIO()
        saved_stdout = sys.stderr
        sys.stderr = f
        resp = request_wrapper(
            requests.post, args=["https://www.google.com"], num_retries=1, timeout=1, output_to_stdout=True
        )
        output = f.getvalue()
        sys.stderr = saved_stdout
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
        resp = request_wrapper(requests.get, ("https://www.google.com",), timeout=1, num_retries=2)
        if resp is None:
            raise ValueError("Resp should not be None")
        assert resp.status_code == 200

    def test_request_wrapper_lambda(self):
        f = io.StringIO()
        u = io.StringIO()

        def retry_func(result):
            try:
                result.json()
            except:
                return True
            else:
                return False

        with redirect_stdout(f), redirect_stderr(u):
            resp = request_wrapper(
                requests.get,
                ("https://www.google.com",),
                num_retries=2,
                retry_func=retry_func,
                timeout=1,
                output_to_stdout=True,
            )

        assert "Manual Retry" in str(u.getvalue()) + str(f.getvalue())

    def test_request_wrapper_retry(self):
        f = io.StringIO()
        u = io.StringIO()

        class TestRequest:
            def __init__(self):
                self.count = 0

            def __call__(self, *args, **kwargs):
                if self.count >= 2:
                    return requests.get(*args, **kwargs)
                else:
                    self.count += 1
                    placeholder = requests.Response()
                    placeholder.status_code = 429
                    placeholder._content = b"Simulated Rate Error"
                    return placeholder

        with redirect_stdout(f), redirect_stderr(u):
            resp = request_wrapper(
                TestRequest(), ("https://www.google.com",), num_retries=3, timeout=1, output_to_stdout=True
            )

        assert resp.status_code == 200

        logs = str(u.getvalue()) + str(f.getvalue())

        assert logs.count("Simulated Rate Error") == 4

    def test_request_wrapper_json(self):
        f = io.StringIO()
        u = io.StringIO()

        class TestRequest:
            def __init__(self):
                self.count = 0

            def __call__(self, *args, **kwargs):
                if self.count >= 2:
                    return requests.get(*args, **kwargs)
                else:
                    self.count += 1
                    placeholder = requests.Response()
                    placeholder.encoding = "utf-8"
                    placeholder.status_code = 200
                    placeholder._content = b"\\\\\d"
                    return placeholder

        with redirect_stdout(f), redirect_stderr(u):
            resp = request_wrapper(
                TestRequest(),
                args=("https://raw.communitydragon.org/latest/cdragon/tft/en_us.json",),
                num_retries=3,
                timeout=1,
                output_to_stdout=True,
                is_json_decodable=True,
            )

        assert resp.status_code == 200

        logs = str(u.getvalue()) + str(f.getvalue())

        assert logs.count("Response is not JSON decodable") == 2

    def test_request_wrapper_key_for_error(self):
        f = io.StringIO()
        u = io.StringIO()

        class TestRequest:
            def __init__(self):
                self.count = 0

            def __call__(self, *args, **kwargs):
                if self.count >= 2:
                    return requests.get(*args, **kwargs)
                else:
                    self.count += 1
                    placeholder = requests.Response()
                    placeholder.encoding = "utf-8"
                    placeholder.status_code = 200
                    placeholder._content = json.dumps({"bad_key": False}).encode("utf-8")
                    return placeholder

        with redirect_stdout(f), redirect_stderr(u):
            resp = request_wrapper(
                TestRequest(),
                ("https://raw.communitydragon.org/latest/cdragon/tft/en_us.json",),
                num_retries=3,
                timeout=1,
                output_to_stdout=True,
                key_for_error="bad_key",
            )

        assert resp.status_code == 200

        logs = str(u.getvalue()) + str(f.getvalue())

        assert logs.count("bad_key") == 4
