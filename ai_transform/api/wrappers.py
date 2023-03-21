import time
import requests

from typing import Union, Sequence, Mapping, Any


def request_wrapper(
    fn: Union[requests.get, requests.post],
    args: Sequence = None,
    kwargs: Mapping[str, Any] = None,
    num_retries: int = 3,
    timeout: int = 30,
) -> requests.Request:
    for _ in range(3):
        try:
            result = fn(*args, **kwargs)
            result.json()
        except:
            time.sleep(timeout)
        else:
            return result
    raise ValueError(f"Request was not able to be completed within {num_retries}")
