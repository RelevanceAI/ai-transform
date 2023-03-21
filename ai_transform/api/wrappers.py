import time
import requests

from typing import Callable, Sequence, Mapping, Any


def openai_wrapper(
    fn: Callable,
    args: Sequence = None,
    kwargs: Mapping[str, Any] = None,
    num_retries: int = 3,
) -> requests.Request:
    for _ in range(3):
        try:
            result = fn(*args, **kwargs)
            try:
                result.json()
            except AttributeError:
                return result
        except:
            time.sleep(30)
        else:
            return result
    raise ValueError(f"Request was not able to be completed within {num_retries}")
