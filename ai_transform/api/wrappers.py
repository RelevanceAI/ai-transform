import time
import logging
import requests

from ai_transform.logger import format_logging_info
from requests.models import Response

from typing import Union, Sequence, Mapping, Any

logger = logging.getLogger(__file__)
logging.basicConfig()


def request_wrapper(
    fn: Union[requests.get, requests.post],
    args: Sequence = None,
    kwargs: Mapping[str, Any] = None,
    num_retries: int = 3,
    timeout: int = 30,
) -> Response:

    if args is None:
        args = ()

    if kwargs is None:
        kwargs = {}

    for _ in range(num_retries):
        try:
            result = fn(*args, **kwargs)
            assert result.status_code == 200, format_logging_info(
                result.content.decode()
            )
        except Exception as e:
            logger.exception(e)
            time.sleep(timeout)
        else:
            return result

    return result
