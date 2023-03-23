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
    output_to_stdout: bool = False,  # support output to stdout to ensure logging is working
) -> Response:

    if args is None:
        args = ()

    if kwargs is None:
        kwargs = {}

    for _ in range(num_retries):
        try:
            result = fn(*args, **kwargs)
            if result.status_code != 200:
                to_log = format_logging_info(
                    {
                        "message": result.content.decode(),
                        "status_code": result.status_code,
                    }
                )
                if output_to_stdout:
                    print(to_log)
                else:
                    logger.debug(to_log)
        except Exception as e:
            logger.exception(e)
            time.sleep(timeout)
        else:
            return result
    return result
