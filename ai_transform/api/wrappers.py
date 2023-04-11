import time
import logging
import requests

from ai_transform.logger import format_logging_info
from requests.models import Response

from typing import Union, Sequence, Mapping, Callable, Any

logger = logging.getLogger(__file__)
logging.basicConfig()


class ManualRetry(Exception):
    pass


def request_wrapper(
    fn: Union[requests.get, requests.post],
    args: Sequence = None,
    kwargs: Mapping[str, Any] = None,
    num_retries: int = 3,
    timeout: int = 30,
    output_to_stdout: bool = False,  # support output to stdout to ensure logging is working
    exponential_backoff: float = 1,
    retry_func: Callable = None,
) -> Response:

    if args is None:
        args = ()

    if kwargs is None:
        kwargs = {}

    if retry_func is None:
        retry_func = lambda result: False

    for n in range(1, num_retries + 1):
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

                raise ValueError(to_log)

            if retry_func(result):
                to_log_for_retry = "Manual Retry Triggered..."
                if output_to_stdout:
                    print(to_log_for_retry)
                else:
                    logger.debug(to_log_for_retry)
                raise ManualRetry

        except (Exception, ManualRetry) as e:
            logger.exception(e)
            time.sleep(timeout * exponential_backoff**n)
        else:
            return result

    return result
