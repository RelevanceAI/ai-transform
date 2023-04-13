import time
import logging
import requests

from json import JSONDecodeError
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
    key_for_error: str = None,
    is_json_decodable: bool = False,
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
                to_log = format_logging_info({"message": result.content.decode(), "status_code": result.status_code})
                if output_to_stdout:
                    print(to_log)
                raise ValueError(to_log)

            if retry_func(result):
                to_log_for_retry = "Manual Retry Triggered..."
                if output_to_stdout:
                    print(to_log_for_retry)
                raise ManualRetry(to_log_for_retry)

            if is_json_decodable or key_for_error:
                try:
                    json_response = result.json()
                    if key_for_error in json_response:
                        raise KeyError
                except JSONDecodeError:
                    error_message = "Response is not JSON decodable"
                    if output_to_stdout:
                        print(error_message)
                    raise JSONDecodeError(error_message)
                except KeyError:
                    error_message = f"{key_for_error} not in JSON response"
                    if output_to_stdout:
                        print(error_message)
                    raise KeyError(error_message)

        except (Exception, ManualRetry, JSONDecodeError, KeyError) as e:
            logger.exception(e)
            time.sleep(timeout * exponential_backoff**n)
        else:
            return result

    return result
