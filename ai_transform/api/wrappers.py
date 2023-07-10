import time
import logging
import requests
import traceback

from json import JSONDecodeError
from ai_transform.logger import format_logging_info, ic
from requests.models import Response
from typing import Union, Sequence, Mapping, Callable, Any

logger = logging.getLogger(__file__)
logging.basicConfig()


class ManualRetryError(Exception):
    pass


class ResultNotOKError(Exception):
    pass


class OrgEntitlementError(Exception):
    pass


def is_response_bad(
    result: Response,
    key_for_error: str = None,
    output_to_stdout: bool = False,  # support output to stdout to ensure logging is working
):
    try:
        json_response = result.json()
        if key_for_error in json_response:
            raise KeyError

    except JSONDecodeError as e:
        error_message = "Response is not JSON decodable"
        if output_to_stdout:
            ic(error_message)
        raise JSONDecodeError(e.msg, e.doc, e.pos)

    except KeyError:
        error_message = f"{key_for_error} not in JSON response"
        if output_to_stdout:
            ic(error_message)
        raise KeyError(error_message)


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

    result: requests.Response = None

    for n in range(1, num_retries + 1):
        try:
            result = fn(*args, **kwargs)

            if not result.ok:
                content = result.content.decode()
                status_code = result.status_code

                to_log = format_logging_info({"message": content, "status_code": result.status_code})
                if output_to_stdout:
                    ic(to_log)

                if status_code == 400 and "Organization Entitlement setting documents" in content:
                    raise OrgEntitlementError(to_log)

                raise ResultNotOKError(to_log)

            if retry_func(result):
                to_log_for_retry = "Manual Retry Triggered..."
                if output_to_stdout:
                    ic(to_log_for_retry)
                raise ManualRetryError(to_log_for_retry)

            if is_json_decodable or key_for_error:
                is_response_bad(result=result, key_for_error=key_for_error, output_to_stdout=output_to_stdout)

        except (ResultNotOKError, ManualRetryError, JSONDecodeError, KeyError, ConnectionResetError) as e:
            ic(traceback.format_exc())
            time.sleep(timeout * exponential_backoff**n)
        else:
            return result

    return result
