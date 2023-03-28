import time
from json import JSONDecodeError
from ai_transform.api.client import Client
from ai_transform.api.api import retry


@retry()
def fail_function(test_client: Client):
    raise ConnectionResetError(104, "Connection reset by peer")


def test_retry_error(test_client):
    t1 = time.time()
    try:
        fail_function(test_client)
    except Exception as e:
        print(e)
        pass
    t2 = time.time()
    assert (t2 - t1) > 20