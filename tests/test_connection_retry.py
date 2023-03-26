import time
from json import JSONDecodeError
from ai_transform.api.api import retry

@retry()
def fail_function():
    raise JSONDecodeError("Intended to fail", "", 2)

def test_retry_error():
    t1 = time.time()
    try:
        fail_function()
    except Exception as e:
        pass
    t2 = time.time()
    assert (t2 - t1 )> 5
