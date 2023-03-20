from typing import Callable, Sequence, Mapping, Any


def openai_wrapper(
    fn: Callable,
    args: Sequence = None,
    kwargs: Mapping[str, Any] = None,
):
    result = fn(*args, **kwargs)
    return result
