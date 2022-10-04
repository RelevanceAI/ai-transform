import warnings
from .abstract_operator import AbstractOperator

try:
    from .ray_operator import AbstractRayOperator
except ModuleNotFoundError:
    warnings.warn(f"run `pip install .[ray]`")
