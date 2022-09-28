from typing import Optional, Union

from slim.types import Filter
from slim.dataset.dataset import Dataset


class Series:
    def __init__(self, dataset: Dataset, field: str):
        self._dataset = dataset
        self._field = field
        self._dtype = dataset.schema[field]
        self._filter_type = self._get_filter_type()

    def _get_filter_type(self):
        if self._dtype == "numeric":
            filter_type = "numeric"
        elif self._dtype == "date":
            filter_type = "date"
        else:
            filter_type = "exact_match"
        return filter_type

    def __eq__(
        self,
        other: Union[str, float, int, bool, None],
        filter_type: Optional[str] = None,
    ) -> Filter:
        if filter_type is None:
            filter_type = self._filter_type
        return [
            {
                "field": self._field,
                "filter_type": filter_type,
                "condition": "==",
                "condition_value": other,
            }
        ]
