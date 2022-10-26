import pandas as pd

from copy import deepcopy
from typing import Any, Optional
from collections import UserDict

from workflows_core.utils.json_encoder import json_encoder


class Document(UserDict):
    def __repr__(self):
        return repr(self.data)

    def __setitem__(self, key: Any, value: Any) -> None:
        try:
            fields = key.split(".")
        except:
            super().__setitem__(key, value)
        else:
            # Assign a pointer.
            pointer = self.data
            for depth, field in enumerate(fields):
                # Assign the value if this is the last entry e.g. stores.fastfood.kfc.item will be item
                if depth == len(fields) - 1:
                    pointer.__setitem__(field, value)
                else:
                    if field in pointer.keys():
                        pointer = pointer.__getitem__(field)
                    else:
                        pointer.update({field: {}})
                        pointer = pointer.__getitem__(field)

    def __getitem__(self, key: Any) -> Any:
        try:
            fields = key.split(".")
        except:
            return super().__getitem__(key)
        else:
            pointer = self.data
            for depth, field in enumerate(fields):
                if depth == len(fields) - 1:
                    return pointer.__getitem__(field)
                else:
                    pointer = pointer.__getitem__(field)

    def get(self, key: Any, default: Optional[Any] = None) -> Any:
        try:
            return self.__getitem__(key)
        except:
            return default

    def set(self, key: Any, value: Any) -> None:
        self.__setitem__(key, value)

    def keys(self):
        try:
            df = pd.json_normalize(self.data, sep=".")
            return list(df.columns)
        except:
            return super().keys()

    def __contains__(self, key) -> bool:
        try:
            return key in self.keys()
        except:
            return super().__contains__(key)

    def to_json(self):
        return json_encoder(deepcopy(self.data))
