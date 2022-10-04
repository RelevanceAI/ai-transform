import pandas as pd

from typing import Any


class Document(dict):
    def set(self, key: str, value: Any) -> None:
        try:
            fields = key.split(".")
            d = self
            for i, f in enumerate(fields):
                if i == len(fields) - 1:
                    d[f] = value
                else:
                    if f in d.keys():
                        d = d[f]
                    else:
                        if f not in d:
                            d.update({f: {}})
                        d = d[f]
        except:
            return super().__setitem__(key, value)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            recur = self
            for f in key.split("."):
                try:
                    recur = recur[f]
                except KeyError:
                    try:
                        return self[key]
                    except:
                        return default

            return recur

    def keys(self):
        try:
            df = pd.json_normalize(self, sep=".")
            return list(set(list(super().keys()) + list(df.columns)))
        except:
            return super().keys()

    def __contains__(self, key) -> bool:
        try:
            return key in self.keys()
        except:
            return super().__contains__(key)


class DocumentUtils:
    pass
