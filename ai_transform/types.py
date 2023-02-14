from typing import Any, Dict, List, NamedTuple, NewType


Vector = NewType("Vector", List[float])

Filter = NewType("Filter", Dict[str, Any])

FieldTransformer = NewType("FieldTransformer", Dict[str, Any])

Schema = NewType("Schema", Dict[str, str])


class Credentials(NamedTuple):
    project: str
    api_key: str
    region: str
    firebase_uid: str

    @property
    def token(self):
        return f"{self.project}:{self.api_key}:{self.region}:{self.firebase_uid}"


GroupBy = NewType("GroupBy", List[Dict[str, Any]])
Metric = NewType("Metric", List[Dict[str, Any]])
