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
