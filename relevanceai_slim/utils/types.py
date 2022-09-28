from typing import Any, Dict, List, NamedTuple, NewType


Vector = NewType("Vector", List[float])

Document = NewType("Document", Dict[str, Any])


class Credentials(NamedTuple):
    project: str
    api_key: str
    region: str
    firebase_uid: str
