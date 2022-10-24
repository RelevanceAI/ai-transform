from collections import UserList
from hashlib import new
from typing import Any, Dict, List, Union

from workflows_core.utils.document import Document


class DocumentList(UserList):
    data: List[Document]

    def __init__(self, initlist=None):
        if initlist is not None:
            for index, document in enumerate(initlist):
                if not isinstance(document, Document):
                    initlist[index] = Document(document)
        super().__init__(initlist)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, key: Union[str, int]) -> Union[Document, "DocumentList"]:
        if isinstance(key, str):
            return [document[key] for document in self.data]
        elif isinstance(key, slice):
            return self.__class__(self.data[key])
        elif isinstance(key, int):
            return self.data[key]

    def __setitem__(self, key: Union[str, int], value: Union[Any, List[Any]]):
        if isinstance(key, str):
            if isinstance(value, list):
                for document, value in zip(self.data, value):
                    document[key] = value
            else:
                for document in self.data:
                    document[key] = value
        elif isinstance(key, int):
            self.data[key] = value

    def to_json(self):
        return [document.to_json() for document in self.data]

    def remove_tag(self, field: str, tag: str, label_field: str = "label") -> None:
        for document in self.data:
            new_tags = []
            for tag_json in document[field]:
                if tag_json[label_field] != tag:
                    new_tags.append(tag_json)
            document[field] = new_tags

    def append_tag(
        self, field: str, value: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:
        if isinstance(value, list):
            for document, tag in zip(self.data, value):
                document[field].append(tag)
        else:
            for document in self.data:
                document[field].append(value)

    def sort_tags(
        self, field: str, sort_field: str = "label", reverse: bool = False
    ) -> None:
        for document in self.data:
            document[field] = sorted(
                document[field],
                key=lambda tag_json: tag_json[sort_field],
                reverse=reverse,
            )
