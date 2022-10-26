import warnings

from collections import UserList
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

    def remove_tag(self, field: str, value: str) -> None:
        warnings.warn("This behaivour is experimental and is subject to change")

        *tag_fields, remove_field = field.split(".")
        tag_field = ".".join(tag_fields)

        for document in self.data:
            new_tags = []

            old_tags = document.get(tag_field, [])
            for tag_json in old_tags:
                if tag_json.get(remove_field) != value:
                    new_tags.append(tag_json)

            document[tag_field] = new_tags

    def append_tag(
        self, field: str, value: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:
        warnings.warn("This behaivour is experimental and is subject to change")

        if isinstance(value, list):
            for document, tag in zip(self.data, value):
                document[field].append(tag)
        else:
            for document in self.data:
                document[field].append(value)

    def sort_tags(self, field: str, reverse: bool = False) -> None:
        warnings.warn("This behaivour is experimental and is subject to change")

        *tag_fields, sort_field = field.split(".")
        tag_field = ".".join(tag_fields)

        for document in self.data:
            tags = document.get(tag_field)

            if tags is not None:
                document[tag_field] = sorted(
                    document[tag_field],
                    key=lambda tag_json: tag_json[sort_field],
                    reverse=reverse,
                )
