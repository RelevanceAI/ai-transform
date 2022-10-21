from collections import UserList
from typing import Any, List, Union

from workflows_core.utils.document import Document


class Documents(UserList):
    data: List[Document]

    def __init__(self, initlist=None):
        if initlist is not None:
            initlist = [Document(document) for document in initlist]
        super().__init__(initlist)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, key: Union[str, int]):
        if isinstance(key, str):
            return [document[key] for document in self.data]
        if isinstance(key, slice):
            return self.__class__(self.data[key])

    def __setitem__(self, key: Union[str, int], value: Union[Any, List[Any]]):
        if isinstance(key, str):
            assert len(value) == len(self.data)
            for document, value in zip(self.data, value):
                document[key] = value
        elif isinstance():
            self.data[key] = value

    def serialize(self):
        return [document.to_dict() for document in self.data]
