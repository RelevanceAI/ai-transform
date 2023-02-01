import re
import uuid

from typing import List
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

            def get_fields(d, parent_key: str = "", fields: List[str] = None):
                """
                ChatGPT wrote this function
                """
                if parent_key != "" and parent_key not in fields:
                    fields.append(parent_key)
                if fields is None:
                    fields = []
                if isinstance(d, dict):
                    for k, v in d.items():
                        key = parent_key + k
                        if isinstance(v, (dict, list)):
                            get_fields(v, key + ".", fields)
                        else:
                            fields.append(key)
                elif isinstance(d, list):
                    for item in d:
                        get_fields(item, parent_key, fields)
                return fields

            return get_fields(self.data)
        except:
            return super().keys()

    def __contains__(self, key) -> bool:
        try:
            return key in self.keys()
        except:
            return super().__contains__(key)

    def to_json(self):
        return json_encoder(deepcopy(self.data))

    def list_chunks(self):
        """
        List the available chunks inside of the document.
        """
        # based on conversation with API team
        return [k for k in self.keys() if k.endswith("_chunk_")]

    def get_chunk(self, chunk_field: str, field: str = None, default: str = None):
        """
        Returns a list of values.
        """
        # provide a recursive implementation for getting chunks
        from workflows_core.utils.document_list import DocumentList

        document_list = DocumentList(self.get(chunk_field, default=default))
        # Get the field across chunks
        if field is None:
            return document_list
        return [d.get(field, default=default) for d in document_list.data]

    def _create_chunk_documents(
        self,
        field: str,
        values: list,
        generate_id: bool = False,
    ):
        """
        create chunk documents based on a given field and value.
        """
        from workflows_core.utils.document_list import DocumentList

        if generate_id:
            docs = [
                {"_id": uuid.uuid4().__str__(), field: values[i], "_order_": i}
                for i in range(len(values))
            ]
        else:
            docs = [{field: values[i], "_order_": i} for i in range(len(values))]
        return DocumentList(docs)

    def _calculate_offset(self, text_to_find, string):
        result = [
            {"start": m.start(), "end": m.end()}
            for m in re.finditer(text_to_find, string)
        ]
        return result

    def set_chunk(
        self,
        chunk_field: str,
        field: str,
        values: list,
        generate_id: bool = False,
    ):
        """
        doc.list_chunks()
        doc.get_chunk("value_chunk_", field="sentence") # returns a list of values
        doc.set_chunk("value_chunk_", field="sentence", values=["hey", "test"])
        """
        # We use upsert behavior for now
        from workflows_core.utils.document_list import DocumentList

        new_chunk_docs = self._create_chunk_documents(
            field,
            values=values,
            generate_id=generate_id,
        )
        # Update on the old chunk docs
        old_chunk_docs = DocumentList(self.get(chunk_field))
        # Relying on immutable property
        [d.update(new_chunk_docs[i]) for i, d in enumerate(old_chunk_docs.data)]

    def split(
        self,
        split_operation: callable,
        chunk_field: str,
        field: str,
        default: Any = None,
        include_offsets: bool = True,
        generate_id: bool = False,
    ):
        """
        The split operation is as follows:

        The split operation returns to us a list of possible values.
        The chunk documents are then created automatically for you.
        """
        if default is None:
            default = []
        value = self.get(field, default)
        split_values = split_operation(value)
        chunk_documents = self._create_chunk_documents(
            field=field, values=split_values, generate_id=generate_id
        )

        if include_offsets:
            for i, d in enumerate(chunk_documents):
                offsets = self._calculate_offset(d[field], value)
                d["_offsets_"] = offsets

        self.set(chunk_field, chunk_documents)

    def operate_on_chunk(
        self,
        operator_function: callable,
        chunk_field: str,
        field: str,
        output_field: str,
        default: Any = None,
    ):
        """
        Add an operate function.
        """
        values = self.get_chunk(chunk_field=chunk_field, field=field, default=default)
        results = operator_function(values)
        self.set_chunk(chunk_field=chunk_field, field=output_field, values=results)
