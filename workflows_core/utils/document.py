import re
import uuid

from typing import Dict, Any
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
            obj = self.data
            for curr_field, next_field in zip(fields, fields[1:] + [None]):
                if next_field is not None:
                    if curr_field.isdigit():
                        curr_field = int(curr_field)
                        try:
                            obj = obj[curr_field]
                        except:
                            if not len(obj):
                                obj.append({})
                            if curr_field >= len(obj):
                                curr_field = len(obj) - 1
                            obj = obj[curr_field]
                        continue

                    if next_field.isdigit():
                        curr_val = obj.get(curr_field, [{}])
                        next_val = curr_val[min(int(next_field), len(curr_val) - 1)]
                        if not isinstance(next_val.get(next_field), list):
                            obj[curr_field] = [next_val]
                            obj = obj[curr_field]

                    elif not next_field.isdigit() and not isinstance(
                        obj.get(curr_field, {}).get(next_field), dict
                    ):
                        obj[curr_field] = {}
                        obj = obj[curr_field]

        obj[fields[-1]] = value

    def __getitem__(self, key: Any) -> Any:
        try:
            fields = key.split(".")
        except:
            return super().__getitem__(key)
        else:
            obj = self.data
            for curr_field in fields[:-1]:
                if curr_field.isdigit():
                    curr_field = int(curr_field)
                obj = obj[curr_field]
            return obj[fields[-1]]

    def get(self, key: Any, default: Optional[Any] = None) -> Any:
        try:
            return self.__getitem__(key)
        except:
            return default

    def set(self, key: Any, value: Any) -> None:
        self.__setitem__(key, value)

    def keys(self, detailed: bool = True):
        def get_keys(dictionary: Dict[str, Any], prefix=""):
            keys = []
            for key, value in dictionary.items():
                current_key = prefix + "." + key if prefix else key
                if isinstance(value, dict):
                    keys.extend(get_keys(value, current_key))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            keys.extend(get_keys(item, current_key + "." + str(i)))
                    keys.append(current_key)
                else:
                    keys.append(current_key)

            return keys

        keys = set(get_keys(self.data))

        keys_to_add = set()
        for key in keys:
            subkeys = key.split(".")
            for i in range(1, len(subkeys)):
                keys_to_add.add(".".join(subkeys[:i]))
        keys.update(keys_to_add)

        if detailed:
            keys_to_add = set()
            for key in keys:
                subkeys = key.split(".")
                for i in range(1, len(subkeys) + 1):
                    keys_to_add.add(
                        ".".join([k for k in subkeys[:i] if not k.isdigit()])
                    )
            keys.update(keys_to_add)

        return list(sorted(keys))

    def __contains__(self, key) -> bool:
        return key in self.keys()

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
