from copy import deepcopy

from ai_transform.utils.document import Document


class TestDocument:
    def test_cast(self, test_document: Document):
        assert test_document["field1.field2"] == 1
        assert test_document["field3"] == 3
        assert test_document.get("field4", 5) == 5

    def test_set(self, test_document: Document):
        v = test_document.get("field1.field2")

        dne = test_document.get("not_field", 3)
        assert dne == 3

        test_document["field4.field5"] = 0

        value = test_document["field4.field5"]
        test_document["field4.field5"] = value + 1

        assert test_document["field4.field5"] == value + 1

    def test_deepcopy(self, test_document: Document):
        copy = deepcopy(test_document)
        copy["field3"] = 4
        copy["field1.field2"] = 4

        assert test_document["field3"] != copy["field3"]
        assert test_document["field1.field2"] != copy["field1.field2"]
        assert copy["field1.field2"] == 4
        assert copy["field3"] == 4

    def test_regular_dict(self, test_document: Document):
        test_document["field1"] = 1
        assert test_document["field1"] == 1

    def test_inplace(self, test_document: Document):
        test_document["field1.field2"] += 4
        assert test_document["field1.field2"] == 5

    def test_keys(self, test_document: Document):
        assert "field1" in test_document
        assert "field1.field2" in test_document

    def test_pop(self, test_document: Document):
        import pdb

        pdb.set_trace()
        value = test_document.pop("field1.field2")
        assert value == 1
        assert "field1" in test_document
        assert "field1.field2" not in test_document

    def test_split(self):
        from ai_transform.utils import Document

        doc = Document(
            {
                "sentence": "This is going to be an interesting time for us all. Won't it be?"
            }
        )

        from sentence_splitter import split_text_into_sentences
        from functools import partial

        def split_text_into_sentences_max(
            text, language, max_number_of_chunks: int = 20
        ):
            if max_number_of_chunks == 0:
                return split_text_into_sentences(text, language=language)
            else:
                return split_text_into_sentences(text, language=language)[
                    :max_number_of_chunks
                ]

        split_function = partial(split_text_into_sentences_max, language="en")

        documents = [doc]

        [
            d.split(split_function, chunk_field="_chunks_.sentence", field="sentence")
            for d in documents
        ]

        assert documents[0] == {
            "sentence": "This is going to be an interesting time for us all. Won't it be?",
            "_chunks_": {
                "sentence": [
                    {
                        "sentence": "This is going to be an interesting time for us all.",
                        "_order_": 0,
                        "_offsets_": [{"start": 0, "end": 51}],
                    },
                    {
                        "sentence": "Won't it be?",
                        "_order_": 1,
                        "_offsets_": [{"start": 52, "end": 63}],
                    },
                ]
            },
        }
