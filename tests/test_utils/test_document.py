from copy import deepcopy

from slim.utils import Document


class TestDocument:
    def test_cast(self, test_document: Document):
        assert test_document.get("field1.field2") == 1
        assert test_document.get("field3") == 3
        assert test_document.get("field4", 5) == 5

    def test_set(self, test_document: Document):
        v = test_document.get("field1.field2")

        dne = test_document.get("not_field", 3)
        assert dne == 3

        test_document.set("field4.field5", 0)

        value = test_document.get("field4.field5")
        test_document.set("field4.field5", value + 1)

        assert test_document.get("field4.field5") == value + 1

    def test_deepcopy(self, test_document: Document):
        copy = deepcopy(test_document)
        copy["field3"] = 4
        copy.set("field1.feild2", 4)

        assert test_document.get("field3") != copy.get("field3")
        assert test_document.get("field1.feild2") != copy.get("field1.feild2")
        assert copy.get("field1.feild2") == 4
        assert copy.get("field3") == 4

    def test_regular_dict(self, test_document: Document):
        test_document.set("field1", 1)
        assert test_document["field1"] == 1
