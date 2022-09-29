import pytest
import random

from slim.dataset.dataset import Dataset
from slim.utils import mock_documents


@pytest.mark.usefixtures("empty_dataset")
class TestDataset1:
    def test_create_delete(self, empty_dataset: Dataset):
        empty_dataset.delete()
        empty_dataset.create()
        assert True

    def test_insert(self, empty_dataset: Dataset):
        documents = mock_documents(100)
        result = empty_dataset.insert_documents(documents)
        assert result["inserted"] == 100


@pytest.mark.usefixtures("full_dataset")
class TestDataset2:
    def test_schema(self, full_dataset: Dataset):
        documents = mock_documents(5)
        keys = documents[0].keys()
        schema = full_dataset.schema
        assert all([key in schema for key in keys])

    def test_series(self, full_dataset: Dataset):
        schema = full_dataset.schema
        series = full_dataset[random.choice(list(schema.keys()))]
        assert True


@pytest.mark.usefixtures("static_dataset")
class TestFilters:
    def test_equals(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"] == 5
        res = static_dataset.get_documents(page_size=1, filters=filters)
        documents = res["documents"]
        assert res["count"] == 1
        assert documents[0]["numeric_field"] == 5

    @pytest.mark.xfail(reason="api bug")
    def test_less_than(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"] < 5
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 5

    @pytest.mark.xfail(reason="api bug")
    def test_greater_than(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"] > 5
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 14

    def test_less_than_equal_to(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"] >= 5
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 15

    def test_greater_than_equal_to(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"] <= 5
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 6

    def test_exists(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"].exists()
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 20

    def test_not_exists(self, static_dataset: Dataset):
        filters = static_dataset["numeric_field"].not_exists()
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 0

    def test_contains(self, static_dataset: Dataset):
        filters = static_dataset["text_field"].contains("3")
        res = static_dataset.get_documents(page_size=20, filters=filters)
        documents = res["documents"]
        values = list(map(lambda document: document["text_field"], documents))
        assert res["count"] == 2
        assert all([value in values for value in {"3", "13"}])

    def test_ids(self, static_dataset: Dataset):
        res = static_dataset.get_documents(page_size=20)
        _ids = list(map(lambda document: document["_id"], res["documents"]))
        filters = static_dataset["_id"] == random.choice(_ids)
        res = static_dataset.get_documents(page_size=20, filters=filters)
        assert res["count"] == 1
