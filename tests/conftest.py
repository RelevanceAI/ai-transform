import os
import random
import string
import pytest

from typing import Any, List, Optional

from slim import Client, Dataset
from slim.api import process_token
from slim.utils import Document, mock_documents, static_documents
from slim.operator import AbstractOperator
from slim.engine import AbstractEngine


TEST_TOKEN = os.getenv("TEST_TOKEN")
test_creds = process_token(TEST_TOKEN)


@pytest.fixture(scope="session")
def test_token() -> str:
    return TEST_TOKEN


@pytest.fixture(scope="session")
def test_client(test_token: str):
    return Client(test_token)


@pytest.fixture(scope="function")
def test_dataset_id():
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    return dataset_id


@pytest.fixture(scope="class")
def empty_dataset(test_client: Client):
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def full_dataset(test_client: Client):
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents(20))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def static_dataset(test_client: Client):
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(static_documents(20))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_document():
    raw_dict = {
        "field1": {"field2": 1},
        "field3": 3,
    }
    return Document(raw_dict)


@pytest.fixture(scope="function")
def test_operator():
    class ExampleOperator(AbstractOperator):
        def __init__(self, field: Optional[str] = None):
            self._field = field

        def transform(self, documents: List[Document]) -> List[Document]:
            """
            Main transform function
            """

            for document in documents:
                document.set(self._field, document.get(self._field) + 1)

            return documents

    return ExampleOperator()


@pytest.fixture(scope="function")
def test_engine(full_dataset: Dataset, test_operator: AbstractOperator):
    class TestEngine(AbstractEngine):
        def apply(self) -> Any:

            for _ in range(self.nb):
                batch = self.get_chunk()
                new_batch = self.operator(batch)
                self.update_chunk(new_batch)

            return

    return TestEngine(full_dataset, test_operator)
