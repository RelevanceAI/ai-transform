import os
import json
import base64
import random
import string
import uuid
import pytest

from typing import Any, List

from workflows_core.api.client import Client
from workflows_core.dataset.dataset import Dataset
from workflows_core.api.helpers import process_token
from workflows_core.utils.document import Document
from workflows_core.utils.random import mock_documents, static_documents
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.engine.abstract_engine import AbstractEngine


TEST_TOKEN = os.getenv("TEST_TOKEN")
test_creds = process_token(TEST_TOKEN)


@pytest.fixture(scope="session")
def test_token() -> str:
    return TEST_TOKEN


@pytest.fixture(scope="session")
def test_client(test_token: str) -> Client:
    return Client(test_token)


@pytest.fixture(scope="function")
def test_dataset_id() -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    return dataset_id


@pytest.fixture(scope="class")
def empty_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def full_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents(20))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def static_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(static_documents(20))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_document() -> Document:
    raw_dict = {
        "field1": {"field2": 1},
        "field3": 3,
    }
    return Document(raw_dict)


@pytest.fixture(scope="function")
def test_operator() -> AbstractOperator:
    class ExampleOperator(AbstractOperator):
        def transform(self, documents: List[Document]) -> List[Document]:
            """
            Main transform function
            """

            for document in documents:
                document["new_field"] = 3

            return documents

    return ExampleOperator()


@pytest.fixture(scope="function")
def test_engine(
    full_dataset: Dataset, test_operator: AbstractOperator
) -> AbstractEngine:
    class TestEngine(AbstractEngine):
        def apply(self) -> Any:

            for chunk in self.iterate():
                new_batch = self.operator(chunk)
                self.update_chunk(new_batch)

            return

    return TestEngine(full_dataset, test_operator)


@pytest.fixture(scope="function")
def test_sentiment_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents(20))
    config = dict(
        job_id=str(uuid.uuid4()),
        authorizationToken=test_client._token,
        dataset_id=dataset_id,
        text_field="sample_1_label",
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token
    test_client.delete_dataset(dataset_id)
