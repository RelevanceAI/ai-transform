import os
import json
import uuid
import base64
import random
import pytest
import string
import time

from typing import List, Dict

from workflows_core.api.client import Client
from workflows_core.dataset.dataset import Dataset
from workflows_core.api.helpers import process_token
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.utils.document import Document
from workflows_core.utils.document_list import DocumentList
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.utils.example_documents import (
    mock_documents,
    static_documents,
    tag_documents,
)


TEST_TOKEN = os.getenv("TEST_TOKEN")
test_creds = process_token(TEST_TOKEN)

rd = random.Random()
rd.seed(0)


def create_id():
    # This makes IDs reproducible for tests related to Modulo function
    return str(uuid.UUID(int=rd.getrandbits(128)))


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
def test_documents() -> DocumentList:
    return mock_documents()


@pytest.fixture(scope="function")
def test_tag_documents() -> DocumentList:
    return tag_documents()


@pytest.fixture(scope="function")
def test_operator() -> AbstractOperator:
    class ExampleOperator(AbstractOperator):
        def transform(self, documents: DocumentList) -> DocumentList:
            """
            Main transform function
            """

            for document in documents:
                document["new_field"] = 3

            return documents

    return ExampleOperator()


@pytest.fixture(scope="function")
def test_engine(full_dataset: Dataset, test_operator: AbstractOperator) -> StableEngine:
    return StableEngine(
        dataset=full_dataset,
        operator=test_operator,
    )


@pytest.fixture(scope="function")
def test_sentiment_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents(20))
    time.sleep(1)
    job_id = str(uuid.uuid4())
    config = dict(
        job_id=job_id,
        authorizationToken=test_client._token,
        dataset_id=dataset_id,
        text_field="sample_1_label",
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    # test_client._api._trigger(
    #     dataset_id,
    #     params=config,
    #     workflow_id=job_id,
    # )
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_simple_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents(20))
    time.sleep(1)
    job_id = str(uuid.uuid4())
    config = dict(
        job_id=job_id,
        authorizationToken=test_client._token,
        dataset_id=dataset_id,
        send_email=True,
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    # test_client._api._trigger(
    #     dataset_id,
    #     params=config,
    #     workflow_id=job_id,
    # )
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_cluster_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents(20))
    job_id = str(uuid.uuid4())
    print(job_id)
    config = dict(
        job_id=job_id,
        authorizationToken=test_client._token,
        dataset_id=dataset_id,
        vector_fields=["sample_1_vector_"],
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    # test_client._api._trigger(
    #     dataset_id,
    #     params=config,
    #     workflow_id=job_id,
    # )
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture()
def test_keyphrases() -> List[Dict]:
    return [
        {
            "_id": "doc_1",
            "keyphrase_score": 10,
            #"parent_document": "test_parent",
            "text": "word",
        },
        {
            "_id": "doc_2",
            # "keyphrase": "word",
            "keyphrase_score": 10,
            "text": "word",
        },
    ]


@pytest.fixture()
def test_keyphrase_dataset(test_client: Client, test_dataset_id: str) -> Dataset:
    docs = mock_documents(100)
    dataset = test_client.Dataset(test_dataset_id)
    dataset.insert_documents(docs, ingest_in_background=False)
    yield dataset
    dataset.delete()
