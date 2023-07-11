import os
import json
import uuid
import time
import base64
import random
import pytest
import string

from typing import List, Dict, Sequence

from ai_transform.api.client import Client
from ai_transform.api.helpers import process_token

from ai_transform.dataset.dataset import Dataset

from ai_transform.engine.stable_engine import StableEngine

from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.operator.dense_operator import DenseOperator

from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList
from ai_transform.utils.example_documents import (
    mock_documents,
    static_documents,
    tag_documents,
    generate_random_vector,
    incomplete_documents,
)


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


@pytest.fixture(scope="function")
def empty_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=False)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def full_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(mock_documents(20))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def partial_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    documents = mock_documents(1000)
    fields = ["sample_1_label", "sample_2_label", "sample_3_label"]
    for document in documents:
        for field in random.sample(fields, k=random.randint(1, 3)):
            document.pop(field)
    dataset.insert_documents(documents)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def simple_partial_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    documents = mock_documents(1000)
    fields = ["sample_1_label"]
    for document in documents:
        if random.random() < 0.5:
            document.pop(fields[0])
    dataset.insert_documents(documents)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def partial_dataset_with_outputs(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    documents = mock_documents(1000)
    fields = ["sample_1_label", "sample_2_label", "sample_3_label"]
    for document in documents:
        for field in random.sample(fields, k=random.randint(1, 3)):
            document.pop(field)
    for document in documents:
        for field in fields:
            if document.get(field) and random.random() < 0.5:
                document[field + "_output"] = document[field] + "_output"
    dataset.insert_documents(documents)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def mixed_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    documents = mock_documents(10)
    stripped = mock_documents(10)
    for document in stripped:
        document.pop("_chunk_")
    documents += stripped
    dataset.insert_documents(documents)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="class")
def static_dataset(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(static_documents(20))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def dense_input_dataset1(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(static_documents(2))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def dense_input_dataset2(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(mock_documents(100))
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def dense_output_dataset1(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def dense_output_dataset2(test_client: Client) -> Dataset:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    yield dataset
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_document() -> Document:
    raw_dict = {"field1": {"field2": 1}, "field3": 3}
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
                if "new_field" not in document:
                    document["new_field"] = 0

                document["new_field"] += 3

            return documents

    return ExampleOperator()


@pytest.fixture(scope="function")
def test_partial_operator() -> AbstractOperator:
    class PartialOperator(AbstractOperator):
        def __init__(self, fields):
            super().__init__(input_fields=fields, output_fields=[field + "_output" for field in fields])

        def transform(self, documents: DocumentList) -> DocumentList:
            """
            Main transform function
            """
            for input_field, output_field in zip(self.input_fields, self.output_fields):
                for document in documents:
                    if document.get(input_field):
                        document[output_field] = document[input_field] + "_output"

            return documents

    return PartialOperator


@pytest.fixture(scope="function")
def test_paid_operator() -> AbstractOperator:
    class ExampleOperator(AbstractOperator):
        def __init__(self):
            super().__init__()

        def transform(self, documents: DocumentList) -> DocumentList:
            """
            Main transform function
            """
            for document in documents:
                if "new_field" not in document:
                    document["new_field"] = 0

                document["new_field"] += 3
                self.n_processed_pricing += 1

            return documents

    return ExampleOperator()


@pytest.fixture(scope="function")
def test_dense_operator(dense_output_dataset1: Dataset, dense_output_dataset2: Dataset) -> DenseOperator:
    class TestDenseOperator(DenseOperator):
        def __init__(self, output_dataset_ids: Sequence[str]):
            self.output_dataset_ids = output_dataset_ids
            super().__init__()

        def transform(self, documents: DocumentList) -> DocumentList:
            """
            Main transform function
            """
            for document in documents:
                if "new_field" not in document:
                    document["new_field"] = 0

                document["new_field"] += 3

            return {dataset_id: documents for dataset_id in self.output_dataset_ids}

    output_dataset_ids = (dense_output_dataset1.dataset_id, dense_output_dataset2.dataset_id)
    return TestDenseOperator(output_dataset_ids)


@pytest.fixture(scope="function")
def test_chunk_dense_operator(dense_output_dataset1: Dataset, dense_output_dataset2: Dataset) -> DenseOperator:
    class TestDenseOperator(DenseOperator):
        def __init__(self, output_dataset_ids: Sequence[str]):
            self.output_dataset_ids = output_dataset_ids
            self._chunk_field = "_chunk_"
            self._text_field = "label"
            self._alias = "default"
            super().__init__()

        def transform(self, documents: List[Document]) -> List[Document]:
            outputs = []
            for document in documents:
                texts = document.get_chunk(chunk_field=self._chunk_field, field=self._text_field)
                text_vectors = [generate_random_vector() for _ in range(len(texts))]

                for sent_index, text_vector in enumerate(text_vectors):
                    outputs.append(
                        {
                            "_id": document["_id"] + f":{sent_index}",
                            f"{self._text_field}_{self._alias}_vector_": text_vector,
                            "_order": sent_index,
                        }
                    )

            return {dataset_id: documents for dataset_id in self.output_dataset_ids}

    output_dataset_ids = (dense_output_dataset1.dataset_id, dense_output_dataset2.dataset_id)
    return TestDenseOperator(output_dataset_ids)


@pytest.fixture(scope="function")
def test_engine(full_dataset: Dataset, test_operator: AbstractOperator) -> StableEngine:
    return StableEngine(dataset=full_dataset, operator=test_operator)


@pytest.fixture(scope="function")
def test_paid_engine(full_dataset: Dataset, test_paid_operator: AbstractOperator) -> StableEngine:
    return StableEngine(dataset=full_dataset, operator=test_paid_operator)


@pytest.fixture(scope="function")
def test_paid_engine_no_refresh(full_dataset: Dataset, test_paid_operator: AbstractOperator) -> StableEngine:
    return StableEngine(dataset=full_dataset, operator=test_paid_operator, refresh=False)


@pytest.fixture(scope="function")
def test_sentiment_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(incomplete_documents(20))
    time.sleep(1)
    job_id = str(uuid.uuid4())
    config = dict(
        job_id=job_id,
        authorizationToken=test_client.credentials.token,
        dataset_id=dataset_id,
        text_field="sample_1_label",
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_user_facing_error_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(mock_documents(20))
    time.sleep(1)
    job_id = str(uuid.uuid4())
    config = dict(
        job_id=job_id,
        dataset_id=dataset_id,
        authorizationToken=test_client.credentials.token,
        text_field="sample_1_description",
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_sentiment_workflow_document_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    time.sleep(1)
    job_id = str(uuid.uuid4())
    config = dict(
        job_id=job_id,
        authorizationToken=test_client.credentials.token,
        text_field="sample_1_label",
        documents=mock_documents(10).to_json(),
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token


@pytest.fixture(scope="function")
def test_simple_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(mock_documents(20))
    time.sleep(1)
    job_id = str(uuid.uuid4())
    config = dict(
        job_id=job_id, authorizationToken=test_client.credentials.token, dataset_id=dataset_id, send_email=True
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_cluster_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(incomplete_documents(20))
    job_id = str(uuid.uuid4())
    print(job_id)
    config = dict(
        job_id=job_id,
        authorizationToken=test_client.credentials.token,
        dataset_id=dataset_id,
        vector_fields=["sample_1_vector_"],
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture(scope="function")
def test_org_error_workflow_token(test_client: Client) -> str:
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id, expire=True)
    dataset.insert_documents(incomplete_documents(20))
    job_id = str(uuid.uuid4())
    print(job_id)
    config = dict(
        job_id=job_id,
        authorizationToken=test_client.credentials.token,
        dataset_id=dataset_id,
        vector_fields=["sample_1_vector_"],
    )
    config_string = json.dumps(config)
    config_bytes = config_string.encode()
    workflow_token = base64.b64encode(config_bytes).decode()
    yield workflow_token
    test_client.delete_dataset(dataset_id)


@pytest.fixture()
def test_keyphrases() -> List[Dict]:
    return [
        {
            "text": "word",
            "_id": "word",
            "ancestors": [],
            "parents": [],
            "level": 1,
            "keyphrase_score": 10,
            "frequency": 3,
            "metadata": {},
        },
        {
            "text": "cat",
            "_id": "cat",
            "ancestors": ["word"],
            "parents": ["word"],
            "level": 0,
            "keyphrase_score": 6.4,
            "frequency": 7,
            "metadata": {},
        },
    ]


@pytest.fixture()
def test_keyphrase_dataset(test_client: Client, test_dataset_id: str) -> Dataset:
    docs = mock_documents(100)
    dataset = test_client.Dataset((test_dataset_id), expire=True)
    dataset.insert_documents(docs, ingest_in_background=False)
    yield dataset
    dataset.delete()
