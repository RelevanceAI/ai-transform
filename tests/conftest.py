import os
import random
import string
from typing import List
import pytest


from slim import Client
from slim.api.helpers import process_token
from slim.types import Document

TEST_TOKEN = os.getenv("TEST_TOKEN")
test_creds = process_token(TEST_TOKEN)

TEST_TOKEN = f"{test_creds.project}:{test_creds.api_key}:{test_creds.api_key}:relevanceai-sdk-test-user"


@pytest.fixture
def test_token() -> str:
    return TEST_TOKEN


@pytest.fixture
def test_client(test_token: str):
    return Client(test_token)


@pytest.fixture(scope="function")
def test_dataset(test_client: Client, test_documents: List[Document]):
    salt = "".join(random.choices(string.ascii_lowercase, k=10))
    dataset_id = f"_sample_dataset_{salt}"
    dataset = test_client.Dataset(dataset_id)
    dataset.insert_documents(test_documents)
    yield test_client.Dataset(dataset_id)
    test_client.delete_dataset(dataset_id)
