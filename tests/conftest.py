import os
import random
import string
from typing import List
import pytest


from slim import Client
from slim.api.helpers import process_token
from slim.utils.random import mock_documents, static_documents

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
