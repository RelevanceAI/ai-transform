import os
import pytest
from typing import Callable, List, Optional
from ai_transform.api.client import Client
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import AbstractWorkflow
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.example_documents import Document
from ai_transform.workflow.context_manager import WORKFLOW_FAIL_MESSAGE
from ai_transform.utils import mock_documents


class BadOperator(AbstractOperator):
    def __init__(
        self, text_field: str, model: str = "cardiffnlp/twitter-roberta-base-sentiment", alias: Optional[str] = None
    ):
        super().__init__()

    def transform(self, documents: List[Document]) -> List[Document]:
        pass


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    job_id = config["job_id"]
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)
    total_workers = config.get("total_workers", None)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = BadOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        select_fields=[text_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
        check_for_missing_fields=False,
    )

    workflow = AbstractWorkflow(engine=engine, job_id=job_id)
    try:
        workflow.raise_user_facing_error("Text field not right.")
    except Exception as e:
        # this raises an error in the workflow
        pass
    status = workflow.get_status()
    assert "user_errors" in status, status


def test_user_facing_error():
    from ai_transform.workflow.helpers import encode_config

    config = {
        "text_field": "sample_1_label",
        "alias": "check",
        "job_id": "test",
        "authorizationToken": os.getenv("TEST_TOKEN"),
        "dataset_id": "sample",
    }
    token = encode_config(config)
    execute(token, None)


class PartialBadOperator(AbstractOperator):
    def __init__(
        self, text_field: str, model: str = "cardiffnlp/twitter-roberta-base-sentiment", alias: Optional[str] = None
    ):
        self._counter = 0
        super().__init__()

    def transform(self, documents: List[Document]) -> List[Document]:
        self._counter += 1
        if self._counter % 2 == 0:
            raise ValueError("Incorrect")
        return documents


def execute_partial_error(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    job_id = config["job_id"]
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)
    total_workers = config.get("total_workers", None)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = PartialBadOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        select_fields=[text_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
        check_for_missing_fields=False,
    )

    workflow = AbstractWorkflow(engine=engine, job_id=job_id)
    workflow.run()
    status = workflow.get_status()
    assert "user_errors" in status, status
    assert WORKFLOW_FAIL_MESSAGE[:10] in status["user_errors"], status["user_errors"]


@pytest.fixture
def setup_dataset(test_client: Client) -> Client.Dataset:
    simple_workflow_dataset = test_client.Dataset("test-simple-workflow-dataset", expire=True)
    docs = mock_documents()
    simple_workflow_dataset.insert_documents(docs)
    yield simple_workflow_dataset
    simple_workflow_dataset.delete()


def test_user_facing_error_partial_bad_operator(setup):
    from ai_transform.workflow.helpers import encode_config

    dataset_id = setup._dataset_id

    config = {
        "text_field": "sample_1_label",
        "alias": "check",
        "job_id": "test",
        "authorizationToken": os.getenv("TEST_TOKEN"),
        "dataset_id": dataset_id,
    }
    token = encode_config(config)
    execute_partial_error(token, None)
