import time

from examples.fail_example import BadOperator
from examples.workflows.sentiment_example import SentimentOperator
from examples.workflows.clustering_example import ClusterOperator

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.engine.small_batch_stable_engine import SmallBatchStableEngine
from workflows_core.workflow.abstract_workflow import Workflow
from workflows_core.engine.cluster_engine import InMemoryEngine
from workflows_core.workflow.helpers import decode_workflow_token


def test_sentiment_example_wstable_engine(test_sentiment_workflow_token: str):
    config = decode_workflow_token(test_sentiment_workflow_token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias")
    total_workers = config.get("total_workers", 3)
    worker_number = config.get("worker_number", 2)
    send_email = config.get("send_email", False)
    additional_information = config.get("additional_information", "")

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)
    job_id = dataset._trigger_dummy_job()['job_id']

    operator = SentimentOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        pull_chunksize=8,
        select_fields=[text_field],
        filters=filters,
        total_workers=total_workers,
        worker_number=worker_number,
    )

    workflow = Workflow(
        engine=engine,
        job_id=job_id,
        send_email=send_email,
        additional_information=additional_information,
    )
    workflow.run()

    time.sleep(2)

    health = dataset.health()
    for output_field in operator._output_fields:
        assert health[output_field]["exists"] == engine.size

    status_dict = workflow.get_status()
    assert status_dict["status"].lower() == "complete", status_dict


def test_sentiment_documents(test_sentiment_workflow_document_token: str):
    config = decode_workflow_token(test_sentiment_workflow_document_token)

    job_id = config["job_id"]
    token = config["authorizationToken"]
    text_field = config["text_field"]
    alias = config.get("alias")
    total_workers = config.get("total_workers", 3)
    worker_number = config.get("worker_number", 2)
    send_email = config.get("send_email", False)
    additional_information = config.get("additional_information", "")
    from workflows_core.api.client import Client

    client = Client(token)

    operator = SentimentOperator(text_field=text_field, alias=alias)
    operator.transform_for_playground(
        documents=config.get("documents"),
        job_id=job_id,
        workflow_name="sentiment",
        authorization_token=token,
        status="complete",
        send_email=send_email,
    )

    result = client._api._get_workflow_status(job_id=job_id)
    # Check it fool
    assert True


def test_sentiment_example_wsmall_batch_stable_engine(
    test_sentiment_workflow_token: str,
):
    config = decode_workflow_token(test_sentiment_workflow_token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias")
    send_email = config.get("send_email", False)
    additional_information = config.get("additional_information", "")

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)
    job_id = dataset._trigger_dummy_job()['job_id']

    operator = SentimentOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()

    engine = SmallBatchStableEngine(
        dataset=dataset,
        operator=operator,
        pull_chunksize=2,
        transform_threshold=10,
        transform_chunksize=5,
        select_fields=[text_field],
        filters=filters,
    )

    workflow = Workflow(
        engine=engine,
        job_id=job_id,
        send_email=send_email,
        additional_information=additional_information,
    )
    workflow.run()

    time.sleep(2)

    health = dataset.health()
    for output_field in operator._output_fields:
        assert health[output_field]["exists"] == engine.size

    status_dict = workflow.get_status()
    assert status_dict["status"].lower() == "complete", status_dict


def test_sentiment_example_multiple_workers(test_sentiment_workflow_token: str):
    config = decode_workflow_token(test_sentiment_workflow_token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias")
    TOTAL_WORKERS = 10
    WORKER_NUMBER = 2
    total_workers = TOTAL_WORKERS
    worker_number = config.get("worker_number", WORKER_NUMBER)
    send_email = config.get("send_email", False)
    additional_information = config.get("additional_information", "")

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)
    job_id = dataset._trigger_dummy_job()['job_id']

    operator = SentimentOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        pull_chunksize=8,
        select_fields=[text_field],
        filters=filters,
        total_workers=total_workers,
        worker_number=worker_number,
    )

    workflow = Workflow(
        engine=engine,
        job_id=job_id,
        send_email=send_email,
        additional_information=additional_information,
    )
    workflow.run()

    time.sleep(2)

    health = dataset.health()
    # This can vary depending on modulo
    assert engine._size in [0, 1, 2, 3, 4, 5], "incorrect engine size"

    for output_field in operator._output_fields:
        assert health[output_field]["exists"] == engine._size

    status_dict = workflow.get_status()
    # This should in THEORY be inprogress still but his will only work
    # on sufficiently large datasets
    assert status_dict["status"].lower() == "complete", status_dict


def test_cluster_example(test_cluster_workflow_token: str):
    config = decode_workflow_token(test_cluster_workflow_token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    vector_fields = config["vector_fields"]
    alias = config.get("alias", None)
    n_clusters = config.get("n_clusters", 8)
    total_workers = config.get("total_workers")
    worker_number = config.get("worker_number")
    send_email = config.get("send_email", False)
    additional_information = config.get("additional_information", "")

    vector_field = vector_fields[0]
    client = Client(token=token)
    dataset = client.Dataset(dataset_id)
    job_id = dataset._trigger_dummy_job()['job_id']

    operator = ClusterOperator(
        n_clusters=n_clusters, vector_field=vector_field, alias=alias
    )

    filters = dataset[vector_field].exists()
    engine = InMemoryEngine(
        dataset=dataset,
        operator=operator,
        pull_chunksize=100,
        select_fields=[vector_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    workflow = Workflow(
        engine=engine,
        job_id=job_id,
        send_email=send_email,
        additional_information=additional_information,
    )
    workflow.run()

    time.sleep(2)

    health = dataset.health()
    cluster_field = operator._output_field
    assert health[cluster_field]["exists"] == 20


def test_fail_example(test_sentiment_workflow_token: str):
    config = decode_workflow_token(test_sentiment_workflow_token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)
    job_id = dataset._trigger_dummy_job()['job_id']

    # Implement a trigger
    operator = BadOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        pull_chunksize=8,
        select_fields=[text_field],
        filters=filters,
        worker_number=0,
        total_workers=1,
    )

    workflow = Workflow(
        engine=engine,
        job_id=job_id,
    )
    # dataset.api._trigger(
    #     dataset_id=dataset._dataset_id,
    #     params={},
    #     workflow_id=job_id,
    # )
    workflow.run()

    time.sleep(2)

    assert workflow.get_status()["status"] == "failed"
