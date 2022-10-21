import time

from examples.workflows.sentiment_example import SentimentOperator
from examples.workflows.clustering_example import ClusterOperator

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.engine.cluster_engine import ClusterEngine
from workflows_core.workflow.helpers import decode_workflow_token


def test_sentiment_example(test_sentiment_workflow_token: str):
    config = decode_workflow_token(test_sentiment_workflow_token)

    job_id = config["job_id"]
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias")
    total_workers = config.get("total_workers")
    worker_number = config.get("worker_number")

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = SentimentOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=8,
        select_fields=[text_field],
        filters=filters,
        total_workers=total_workers,
        worker_number=worker_number,
    )

    workflow = AbstractWorkflow(
        engine=engine,
        job_id=job_id,
    )
    workflow.run()

    time.sleep(2)

    health = dataset.health()
    for output_field in operator._output_fields:
        assert health[output_field]["exists"] == engine.size

    status_dict = workflow.get_status()
    assert status_dict["status"].lower() == "complete"


def test_cluster_example(test_cluster_workflow_token: str):
    config = decode_workflow_token(test_cluster_workflow_token)

    job_id = config["job_id"]
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    vector_fields = config["vector_fields"]
    alias = config.get("alias", None)
    n_clusters = config.get("n_clusters", 8)
    total_workers = config.get("total_workers")
    worker_number = config.get("worker_number")

    vector_field = vector_fields[0]
    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = ClusterOperator(
        n_clusters=n_clusters, vector_field=vector_field, alias=alias
    )

    filters = dataset[vector_field].exists()
    engine = ClusterEngine(
        dataset=dataset,
        operator=operator,
        chunksize=100,
        select_fields=[vector_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    workflow = AbstractWorkflow(
        engine=engine,
        job_id=job_id,
    )
    workflow.run()

    assert True
