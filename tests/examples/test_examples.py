import time

from examples.workflows.sentiment_example import SentimentOperator

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.workflow.helpers import decode_workflow_token


def test_sentiment_example(test_sentiment_workflow_token: str):
    config = decode_workflow_token(test_sentiment_workflow_token)

    workflow_id = config["workflow_id"]
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)

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
    )

    workflow = AbstractWorkflow(engine=engine, workflow_id=workflow_id)
    workflow.run()

    time.sleep(2)

    assert operator._output_field in dataset.schema
    health = dataset.health()
    for output_field in operator._output_fields:
        assert health[output_field]["exists"] == engine.size

    assert workflow.get_status()["job_status"].lower() == "complete"
