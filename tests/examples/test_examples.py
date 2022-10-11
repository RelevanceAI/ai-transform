import time

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token

from examples.workflows.sentiment_example import SentimentOperator, SentimentWorkflow
from examples.engines.ray_example import ExampleWorkflow, RayOperator, RayEngine


def test_sentiment_example(test_sentiment_workflow_token: str):
    config = decode_workflow_token(test_sentiment_workflow_token)

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

    workflow = SentimentWorkflow(engine)
    workflow.run()

    time.sleep(2)

    assert operator._output_field in dataset.schema
    health = dataset.health()
    for output_field in operator._output_fields:
        assert health[output_field]["exists"] == engine.size


def test_ray_example(test_ray_workflow_token: str):
    config = decode_workflow_token(test_ray_workflow_token)

    token = config["authorizationToken"]
    datatset_id = config["dataset_id"]
    field = config["field"]

    client = Client(token=token)

    dataset = client.Dataset(datatset_id)
    operator = RayOperator(field=field)

    engine = RayEngine(dataset=dataset, operator=operator)

    workflow = ExampleWorkflow(engine)
    workflow.run()
