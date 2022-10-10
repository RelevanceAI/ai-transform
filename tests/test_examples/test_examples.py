from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token

from examples.workflows.sentiment_example import SentimentOperator, SentimentWorkflow


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

    assert operator._output_field in dataset.schema
