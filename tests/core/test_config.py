from examples.workflows.sentiment_example import SentimentOperator

from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.abstract_workflow import Workflow

from tests.conftest import SentimentConfig


def test_from_config(test_sentiment_config: SentimentConfig, full_dataset: Dataset):
    operator = SentimentOperator.from_config(test_sentiment_config)

    engine = StableEngine.from_config(
        test_sentiment_config, dataset=full_dataset, operator=operator
    )

    workflow = Workflow.from_config(test_sentiment_config, engine=engine)

    assert operator.input_fields
    assert operator.output_fields
    assert engine.dataset
    assert workflow.engine
