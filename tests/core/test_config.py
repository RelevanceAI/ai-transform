from examples.workflows.sentiment_example import SentimentOperator

from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.abstract_workflow import Workflow

from tests.conftest import SentimentConfig


def test_from_config(test_sentiment_config: SentimentConfig, full_dataset: Dataset):
    operator = SentimentOperator.from_config(test_sentiment_config)

    engine = StableEngine.from_config(
        test_sentiment_config, dataset=full_dataset, operator=operator
    )

    workflow = Workflow.from_config(test_sentiment_config, engine=engine)

    assert engine.dataset
