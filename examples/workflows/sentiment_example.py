# For this workflow to to run, you need the following;
# transformers[torch]==4.18.0
# relevance-workflows-core

import torch

from typing import List, Optional

from transformers import pipeline
from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.random import Document


class SentimentOperator(AbstractOperator):
    LABELS = {
        "LABEL_0": "negative",
        "LABEL_1": "neutral",
        "LABEL_2": "positive",
    }

    def __init__(
        self,
        text_field: str,
        model: str = "cardiffnlp/twitter-roberta-base-sentiment",
        alias: Optional[str] = None,
    ):

        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        self._model = pipeline(
            "sentiment-analysis", model=model, device=device, return_all_scores=True
        )

        self._text_field = text_field
        self._alias = model.replace("/", "-") if alias is None else alias
        self._output_field = f"_sentiment_.{text_field}.{self._alias}"

        output_fields = [
            f"{self._output_field}.sentiment",
            f"{self._output_field}.overall_sentiment_score",
        ]
        super().__init__(
            input_fields=[text_field],
            output_fields=output_fields,
        )

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """

        batch = [document.get(self._text_field) for document in documents]
        labels = self._model(batch)

        for index in range(len(labels)):
            _labels = [l for i, l in enumerate(labels[index]) if i != 1]
            _label = max(_labels, key=lambda logit: logit["score"])

            label = SentimentOperator.LABELS[_label["label"]]

            _score = _label["score"]
            if label == "positive":
                score = score

            elif label == "negative":
                score = -_score

            sentiment = dict(sentiment=score, overall_sentiment_score=label)
            documents[index].set(self._output_field, sentiment)

        return documents


class SentimentWorkflow(AbstractWorkflow):
    pass


def execute(token, logger, worker_number=0, *args, **kwargs):
    config = decode_workflow_token(token)

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
        worker_number=worker_number,
    )

    workflow = SentimentWorkflow(engine)
    workflow.run()
    # Run workflow example


if __name__ == "__main__":
    # For script things
    import argparse

    parser = argparse.ArgumentParser(description="An example workflow.")
    parser.add_argument(
        "--workflow-token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.workflow_token, print)
