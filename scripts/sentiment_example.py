import os
import json
import torch
import base64

from typing import List, Optional

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine

from workflows_core.utils.random import mock_documents
from workflows_core.workflow.helpers import decode_workflow_token

from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator

from workflows_core.utils.random import Document

from transformers import pipeline


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
    operator: SentimentOperator

    def pre_hook(self):
        """
        Optional Method
        """
        print("Starting Workflow")
        print(f"Using `{type(self.operator).__name__}` as Operator")

    def post_hook(self):
        """
        Optional Method
        """
        print("Finished Workflow!")


def main(token: str):
    config = decode_workflow_token(token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)

    client = Client(token=token)

    client.delete_dataset(dataset_id)
    dataset = client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents())

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


if __name__ == "__main__":
    config = dict(
        authorizationToken=os.getenv("TOKEN"),
        dataset_id="test_dataset",
        text_field="sample_1_label",
    )
    string = f"{json.dumps(config)}"
    bytes = string.encode()
    token = base64.b64encode(bytes).decode()
    main(token)
