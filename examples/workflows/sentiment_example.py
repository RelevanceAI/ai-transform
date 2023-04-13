# For this workflow to to run, you need the following;
# transformers[torch]==4.18.0
# relevance-workflows-core

import uuid
import torch

from typing import Callable, List, Optional

from transformers import pipeline
from ai_transform.api.client import Client
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import AbstractWorkflow
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.document_list import DocumentList
from ai_transform.config import BaseConfig
from pydantic import Field


class SentimentOperator(AbstractOperator):
    LABELS = {"LABEL_0": "negative", "LABEL_1": "neutral", "LABEL_2": "positive"}

    def __init__(
        self, text_field: str, model: str = "cardiffnlp/twitter-roberta-base-sentiment", alias: Optional[str] = None
    ):

        device = 0 if torch.cuda.is_available() else -1
        self._model = pipeline("sentiment-analysis", model=model, device=device, return_all_scores=True)

        self._text_field = text_field
        self._alias = model.replace("/", "-") if alias is None else alias
        self._output_field = f"_sentiment_.{text_field}.{self._alias}"

        super().__init__(
            input_fields=[text_field],
            output_fields=[f"{self._output_field}.sentiment", f"{self._output_field}.overall_sentiment_score"],
        )

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Main transform function
        """

        batch = [document[self._text_field] for document in documents]
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
            documents[index][self._output_field] = sentiment

        return documents


class SentimentConfig(BaseConfig):
    # BaseConfig automatically handles authorizationToken, job_id, etc.
    # We put the SentimentConfig here so that we can auto-generate
    # a JSONSchema
    textFields: str = Field(..., description="The text field to run sentiment on.")
    alias: Optional[str] = Field(None, description="The alias for each sentiment component.")
    transform_chunksize: Optional[int] = Field(8, description="The amount to transform at any 1 time.")


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config: SentimentConfig = SentimentConfig.read_token(token)
    # You can access dot-notation with typehinting. Yay.
    job_id = config.job_id
    token = config.authorizationToken
    dataset_id = config.dataset_id
    text_field = config.textFields
    total_workers = config.total_workers
    alias = config.alias
    transform_chunksize = config.transform_chunksize
    # defaults to None as specified in field

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = SentimentOperator(text_field=text_field, alias=alias)
    if config.documents:
        return operator.transform_for_playground(
            config.documents,
            job_id=job_id,
            workflow_name="Sentiment",
            authorization_token=token,
            additional_information="",
            metadata={},
            status="complete",
            send_email=False,
            worker_number=worker_number,
        )

    filters = dataset[text_field].exists()

    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=transform_chunksize,
        select_fields=[text_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    workflow = AbstractWorkflow(engine=engine, job_id=job_id)
    workflow.run()

    field_children = dataset.list_field_children()["results"]
    assert field_children[0]["field"] == text_field
    assert field_children[0]["field_children"][0] == operator.output_fields[0]


if __name__ == "__main__":
    # For script things
    import argparse

    parser = argparse.ArgumentParser(description="Sentiment workflow.")
    parser.add_argument(
        "token", type=str, help="a base64 encoded token that contains parameters for running the workflow"
    )
    args = parser.parse_args()
    execute(args.token, print)
