# For this workflow to to run, you need the following;
# transformers[torch]==4.18.0
# sentence-transformers==2.2.0
# sentencepiece==0.1.95
# sentence-splitter==1.4.0
# protobuf==3.20.1

import uuid
import numpy as np

from typing import Callable, List, Optional

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.random import Document

from sentence_transformers import CrossEncoder


def softmax(array: np.ndarray, axis: int = -1):
    """
    Softmax function
    """
    exp = np.exp(array)
    return exp / np.sum(exp, axis=axis)[:, np.newaxis]


class TaggingOperator(AbstractOperator):
    LABELS = {
        0: "contradiction",
        1: "neutral",
        2: "entailment",
    }

    def __init__(
        self,
        survey_question: str,
        text_field: str,
        taxonomy: List[str],
        model: str = "microsoft/deberta-v2-xxlarge-mnli",
        alias: Optional[str] = None,
        min_score: float = 0.1,
        max_number_of_labels: int = 5,
    ):
        self._model = CrossEncoder(model)

        self._survey_question = survey_question
        self._text_field = text_field
        self._taxonomy = taxonomy
        self._alias = model.replace("/", "-") if alias is None else alias
        self._output_field = f"_surveytag_.{text_field}.{self._alias}"
        self._min_score = min_score
        self._max_number_of_labels = max_number_of_labels

        super().__init__(
            input_fields=[text_field],
            output_fields=[
                f"{self._output_field}.label",
                f"{self._output_field}.score",
            ],
        )

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """

        tags = [tag for _ in documents for tag in self._taxonomy]
        batch = [
            (
                f"{self._survey_question} {document.get(self._text_field)}",
                f"About {tag}.",
            )
            for document in documents
            for tag in self._taxonomy
        ]
        batch_index_mapping = {
            (document_index * len(self._taxonomy) + tag_index): document_index
            for document_index in range(len(documents))
            for tag_index in range(len(self._taxonomy))
        }

        logits = []
        for batch_index in range((len(batch) // 32) + 1):
            score = self._model.predict(
                batch[batch_index * 32 : (batch_index + 1) * 32]
            )
            logits.append(score)
        logits = np.concatenate(logits, axis=0)
        logits = softmax(logits)
        argmax = np.argmax(logits, axis=-1)

        for document in documents:
            document.set(self._output_field, [{"label": "[No Tag]"}])

        for batch_index, (max_index, scores) in enumerate(zip(argmax, logits)):
            score = scores[max_index]
            label = self.LABELS[max_index]
            if score > self._min_score and label == "entailment":
                tag = tags[batch_index]
                prediction = {
                    "pred": self.LABELS[max_index],
                    "label": tag,
                    "score": scores[max_index],
                }
                document = documents[batch_index_mapping[batch_index]]
                document_tags = document.get(self._output_field)
                document.set(self._output_field, document_tags + [prediction])

        for document in documents:
            document_tags = document.get(self._output_field)
            if len(document_tags) == 0:
                document.set(self._output_field, [{"label": "[No Tag]"}])

        return documents


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    workflow_id = config.get("workflow_id", str(uuid.uuid4()))
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    survey_question = config.get("surveyQuestion", "")
    if not survey_question.endswith(" "):
        survey_question += " "
    labels = config["taxonomy_labels"]
    text_field = config["textFields"]
    alias = config.get("alias", None)
    max_number_of_labels = int(config.get("max_number_of_labels", 5))
    filters = config.get("filters", None)
    after_id = config.get("after_id")
    pull_limit = config.get("pull_limit")
    parallel_job_id = config.get("parallel_job_id", "")

    alias = config.get("alias", None)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = TaggingOperator(
        survey_question=survey_question,
        text_field=text_field,
        taxonomy=labels,
        alias=alias,
        max_number_of_labels=max_number_of_labels,
    )

    filters = dataset[text_field].exists()

    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=100,
        select_fields=[text_field],
        filters=filters,
        worker_number=worker_number,
    )

    workflow = AbstractWorkflow(workflow_id, engine)
    workflow.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Emotion workflow.")
    parser.add_argument(
        "token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.token, print)
