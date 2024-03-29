import argparse

from typing import Callable, List, Optional

from ai_transform.api.client import Client
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import AbstractWorkflow
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.document_list import DocumentList

from sentence_transformers import SentenceTransformer


class VectorizeTextOperator(AbstractOperator):
    def __init__(
        self, text_field: str, model: str = "sentence-transformers/all-mpnet-base-v2", alias: Optional[str] = None
    ):
        self._model = SentenceTransformer(model)
        self._model.eval()

        self._text_field = text_field
        self._alias = model.replace("/", "-") if alias is None else alias
        self._output_field = f"{text_field}_{self._alias}_vector_"

        super().__init__(input_fields=[self._text_field], output_fields=[self._output_field])

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Main transform function
        """
        batch = [document[self._text_field] for document in documents]
        vectors = self._model.encode(batch).tolist()

        for index in range(len(vectors)):
            documents[index][self._output_field] = vectors[index]

        return documents


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    job_id = config["job_id"]
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)
    total_workers = config.get("total_workers", None)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = VectorizeTextOperator(text_field=text_field, alias=alias)

    filters = dataset[text_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=8,
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
    import argparse

    parser = argparse.ArgumentParser(description="Vectorize Text Workflow.")
    parser.add_argument(
        "token", type=str, help="a base64 encoded token that contains parameters for running the workflow"
    )
    args = parser.parse_args()
    execute(args.token, print)
