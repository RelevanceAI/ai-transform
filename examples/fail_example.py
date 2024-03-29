import argparse

from typing import Callable, List, Optional

from ai_transform.api.client import Client
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import AbstractWorkflow
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.example_documents import Document
from ai_transform.errors import UserFacingError


class BadOperator(AbstractOperator):
    def __init__(
        self, text_field: str, model: str = "cardiffnlp/twitter-roberta-base-sentiment", alias: Optional[str] = None
    ):
        super().__init__()

    def transform(self, documents: List[Document]) -> List[Document]:
        raise ValueError


class UserFacingErrorOperator(AbstractOperator):
    def __init__(
        self,
        client: Client,
        job_id: str,
        workflow_name: str,
        text_field: str,
        model: str = "cardiffnlp/twitter-roberta-base-sentiment",
        alias: Optional[str] = None,
    ):
        self.client = client
        self.job_id = job_id
        self.workflow_name = workflow_name

        self.text_field = text_field
        super().__init__()

    def transform(self, documents: List[Document]) -> List[Document]:
        try:
            raise ValueError

        except:
            # pass
            raise UserFacingError(
                f"dataset must contain `{self.text_field}`", self.client, self.job_id, self.workflow_name
            )


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

    operator = BadOperator(text_field=text_field, alias=alias)

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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Vectorize Text Workflow.")
    parser.add_argument(
        "token", type=str, help="a base64 encoded token that contains parameters for running the workflow"
    )
    args = parser.parse_args()
    execute(args.token, print)
