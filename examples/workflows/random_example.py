"""
An example showing how to write a simple workflow.
In this workflow, we add one to every number.
"""
# For this workflow to to run, you need the following;
# transformers[torch]==4.18.0
# relevance-workflows-core

import uuid
import random
from typing import Callable, List, Optional

from ai_transform.api.client import Client
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import AbstractWorkflow
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.document_list import DocumentList


class RandomOperator(AbstractOperator):
    def __init__(
        self,
        numeric_field: str,
    ):
        self.numeric_field = numeric_field

        super().__init__(
            input_fields=[numeric_field],
            output_fields=[f"_random_.{numeric_field}"],
        )

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Main transform function
        """
        for d in documents:
            # add one to the numeric field
            d[self.numeric_field] = d[self.numeric_field] + 1
        return documents


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    job_id = config.get("job_id", str(uuid.uuid4()))
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    numeric_field = config["numeric_field"]
    total_workers = config.get("total_workers", None)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = RandomOperator(numeric_field=numeric_field)

    filters = dataset[numeric_field].exists()

    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=8,
        select_fields=[numeric_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    workflow = AbstractWorkflow(engine=engine, job_id=job_id, name="AddOne")
    workflow.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sentiment workflow.")
    parser.add_argument(
        "token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.token, print)
