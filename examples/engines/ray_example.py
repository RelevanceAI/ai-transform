from typing import Callable

import pandas as pd

from workflows_core.api.client import Client
from workflows_core.engine.ray_engine import RayEngine
from workflows_core.operator.ray_operator import AbstractRayOperator
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.workflow.helpers import decode_workflow_token


class RayOperator(AbstractRayOperator):
    def __init__(self, field: str):
        self._field = field

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transform function
        """
        df[self._field] += 1

        return df


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(args.workflow_token)

    token = config["authorizationToken"]
    datatset_id = config["dataset_id"]
    field = config["field"]

    client = Client(token=token)

    dataset = client.Dataset(datatset_id)
    operator = RayOperator(field=field)

    engine = RayEngine(dataset=dataset, operator=operator)

    workflow = AbstractWorkflow(engine)
    workflow.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="An example workflow.")
    parser.add_argument(
        "--workflow-token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.workflow_token, print)
