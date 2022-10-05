import os
import json
import base64
from typing import List

import pandas as pd
import pyarrow as pa

from ray.data.block import Block

from workflows_core.api import Client
from workflows_core.engine import RayEngine
from workflows_core.operator import AbstractRayOperator
from workflows_core.workflow import AbstractWorkflow
from workflows_core.utils import Document
from workflows_core.workflow.helpers import decode_workflow_token

TOKEN = os.getenv("TOKEN")


class RayOperator(AbstractRayOperator):
    def __init__(self, field: str):
        self._field = field

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transform function
        """
        df[self._field] += 1

        return df


class ExampleWorkflow(AbstractWorkflow):
    pass


def main(token: str):
    config = decode_workflow_token(token)

    token = config["authorizationToken"]
    datatset_id = config["dataset_id"]
    field = config["field"]

    client = Client(token=token)

    dataset = client.Dataset(datatset_id)
    operator = RayOperator(field=field)

    engine = RayEngine(dataset=dataset, operator=operator)

    workflow = ExampleWorkflow(engine)
    workflow.run()


if __name__ == "__main__":
    config = dict(
        authorizationToken=os.getenv("TOKEN"),
        dataset_id="test_dataset",
        field="new_field1.new_field2",
    )
    string = f"{json.dumps(config)}"
    bytes = string.encode()
    token = base64.b64encode(bytes).decode()
    main(token)
