import base64
import json
import os

from copy import deepcopy
from typing import Any, List

from workflows_core.api import Client
from workflows_core.engine import StableEngine
from workflows_core.workflow import AbstractWorkflow
from workflows_core.operator import AbstractOperator
from workflows_core.utils import Document
from workflows_core.workflow.helpers import decode_workflow_token

TOKEN = os.getenv("TOKEN")


class ExampleOperator(AbstractOperator):
    def __init__(self, field: str):
        self._field = field

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """
        for document in documents:
            before = document.get(self._field)
            document.set(self._field, document.get(self._field) * 2)
            after = document
            print(before, after.get(self._field))

        return documents


class ExampleWorkflow(AbstractWorkflow):
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
        print(f"Dataset has `{len(self.dataset)}` documents")
        print("Finished Workflow")


def main(token: str):
    config = decode_workflow_token(token)

    token = config["authorizationToken"]
    datatset_id = config["dataset_id"]
    field = config["field"]

    client = Client(token=token)

    dataset = client.Dataset(datatset_id)
    operator = ExampleOperator(field=field)

    engine = StableEngine(dataset=dataset, operator=operator)

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
