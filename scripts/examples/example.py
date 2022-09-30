import base64
import json
import os

from copy import deepcopy
from typing import Any, List

from slim.api import Client
from slim.engine import AbstractEngine
from slim.workflow import AbstractWorkflow
from slim.operator import AbstractOperator
from slim.utils import Document
from slim.workflow.helpers import decode_workflow_token

TOKEN = os.getenv("TOKEN")


class ExampleOperator(AbstractOperator):
    def __init__(self, field: str):
        self._field = field

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """
        for document in documents:
            before = deepcopy(document)
            document.set(self._field, document.get(self._field) / 2)
            after = document
            print(before.get(self._field), after.get(self._field))

        return documents


class ExampleEngine(AbstractEngine):
    def apply(self) -> Any:

        for _ in range(self.nb):
            batch = self.get_chunk()
            new_batch = self.operator(batch)
            self.update_chunk(new_batch)

        return


class ExampleWorkflow(AbstractWorkflow):
    def pre_hook(self):
        """
        Optional Method
        """
        print("Starting Workflow")
        print(f"Using {type(self.operator).__name__} as Operator")

    def post_hook(self):
        """
        Optional Method
        """
        print(f"Dataset has {len(self.dataset)} documents")
        print("Finished Workflow")


def main(token: str):
    config = decode_workflow_token(token)

    token = config["authorizationToken"]
    datatset_id = config["dataset_id"]
    field = config["field"]

    client = Client(token=token)

    dataset = client.Dataset(datatset_id)
    operator = ExampleOperator(field=field)

    engine = ExampleEngine(dataset=dataset, operator=operator)

    workflow = ExampleWorkflow(engine)
    workflow.run()


if __name__ == "__main__":
    config = dict(
        authorizationToken=os.getenv("TOKEN"),
        dataset_id="test_dataset",
        field="feild1.field2",
    )
    string = f"{json.dumps(config)}"
    bytes = string.encode()
    token = base64.b64encode(bytes).decode()
    main(token)
