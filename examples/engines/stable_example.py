import argparse

from typing import List

from workflows_core.api.client import Client
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.document import Document
from workflows_core.workflow.helpers import decode_workflow_token


class ExampleOperator(AbstractOperator):
    def __init__(self, field: str):
        self._field = field

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """
        for document in documents:
            before = document.get(self._field)
            document.set(self._field, document.get(self._field, 3) * 2)
            after = document
            print(before, after.get(self._field))

        return documents


class ExampleWorkflow(AbstractWorkflow):
    pass


def main(args):
    config = decode_workflow_token(args.workflow_token)

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
    parser = argparse.ArgumentParser(description="An example workflow.")
    parser.add_argument(
        "--workflow-token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    main(args)
