import argparse
from typing import Callable, List


from workflows_core.utils.document import Document
from workflows_core.api.client import Client
from workflows_core.engine.ray_engine import RayEngine
from workflows_core.operator.ray_operator import AbstractRayOperator
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.workflow.helpers import decode_workflow_token


class RayOperator(AbstractRayOperator):
    def __init__(self, field: str):
        self._field = field

        super().__init__(input_fields=[field])

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """
        for document in documents:
            document.set(self._field, 1)

        return documents


class ExampleWorkflow(AbstractWorkflow):
    pass


def execute(
    workflow_token: str, logger: Callable, worker_number: int = 0, *args, **kwargs
):
    config = decode_workflow_token(workflow_token)

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
    parser = argparse.ArgumentParser(description="An example workflow.")
    parser.add_argument(
        "--workflow-token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.workflow_token, print)
