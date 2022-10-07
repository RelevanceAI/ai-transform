import os
import json
import base64

from typing import List

from workflows_core.api.client import Client
from workflows_core.engine.ray_engine import RayEngine
from workflows_core.operator.ray_operator import AbstractRayOperator
from workflows_core.utils.document import Document
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.workflow.helpers import decode_workflow_token


class RayOperator(AbstractRayOperator):
    def __init__(self, field: str):
        self._field = field

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """
        # import pdb; pdb.set_trace()
        for document in documents:
            document.set(self._field, 1)

        return documents


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
