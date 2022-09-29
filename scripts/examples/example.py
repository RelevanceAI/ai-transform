from copy import deepcopy
import os

from typing import Any, List

from slim.api import Client
from slim.engines import AbstractEngine
from slim.workflow import AbstractWorkflow
from slim.operator import AbstractOperator
from slim.utils import Document

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


def main():
    client = Client(token=TOKEN)

    dataset = client.Dataset("test_dataset")
    operator = ExampleOperator(field="new_field1.new_field2")

    engine = ExampleEngine(dataset=dataset, operator=operator)

    workflow = ExampleWorkflow(engine)
    workflow.run()


if __name__ == "__main__":
    main()
