import time

from workflows_core.api.client import Client
from workflows_core.engine.dense_output_engine import DenseOutputEngine

from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.workflow.abstract_workflow import Workflow

from workflows_core.utils.example_documents import mock_documents


class TestStableEngine:
    def test_dense_output_engine(
        self, test_client: Client, test_dense_operator: AbstractOperator
    ):
        input_dataset = test_client.Dataset("input_dataset1")
        input_dataset.insert_documents(mock_documents(2))

        output_dataset1 = test_client.Dataset("ouptut_dataset1")
        output_dataset2 = test_client.Dataset("ouptut_dataset2")

        engine = DenseOutputEngine(
            input_dataset=input_dataset,
            output_datasets=[
                output_dataset1,
                output_dataset2,
            ],
            operator=test_dense_operator,
        )
        workflow = Workflow(
            name="workflow_test123",
            engine=engine,
            job_id="test_job123",
        )
        workflow.run()

        time.sleep(4)

        documents = input_dataset.get_all_documents(select_fields=["new_field"])
        assert len(documents["documents"]) == 2
        for document in documents["documents"]:
            assert "new_field" not in document

        documents = output_dataset1.get_all_documents(select_fields=["new_field"])
        assert len(documents["documents"]) == 2
        for document in documents["documents"]:
            assert document["new_field"] == 3

        documents = output_dataset2.get_all_documents(select_fields=["new_field"])
        assert len(documents["documents"]) == 2
        for document in documents["documents"]:
            assert document["new_field"] == 3

        test_client.delete_dataset("input_dataset1")
        test_client.delete_dataset("ouptut_dataset1")
        test_client.delete_dataset("ouptut_dataset2")
