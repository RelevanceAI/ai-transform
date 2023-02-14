import time

from ai_transform.api.client import Client
from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.dense_output_engine import DenseOutputEngine

from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.workflow.abstract_workflow import Workflow

from ai_transform.utils.example_documents import mock_documents


class TestDenseOutputEngine:
    def test_dense_output_engine(
        self,
        test_dense_operator: AbstractOperator,
        dense_input_dataset: Dataset,
        dense_output_dataset1: Dataset,
        dense_output_dataset2: Dataset,
    ):
        engine = DenseOutputEngine(
            dataset=dense_input_dataset,
            operator=test_dense_operator,
        )
        workflow = Workflow(
            name="workflow_test123",
            engine=engine,
            job_id="test_job123",
        )
        workflow.run()

        time.sleep(4)

        documents = dense_input_dataset.get_all_documents(select_fields=["new_field"])
        assert len(documents["documents"]) == 2
        for document in documents["documents"]:
            assert "new_field" not in document

        documents = dense_output_dataset1.get_all_documents(select_fields=["new_field"])
        assert len(documents["documents"]) == 2
        for document in documents["documents"]:
            assert document["new_field"] == 3

        documents = dense_output_dataset2.get_all_documents(select_fields=["new_field"])
        assert len(documents["documents"]) == 2
        for document in documents["documents"]:
            assert document["new_field"] == 3

        input_dataset_metadata = dense_input_dataset.get_metadata()["results"]
        assert "_child_datasets_" in input_dataset_metadata

        for output_dataset in [dense_output_dataset1, dense_output_dataset2]:
            output_dataset_metadata = output_dataset.get_metadata()["results"]
            assert "_parent_dataset_" in output_dataset_metadata
