import numpy as np

from typing import Callable, Optional

from ai_transform.api.client import Client
from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.in_memory_engine import InMemoryEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import AbstractWorkflow
from ai_transform.operator.abstract_operator import AbstractOperator

from ai_transform.utils.document_list import DocumentList

from sklearn.cluster import KMeans


class ClusterOperator(AbstractOperator):
    def __init__(self, n_clusters: int, vector_field: str, alias: Optional[str] = None):
        self._model = KMeans(n_clusters=n_clusters)

        self._vector_field = vector_field
        self._alias = f"kmeans-{n_clusters}" if alias is None else alias
        self._output_field = f"_cluster_.{vector_field}.{self._alias}"

        super().__init__(input_fields=[self._vector_field], output_fields=[self._output_field])

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Main transform function
        """

        vectors = np.array([np.array(document[self._vector_field]) for document in documents])
        labels = self._model.fit_predict(vectors).tolist()

        for document, label in zip(documents, labels):
            document[self._output_field] = f"cluster_{label}"

        return documents

    def post_hooks(self, dataset: Dataset):
        """
        Insert the centroids after clustering
        """
        centroid_documents = [
            dict(_id=f"cluster_{_id}", centroid_vector=centroid_vector)
            for _id, centroid_vector in enumerate(self._model.cluster_centers_.tolist())
        ]
        dataset[self._output_field].insert_centroids(centroid_documents=centroid_documents)


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    job_id = config.get("job_id")
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    vector_fields = config["vector_fields"]
    vector_field = vector_fields[0]
    alias = config.get("alias", None)
    n_clusters = config.get("n_clusters", 8)
    total_workers = config.get("total_workers")
    send_email = config.get("send_email", True)
    additional_information = config.get("additional_information", "")

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = ClusterOperator(n_clusters=n_clusters, vector_field=vector_field, alias=alias)

    filters = dataset[vector_field].exists()
    engine = InMemoryEngine(
        dataset=dataset,
        operator=operator,
        chunksize=8,
        select_fields=[vector_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    workflow = AbstractWorkflow(
        name="Example Clustering Workflow",
        engine=engine,
        job_id=job_id,
        send_email=send_email,
        additional_information=additional_information,
    )
    workflow.run()

    field_children = dataset.list_field_children()["results"]
    assert field_children[0]["field"] == vector_fields[0]
    assert field_children[0]["field_children"][0] == operator.output_fields[0]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clustering workflow.")
    parser.add_argument(
        "token", type=str, help="a base64 encoded token that contains parameters for running the workflow"
    )
    args = parser.parse_args()
    execute(args.token, print)
