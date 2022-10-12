import numpy as np

from typing import Callable, List, Optional

from workflows_core.api.client import Client
from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator

from workflows_core.utils.random import Document

from sklearn.cluster import KMeans


class ClusterOperator(AbstractOperator):
    def __init__(
        self,
        n_clusters: int,
        vector_field: str,
        alias: Optional[str] = None,
    ):
        self._model = KMeans(n_clusters=n_clusters)

        self._vector_field = vector_field
        self._alias = f"kmeans-{n_clusters}" if alias is None else alias
        self._output_field = f"_cluster_.{vector_field}.{self._alias}"

        super().__init__(
            input_fields=[self._vector_field],
            output_fields=[self._output_field],
        )

    def transform(self, documents: List[Document]) -> List[Document]:
        """
        Main transform function
        """

        vectors = np.array(
            [np.array(document.get(self._vector_field)) for document in documents]
        )
        labels = self._model.fit_predict(vectors).tolist()

        for document, label in zip(documents, labels):
            document.set(self._output_field, f"cluster_{label}")

        return documents

    def post_hooks(self, dataset: Dataset):
        """
        Insert the centroids after clustering
        """
        centroid_documents = [
            dict(_id=f"cluster_{_id}", centroid_vector=centroid_vector)
            for _id, centroid_vector in enumerate(self._model.cluster_centers_.tolist())
        ]

        alias = self._alias
        vector_field = self._vector_field

        dataset[vector_field].insert_centroids(
            centroid_documents=centroid_documents, alias=alias
        )


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):

    config = decode_workflow_token(args.workflow_token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    vector_field = config["vector_field"]
    alias = config.get("alias", None)
    n_clusters = config.get("n_clusters", 8)

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    operator = ClusterOperator(
        n_clusters=n_clusters, vector_field=vector_field, alias=alias
    )

    filters = dataset[vector_field].exists()
    engine = StableEngine(
        dataset=dataset,
        operator=operator,
        chunksize=None,
        select_fields=[vector_field],
        filters=filters,
    )

    workflow = AbstractWorkflow(engine)
    workflow.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clustering workflow.")
    parser.add_argument(
        "token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.workflow_token, print)
