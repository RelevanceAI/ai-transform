import os
import json
import base64
import numpy as np

from typing import List, Optional

from core.api.client import Client
from core.engine.stable_engine import StableEngine

from core.utils.random import mock_documents
from core.workflow.helpers import decode_workflow_token

from core.workflow.abstract_workflow import AbstractWorkflow
from core.operator.abstract_operator import AbstractOperator

from core.utils.random import Document

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


class ClusterWorkflow(AbstractWorkflow):
    operator: ClusterOperator

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
        centroid_documents = [
            dict(_id=f"cluster_{_id}", centroid_vector=centroid_vector)
            for _id, centroid_vector in enumerate(
                self.operator._model.cluster_centers_.tolist()
            )
        ]

        alias = self.operator._alias
        vector_field = self.operator._vector_field

        self.dataset[vector_field].insert_centroids(
            centroid_documents=centroid_documents, alias=alias
        )
        print(f"Successfully inserted `{len(centroid_documents)}` centroids")


def main(token: str):
    config = decode_workflow_token(token)

    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    vector_field = config["vector_field"]
    alias = config.get("alias", None)
    n_clusters = config.get("n_clusters", 8)

    client = Client(token=token)

    client.delete_dataset(dataset_id)
    dataset = client.Dataset(dataset_id)
    dataset.insert_documents(mock_documents())

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

    workflow = ClusterWorkflow(engine)
    workflow.run()


if __name__ == "__main__":
    config = dict(
        authorizationToken=os.getenv("TOKEN"),
        dataset_id="test_dataset",
        vector_field="sample_1_vector_",
    )
    string = f"{json.dumps(config)}"
    bytes = string.encode()
    token = base64.b64encode(bytes).decode()
    main(token)
