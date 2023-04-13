"""
Cluster sentences.
In this script, we are looking to perform the following:
- Split text into multiple sentences.
- Vectorize each sentence and store it as a vector inside the chunk
- Cluster based on the chunks
"""
import random
import numpy as np

from functools import partial
from typing import Callable, List, Optional, Union

from ai_transform.api.client import Client
from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.in_memory_engine import InMemoryEngine
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.workflow.helpers import decode_workflow_token
from ai_transform.workflow.abstract_workflow import Workflow
from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.utils.document_list import DocumentList

from sklearn.cluster import KMeans
from sentence_splitter import split_text_into_sentences


class SentenceSplitterOperator(AbstractOperator):
    def __init__(self, field: str, output_chunk_field: str = "sentence_chunk_", language: str = "en"):
        self.split_function = partial(split_text_into_sentences, language=language)
        self._field = field
        self._output_chunk_field = output_chunk_field
        super().__init__(input_fields=[self._field], output_fields=[self._output_chunk_field])

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Split sentences into output chunk fields
        """
        [d.split(self.split_function, chunk_field=self._output_chunk_field, field=self._field) for d in documents]
        return documents


class ChunkVectorizerOperator(AbstractOperator):
    def __init__(self, text_field: str, chunk_field: str, output_field: str):
        self._text_field = text_field
        self._chunk_field = chunk_field
        self._output_field = output_field
        super().__init__(input_fields=[self._chunk_field], output_fields=[self._chunk_field])

    def _random_vector(self):
        return [random.randint(0, 99) for _ in range(10)]

    def _vectorize(self, chunk_values, *args, **kwargs):
        return [self._random_vector() for _ in range(len(chunk_values))]

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Split sentences
        """
        [
            d.operate_on_chunk(
                operator_function=self._vectorize,
                chunk_field=self._chunk_field,
                field=self._text_field,
                output_field=self._output_field,
            )
            for d in documents
        ]
        return documents


class ChunkClusterOperator(AbstractOperator):
    def __init__(self, n_clusters: int, chunk_field: str, vector_field: str, alias: Optional[str] = None):
        self._model = KMeans(n_clusters=n_clusters)

        self._chunk_field = chunk_field
        self._vector_field = vector_field
        self._alias = f"kmeans-{n_clusters}" if alias is None else alias
        self._output_field = f"_cluster_.{vector_field}.{self._alias}"

        super().__init__(input_fields=[self._vector_field], output_fields=[self._output_field])

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Main transform function
        """
        vectors = documents.get_chunks_as_flat(chunk_field=self._chunk_field, field=self._vector_field)
        vectors = np.array(vectors)
        labels = self._model.fit_predict(vectors).tolist()

        for i, chunk_labels in enumerate(documents.split_by_chunk(chunk_field=self._chunk_field, values=labels)):
            chunk_labels = [f"cluster_{l}" for l in chunk_labels]
            documents[i][self._output_field] = chunk_labels
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

        dataset[vector_field].insert_centroids(centroid_documents=centroid_documents, alias=alias)


def execute(token: str, logger: Callable, worker_number: int = 0, *args, **kwargs):
    config = decode_workflow_token(token)

    job_id = config.get("job_id")
    token = config["authorizationToken"]
    dataset_id = config["dataset_id"]
    text_field = config["text_field"]
    alias = config.get("alias", None)
    n_clusters = config.get("n_clusters", 8)
    total_workers = config.get("total_workers")
    send_email = config.get("send_email", True)
    additional_information = config.get("additional_information", "")
    filters = config.get("filters", [])

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)

    CHUNK_FIELD = "sentence_chunk_"
    VECTOR_FIELD = text_field + "_vector_"
    TRANSFORM_CHUNKSIZE = 200
    sentence_splitter_operator = SentenceSplitterOperator(field=text_field, output_chunk_field=CHUNK_FIELD)

    stable_engine = StableEngine(
        dataset=dataset,
        operator=sentence_splitter_operator,
        transform_chunksize=TRANSFORM_CHUNKSIZE,
        select_fields=[text_field],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    sentence_splitter_workflow = Workflow(
        name="Sentence Splitting", engine=stable_engine, job_id=job_id, send_email=False, additional_information=""
    )
    sentence_splitter_workflow.run()

    vectorize_operator = ChunkVectorizerOperator(
        text_field=text_field, chunk_field=CHUNK_FIELD, output_field=VECTOR_FIELD
    )

    stable_engine = StableEngine(
        dataset=dataset,
        operator=vectorize_operator,
        transform_chunksize=TRANSFORM_CHUNKSIZE,
        select_fields=[CHUNK_FIELD],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    vectorize_workflow = Workflow(
        name="Example Engine",
        engine=stable_engine,
        job_id=job_id,
        send_email=False,
        additional_information=additional_information,
    )
    vectorize_workflow.run()

    operator = ChunkClusterOperator(
        n_clusters=n_clusters, vector_field=VECTOR_FIELD, alias=alias, chunk_field=CHUNK_FIELD
    )

    # filters = dataset[VECTOR_FIELD].exists()
    engine = InMemoryEngine(
        dataset=dataset,
        operator=operator,
        select_fields=[CHUNK_FIELD],
        filters=filters,
        worker_number=worker_number,
        total_workers=total_workers,
    )

    workflow = Workflow(
        name="Example Clustering Workflow",
        engine=engine,
        job_id=job_id,
        send_email=send_email,
        additional_information=additional_information,
    )
    workflow.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clustering workflow.")
    parser.add_argument(
        "token", type=str, help="a base64 encoded token that contains parameters for running the workflow"
    )
    args = parser.parse_args()
    execute(args.token, print)
