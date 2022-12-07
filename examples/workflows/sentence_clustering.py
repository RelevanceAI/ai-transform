import uuid
import random
import numpy as np
from typing import Callable, List, Optional, Union
from workflows_core.api.client import Client
from workflows_core.dataset.dataset import Dataset
from workflows_core.engine.cluster_engine import InMemoryEngine
from workflows_core.workflow.helpers import decode_workflow_token
from workflows_core.workflow.abstract_workflow import AbstractWorkflow
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.document_list import DocumentList, Document
from sklearn.cluster import KMeans
from sentence_splitter import split_text_into_sentences
from functools import partial

class SentenceSplitter(AbstractOperator):
    def __init__(
        self, 
        field: str,
        output_chunk_field: str="sentence_chunk_",
        language: str='en'):
        self.split_function = partial(split_text_into_sentences, language=language)
        self._field = field
        self._output_chunk_field = output_chunk_field

        
    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Split sentences into output chunk fields
        """
        [
            d.split(
                self.split_function, 
                chunk_field=self._output_chunk_field,
                field=self._field
            ) for d in documents
        ]
        return documents

class VectorizerOperator(AbstractOperator):
    def __init__(self, text_field: str, chunk_field: str,
        output_field: str):
        self._text_field = text_field
        self._chunk_field = chunk_field
        self._output_field = output_field
    
    def _vectorize(self, *args, **kwargs):
        return [random.randint(0, 99) for _ in 10]
        
    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Split sentences
        """
        [
            d.operate_on_chunk(
                operator_function=self._vectorize,
                chunk_field=self._chunk_field,
                field=self._text_field,
                output_field=self._output_field
            )
            for d in documents
        ]
        return documents

class ChunkClusterOperator(AbstractOperator):
    def __init__(
        self,
        n_clusters: int,
        chunk_field: str,
        vector_field: str,
        alias: Optional[str] = None,
    ):
        self._model = KMeans(n_clusters=n_clusters)

        self._chunk_field = chunk_field
        self._vector_field = vector_field
        self._alias = f"kmeans-{n_clusters}" if alias is None else alias
        self._output_field = f"_cluster_.{vector_field}.{self._alias}"

        super().__init__(
            input_fields=[self._vector_field],
            output_fields=[self._output_field],
        )

    def transform(self, documents: DocumentList) -> DocumentList:
        """
        Main transform function
        """
        vectors = np.array(
            [np.array(document.get_chunk(
                chunk_field=self._chunk_field, 
                field=self._vector_field
            )) for document in documents]
        )
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

        alias = self._alias
        vector_field = self._vector_field

        dataset[vector_field].insert_centroids(
            centroid_documents=centroid_documents, alias=alias
        )


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

    client = Client(token=token)
    dataset = client.Dataset(dataset_id)


    sentence_splitter_operator = SentenceSplitter(
        field=text_field,
    )
    vectorize_operator = VectorizerOperator(
        
    )

    operator = ChunkClusterOperator(
        n_clusters=n_clusters, vector_field=vector_field, alias=alias
    )

    filters = dataset[vector_field].exists()
    engine = InMemoryEngine(
        dataset=dataset,
        operator=operator,
        chunksize=16,
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clustering workflow.")
    parser.add_argument(
        "token",
        type=str,
        help="a base64 encoded token that contains parameters for running the workflow",
    )
    args = parser.parse_args()
    execute(args.token, print)
