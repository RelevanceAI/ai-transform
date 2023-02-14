try:
    from sklearn.cluster import MiniBatchKMeans
except:
    pass
else:

    import time
    import numpy as np

    from ai_transform.dataset.dataset import Dataset
    from ai_transform.engine.multipass_engine import MultiPassEngine

    from ai_transform.operator.abstract_operator import AbstractOperator
    from ai_transform.workflow.abstract_workflow import Workflow

    class TestMultiPassEngine:
        def test_multipass_engine(
            self, full_dataset: Dataset, test_operator: AbstractOperator
        ):
            engine = MultiPassEngine(
                dataset=full_dataset,
                operators=[
                    test_operator,
                    test_operator,
                ],
            )
            workflow = Workflow(
                name="test_multipass_engine",
                engine=engine,
                job_id="test_multipass_engine",
            )
            workflow.run()

            time.sleep(4)

            documents = full_dataset.get_all_documents(select_fields=["new_field"])
            for document in documents["documents"]:
                assert document["new_field"] == 6

        def test_multipass_engine_with_clustering(self, full_dataset: Dataset):
            class FitOperator(AbstractOperator):
                def __init__(self, model: MiniBatchKMeans, vector_field: str):
                    self._model = model
                    self._vector_field = vector_field
                    super().__init__(input_fields=[vector_field], output_fields=[])

                def transform(self, documents):
                    vectors = np.array(
                        [document[self._vector_field] for document in documents]
                    )
                    self._model.partial_fit(vectors)

            class PredictOperator(AbstractOperator):
                def __init__(
                    self, model: MiniBatchKMeans, vector_field: str, output_field: str
                ):
                    self._model = model
                    self._vector_field = vector_field
                    self._output_field = output_field
                    super().__init__(
                        input_fields=[vector_field], output_fields=[output_field]
                    )

                def transform(self, documents):
                    vectors = np.array(
                        [document[self._vector_field] for document in documents]
                    )
                    labels = self._model.predict(vectors)
                    for document, label in zip(documents, labels):
                        document[self._output_field] = f"cluster_{label}"

                    return documents

            vector_field = "sample_1_vector_"
            alias = "default"
            output_field = f"_cluster_.{vector_field}.{alias}"

            batch_size = 5
            kmeans_model = MiniBatchKMeans(n_clusters=3, batch_size=batch_size)
            fit_operator = FitOperator(kmeans_model, vector_field)
            predict_operator = PredictOperator(kmeans_model, vector_field, output_field)

            engine = MultiPassEngine(
                dataset=full_dataset,
                operators=[
                    fit_operator,
                    predict_operator,
                ],
                transform_chunksize=batch_size,
            )
            workflow = Workflow(
                name="test_multipass_engine_with_clustering",
                engine=engine,
                job_id="test_multipass_engine_with_clustering",
            )
            workflow.run()

            time.sleep(4)

            schema = full_dataset.schema
            assert output_field in schema
