import itertools
import numpy as np

from types import FunctionType

from workflows_core.api.client import Client
from workflows_core.dataset.field import Field, ClusterField, KeyphraseField


def list_methods(cls):
    return set(
        x
        for x, y in cls.__dict__.items()
        if isinstance(y, (FunctionType, classmethod, staticmethod))
    )


def list_parent_methods(cls):
    return set(
        itertools.chain.from_iterable(
            list_methods(c).union(list_parent_methods(c)) for c in cls.__bases__
        )
    )


def list_subclass_methods(cls, is_narrow: bool = False):
    methods = list_methods(cls)
    if is_narrow:
        parentMethods = list_parent_methods(cls)
        return set(cls for cls in methods if not (cls in parentMethods))
    else:
        return methods


class TestField:
    FIELD_METHODS = list_subclass_methods(Field)
    CLUSTER_FIELD_METHODS = list_subclass_methods(ClusterField)
    KEYPHRASE_FIELD_METHODS = list_subclass_methods(KeyphraseField)

    def test_cluster_field_methods(self):
        for method in self.CLUSTER_FIELD_METHODS:
            assert method in self.FIELD_METHODS

    def test_keyphrase_field_methods(self):
        for method in self.KEYPHRASE_FIELD_METHODS:
            assert method in self.FIELD_METHODS


def test_create_centroid_documents(test_client: Client, test_dataset_id: str):
    vectors = np.random.random((20, 4))
    labels = np.arange(10, dtype=np.longdouble)
    labels = np.concatenate((labels, labels), axis=-1)

    test_dataset = test_client.Dataset(test_dataset_id)
    cluster_field = "_cluster_.vector_field.alias"

    centroid_documents = test_dataset[cluster_field].create_centroid_documents(
        vectors, labels
    )

    assert len(centroid_documents) == 10
    for centroid_document in centroid_documents:
        assert len(centroid_document["vector_field"]) == 4
