"""
Test Keyphrase CRUD endpoints
"""
import pytest
import uuid
from workflows_core.utils import DocumentList
from workflows_core.dataset.dataset import Dataset
from workflows_core.api.client import Client
from workflows_core.utils.example_documents import mock_documents

# write a few tests
class TestClient:
    def test_upsert_keyphrases(
        self,
        test_client: Client, 
        test_keyphrase_dataset: Dataset,
        test_keyphrases: DocumentList
    ):
        # Test that upserting keyphrases is good
        field = "sample_1_label"
        alias = "default"
        test_dataset_id = test_keyphrase_dataset.dataset_id
        keyphrase_id = str(uuid.uuid4())
        result = test_client._api._bulk_update_keyphrase(
            dataset_id=test_dataset_id,
            field=field,
            alias=alias,
            updates=test_keyphrases,
            keyphrase_id=keyphrase_id
        )
        # Now that that we saw the actual dataset
        result = test_client._api._get_keyphrase(
            test_dataset_id, field=field, 
            alias=alias,
            keyphrase_id=keyphrase_id
        )
        assert len(result['results']) > 0, result
