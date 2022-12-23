"""
Test Keyphrase CRUD endpoints
"""
import uuid

from workflows_core.utils import DocumentList
from workflows_core.dataset.dataset import Dataset
from workflows_core.api.client import Client

# write a few tests
class TestClient:
    def test_upsert_keyphrases(
        self,
        test_client: Client,
        test_keyphrase_dataset: Dataset,
        test_keyphrases: DocumentList,
    ):
        # Test that upserting keyphrases is good
        field = "sample_1_label"
        alias = "default"
        test_dataset_id = test_keyphrase_dataset.dataset_id
        keyphrase_id = str(uuid.uuid4())
        keyphrase_ids = []
        for d in test_keyphrases:
            keyphrase_id = str(uuid.uuid4())
            keyphrase_ids.append(keyphrase_id)
            d["_id"] = keyphrase_id

        result = test_client._api._bulk_update_keyphrase(
            dataset_id=test_dataset_id,
            field=field,
            alias=alias,
            updates=test_keyphrases,
        )
        # Now that that we saw the actual dataset
        result = test_client._api._get_keyphrase(
            test_dataset_id, field=field, alias=alias, keyphrase_id=keyphrase_id
        )
        print(result)
        assert result["text"] == "word" and result["keyphrase_score"] == 10, result
