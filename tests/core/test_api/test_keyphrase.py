"""
Test Keyphrase CRUD endpoints
"""
import uuid

from ai_transform.utils import List
from ai_transform.dataset.dataset import Dataset
from ai_transform.api.client import Client
from ai_transform.utils.keyphrase import Keyphrase


# write a few tests
class TestClient:
    def test_upsert_keyphrases(
        self, test_client: Client, test_keyphrase_dataset: Dataset, test_keyphrases: List[Keyphrase]
    ):
        # Test that upserting keyphrases is good
        field = "sample_1_label"
        alias = "default"
        test_dataset_id = test_keyphrase_dataset.dataset_id

        result = test_client.api._bulk_update_keyphrase(
            dataset_id=test_dataset_id, field=field, alias=alias, updates=test_keyphrases
        )
        # Now that that we saw the actual dataset
        result = test_client.api._get_keyphrase(test_dataset_id, field=field, alias=alias, keyphrase_id="word")
        print(result)
        assert result["text"] == "word" and result["keyphrase_score"] == 10, result
