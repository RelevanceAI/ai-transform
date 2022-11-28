import pytest

from workflows_core.dataset.dataset import Dataset
from workflows_core.dataset.field import KeyphraseField


@pytest.mark.usefixtures("full_dataset")
class TestKeyphraseCrud:
    def test_crud(self, full_dataset: Dataset):
        # documents = mock_documents(10)
        # dataset.insert_documents(documents)

        keyphrase_field = KeyphraseField(dataset=full_dataset, field="sample_1_label")

        keyphrase_field.bulk_update_keyphrases(
            updates=[
                {
                    "_id": "cat",
                    "text": "cat",
                    "count": 5,
                    "sentiment_score": 0.8,
                    "goodness_score": 1.2,
                },
                {
                    "_id": "pig",
                    "text": "pig",
                    "count": 4,
                    "sentiment_score": 0.7,
                    "goodness_score": 1.1,
                },
            ],
            alias="default",
        )

        keyphrase = keyphrase_field.get_keyphrase(alias="default", keyphrase_id="cat")
        assert "cat" == keyphrase["text"]
