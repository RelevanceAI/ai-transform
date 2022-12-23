import pytest

from workflows_core.dataset.dataset import Dataset


@pytest.mark.usefixtures("full_dataset")
class TestKeyphraseCrud:
    def test_crud(self, full_dataset: Dataset):

        keyphrase_field = full_dataset["_keyphrase_.sample_1_label.default"]

        response = keyphrase_field.bulk_update_keyphrases(
            updates=[
                {
                    "_id": "cat",
                    "text": "cat",
                    "frequency": 5,
                    # "sentiment_score": 0.8,
                    "keyphrase_score": 1.2,
                },
                {
                    "_id": "pig",
                    "text": "pig",
                    "frequency": 4,
                    # "sentiment_score": 0.7,
                    "keyphrase_score": 1.1,
                },
            ],
        )

        keyphrase = keyphrase_field.get_keyphrase(keyphrase_id="cat")
        assert "cat" == keyphrase["text"]
