import pytest

from workflows_core.dataset.dataset import Dataset
from workflows_core.utils.keyphrase import Keyphrase


@pytest.mark.usefixtures("full_dataset")
class TestKeyphraseCrud:
    def test_crud(self, full_dataset: Dataset):
        keyphrase_field = full_dataset["_keyphrase_.sample_1_label.default"]

        keyphrase1 = Keyphrase(
            text="cat",
            _id="cat",
            ancestors=[],
            parents=[],
            level=0,
            keyphrase_score=1.1,
            frequency=2,
            metadata={},
        )
        keyphrase2 = Keyphrase(
            text="cat",
            _id="dog",
            ancestors=["cat"],
            parents=["cat"],
            level=1,
            keyphrase_score=1.1,
            frequency=1,
            metadata={},
        )
        updates = [keyphrase1, keyphrase2]
        response = keyphrase_field.bulk_update_keyphrases(
            updates=updates,
        )

        keyphrase = keyphrase_field.get_keyphrase(keyphrase_id="cat")
        assert "cat" == keyphrase["text"]
