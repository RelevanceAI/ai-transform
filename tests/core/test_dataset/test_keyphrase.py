import pytest

from workflows_core.dataset.dataset import Dataset
from workflows_core.dataset.field import KeyphraseField


@pytest.mark.usefixtures("full_dataset")
class TestKeyphraseCrud:
    def test_crud(self, full_dataset: Dataset):
        # documents = mock_documents(10)
        # dataset.insert_documents(documents)

        keyphrase_field = KeyphraseField(dataset=full_dataset, field="sample_1_label")

        keyphrase_field.update_keyphrases(keyphrases_update={"cat": {"count": 5, "sentiment_score": 0.8, "goodness_score": 1.2},
                                                             "pig": {"count": 4, "sentiment_score": 0.7, "goodness_score": 1.1}
                                                             },
                                          alias='default')
        keyphrases = keyphrase_field.get_keyphrases(alias='default')
        assert "cat" in keyphrases and "pig" in keyphrases

        keyphrase_field.insert_keyphrases(keyphrases_insert={"cat": {"count": 5, "sentiment_score": 0.7, "goodness_score": 1.2},
                                                             "duck": {"count": 6, "sentiment_score": 0.9, "goodness_score": 1.5}
                                                             },
                                          alias='default')
        keyphrases = keyphrase_field.get_keyphrases(alias='default')
        assert "cat" in keyphrases and "pig" in keyphrases and "duck" in keyphrases
        assert keyphrases['cat']['count'] == 10
        assert keyphrases['cat']['sentiment_score'] == 0.7

        keyphrase_field.remove_keyphrases(keyphrases_remove={"cat": {"count": 5, "sentiment_score": 0.8, "goodness_score": 1.2},
                                                             "chicken": {"count": 4, "sentiment_score": 0.7, "goodness_score": 1.1}
                                                             },
                                          alias='default')
        keyphrases = keyphrase_field.get_keyphrases(alias='default')
        assert "cat" not in keyphrases and "pig" in keyphrases and "duck" in keyphrases and "chicken" not in keyphrases
