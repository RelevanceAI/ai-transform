from ai_transform.logger import Logger
from ai_transform.utils.example_documents import mock_documents


class TestLogger:
    def test_logger(self):
        logger = Logger()

        logger("this is debug statement")

        documents = mock_documents()
        logger(documents)

        logger(documents[0])
        logger(documents[0], no_vectors=True)
