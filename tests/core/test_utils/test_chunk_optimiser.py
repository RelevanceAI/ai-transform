from workflows_core.utils import mock_documents, get_optimal_chunksize, json_encoder


def test_chunk_optimiser():
    documents = mock_documents(100, 128)
    documents = json_encoder(documents)
    chunksize = get_optimal_chunksize(documents)
    assert 0 < chunksize <= 1024
