import io
import sys
import json

from typing import List, Dict, Any

from workflows_core.utils import json_encoder

ONE_MB = 2**20
MAX_MB = 20


def get_optimal_chunksize(
    documents: List[Dict[str, Any]], encoding: str = "utf-8"
) -> int:
    """
    Calculates the optimal number of documents to recieve
    """
    documents = json_encoder(documents)
    documents_json_string = json.dumps(documents)

    buffer = io.BytesIO()
    buffer.write(bytes(documents_json_string, encoding=encoding))
    payload_bytes = sys.getsizeof(buffer)

    avg_mb_per_document = payload_bytes / ONE_MB
    chunksize = min(1024, MAX_MB / avg_mb_per_document)
    return int(chunksize)
