"""
Utility function to mock documents. Aimed at helping users reproduce errors
if required.
The schema for the documents is as follows:

.. code-block::

    {'_chunk_': 'chunks',
    '_chunk_.label': 'text',
    '_chunk_.label_chunkvector_': {'chunkvector': 5},
    'insert_date_': 'date',
    'sample_1_description': 'text',
    'sample_1_label': 'text',
    'sample_1_value': 'numeric',
    'sample_1_vector_': {'vector': 5},
    'sample_2_description': 'text',
    'sample_2_label': 'text',
    'sample_2_value': 'numeric',
    'sample_2_vector_': {'vector': 5},
    'sample_3_description': 'text',
    'sample_3_label': 'text',
    'sample_3_value': 'numeric',
    'sample_3_vector_': {'vector': 5}}

Parameters
------------

number_of_documents: int
    The number of documents to mock
vector_length: int
    The length of vectors

.. code-block::

    from slim.utils import mock_documents

    documents = mock_documents(10)

"""

import random
import string

from ai_transform.types import Vector
from ai_transform.utils.document import Document
from ai_transform.utils.document_list import DocumentList
import uuid

rd = random.Random()
rd.seed(2)


def create_id():
    # This makes IDs reproducible for tests related to Modulo function
    return str(uuid.UUID(int=rd.getrandbits(128)))


def generate_random_string(string_length: int = 5) -> str:

    """Generate a random string of letters and numbers"""
    return "".join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(string_length)
    )


def generate_random_vector(vector_length: int = 5) -> Vector:
    """Generate a random list of floats"""
    return [random.random() for _ in range(vector_length)]


def generate_random_label(label_value: int = 5) -> str:
    return f"label_{random.randint(0, label_value)}"


def generate_random_integer(min: int = 0, max: int = 100) -> int:
    return random.randint(min, max)


def vector_document(vector_length: int) -> Document:
    document = {
        "_id": create_id(),
        "sample_1_label": generate_random_label(),
        "sample_2_label": generate_random_label(),
        "sample_3_label": generate_random_label(),
        "sample_1_description": generate_random_string(),
        "sample_2_description": generate_random_string(),
        "sample_3_description": generate_random_string(),
        "sample_1_vector_": generate_random_vector(),
        "sample_2_vector_": generate_random_vector(),
        "sample_3_vector_": generate_random_vector(),
        "sample_1_value": generate_random_integer(),
        "sample_2_value": generate_random_integer(),
        "sample_3_value": generate_random_integer(),
        "_chunk_": [
            {
                "label": generate_random_label(),
                "label_chunkvector_": generate_random_vector(),
            }
        ],
    }

    return Document(document)


def mock_documents(n: int = 100, vector_length: int = 5) -> DocumentList:
    return DocumentList([vector_document(vector_length) for _ in range(n)])


def static_documents(n: int = 100) -> DocumentList:
    return DocumentList([{"text_field": str(i), "numeric_field": i} for i in range(n)])


def tag_document(n_tags: int = 5):
    document = {
        "text": "This is some random text",
        "_surveytag_": {
            "text": [
                {"label": generate_random_label(), "value": random.random()}
                for _ in range(random.randint(0, n_tags))
            ]
        },
    }
    return Document(document)


def tag_documents(n: int = 100):
    return DocumentList([tag_document() for _ in range(n)])
