import os
from ai_transform.utils import encode_parameters

token = encode_parameters(
    parameters={
        "text_field": "review_title",
        "dataset_id": "a-0",
        "authorizationToken": os.environ["TEST_TOKEN"],
        "n_clusters": 8,
    }
)

print(token)
