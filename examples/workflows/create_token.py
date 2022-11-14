import os
from workflows_core.utils import encode_parameters

token = encode_parameters(
    parameters={
        "numeric_field": "rating",
        "dataset_id": "aaa-test",
        "authorizationToken": os.environ["TEST_TOKEN"],
    }
)

print(token)
