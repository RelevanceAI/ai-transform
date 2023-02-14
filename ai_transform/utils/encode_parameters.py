import base64
import json


def encode_parameters(parameters: dict):
    return base64.b64encode(json.dumps(parameters).encode()).decode()
