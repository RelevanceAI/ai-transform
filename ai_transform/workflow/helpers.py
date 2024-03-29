"""
Base64 decoding for workflows
"""
import argparse
import os
import base64
import json
import time
from ai_transform.dataset.dataset import Dataset
from typing import Any, Mapping


def decode_workflow_token(token: str) -> Mapping[str, Any]:
    """
    It takes a token, decodes it, and returns the decoded token

    Parameters
    ----------
    token
        The token that was generated by the workflow.

    Returns
    -------
        A dictionary of the workflow configuration.

    """
    config = json.loads(base64.b64decode(token + "==="))
    # Set workflow ID for tracking
    os.environ["WORKFLOW_ID"] = config.get("job_id", "")
    return config


def read_token_from_script():
    """
    Reads in a token from script and returns a config as a
    dictionary object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("token", help="The token used for the workflow config.")
    args = parser.parse_args()
    token = args.token
    config = decode_workflow_token(token)
    return config


def safe_divide(x, y):
    if y == 0:
        return 0
    else:
        return x / y


def calculate_health_proportion(health: dict):
    """
    Example health object looks like this:
    {"exists": 300, "missing": 100}
    """
    return safe_divide(health["exists"], (health["exists"] + health["missing"]))


def poll_until_health_updates(
    dataset: Dataset, field: str, minimum_coverage: float = 0.95, sleep_timer: int = 10, max_time: int = 600
):
    """
    Poll until the dataset has all required dataset.
    """
    health = dataset.health()
    field_health = health[field]
    proportion = calculate_health_proportion(field_health)
    start_time = time.time()
    while proportion <= minimum_coverage:
        time.sleep(sleep_timer)
        health = dataset.health()
        field_health = health[field]
        proportion = calculate_health_proportion(field_health)
        current_time = time.time()
        if (current_time - start_time) > max_time:
            break
    return


def poll_until_health_updates_with_input_field(
    dataset: Dataset,
    input_field: str,
    output_field: str,
    minimum_coverage: float = 0.95,
    sleep_timer: int = 10,
    max_time: int = 600,
):
    """
    Poll until the dataset has all required dataset.
    Arguments:
        dataset: Dataset object
        input_field: the input field to poll on
        outout_field: the output field,
        minimum_coverage: the minimum amount of coverage
            required based on the input field
        sleep_timer:
            the time in between each poll request
    """
    input_field_health = dataset.health()[input_field]
    min_coverage = calculate_health_proportion(input_field_health) * minimum_coverage
    return poll_until_health_updates(
        dataset=dataset, field=output_field, minimum_coverage=min_coverage, sleep_timer=sleep_timer, max_time=max_time
    )


def encode_config(data: dict):
    return base64.b64encode(json.dumps(data).encode()).decode()
