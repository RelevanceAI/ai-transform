"""
Config class
"""
import uuid
import json
import base64
import argparse

from typing import Optional, List
from workflows_core.workflow.helpers import decode_workflow_token

from pydantic import BaseModel, Field


def generate_random_string():
    return str(uuid.uuid4())


class BaseConfig(BaseModel):
    """
    An example configuration for workflows so that we can modify the the schema.

    .. code-block::

        from workflows_core.config import BaseConfig

        class SentimentConfig(BaseConfig):
            text_field: str


        result = SentimentConfig.to_schema()

    """

    authorizationToken: str = None
    dataset_id: Optional[str] = None
    job_id: Optional[str] = Field(
        default_factory=generate_random_string, description="the job ID"
    )
    total_workers: Optional[int] = Field(default=None, description="Total workers.")
    send_email: Optional[bool] = Field(
        default=True,
        description="If True, sends an email upon completion. Otherwise, False.",
    )
    additional_information: Optional[str] = Field(
        default="", description="What to include in the e-mail."
    )
    filters: Optional[list] = Field(default=[], description="Filters to apply.")
    documents: Optional[list] = Field(
        default=None,
        description="You can submit documents and have it run immediately.",
    )

    @classmethod
    def to_schema(self):
        return self.schema_json()

    def read_from_argparser(self, argparser: argparse.ArgumentParser):
        # Enables behavior such
        # reads in required attributes from argparser
        for k in self.dict():
            # gets the required attributes like 'text_field'
            setattr(self, k, getattr(argparser, k))

    @classmethod
    def read_token(self, token: str):
        # Enables behavior such
        # reads in required attributes from argparser
        config_dict = decode_workflow_token(token)
        return self.parse_obj(config_dict)

    @classmethod
    def read_dict(self, data: dict):
        # Read from data dictionary
        for k in self.dict():
            setattr(self, k, data.get(k))

    def get(self, value, default=None):
        """
        For backwards compatibility with previous dictionary-input
        configs
        """
        if hasattr(self, value):
            return getattr(self, value)
        else:
            return default

    def __getitem__(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def generate_token(self, *args, **kwargs):
        # Generates a token for you anytime
        config: dict = self.dict()
        return base64.b64encode(json.dumps(config).encode()).decode()


class BaseTransformConfig(BaseConfig):
    """
    Same as BaseConfig but a few more additional attributes.
    This is suitable for basic transformations that go through
    the same pulling, transforming and then pushing (e.g. sentiment or emotion workflows)
    """

    pull_chunksize: Optional[int] = Field(
        default=1000,
        description="How many do you want to download and upload to the server.",
    )
    transform_chunksize: Optional[int] = Field(
        default=20, description="How many do you want to transform each time?"
    )
    refresh: Optional[bool] = Field(
        default=False,
        description="If True, re-runs the workflow on the entire dataset.",
    )
    output_to_status: Optional[bool] = Field(
        default=False, description="If True, it will output results to status object."
    )
    documents: Optional[List[object]] = Field(
        default_factory=lambda: [],
        description="If passed in, documents will be used instead of dataset.",
    )
    limit_documents: Optional[int] = Field(
        default=None,
        description="If passed in, the transform will be limited to the number of documents specified here",
    )
