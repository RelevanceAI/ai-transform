"""
Config class
"""
import argparse
import base64
import json
import uuid
from typing import Optional
from workflows_core.workflow.helpers import decode_workflow_token

from pydantic import BaseModel, Field
from pydantic.schema import schema

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
    dataset_id: str = None 
    job_id: Optional[str] = str(uuid.uuid4()) # if missing - generates a random one
    total_workers: Optional[int] = Field(default=None, description="Total workers.")
    send_email: Optional[bool] = Field(default=True, description="Missing")

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
        for k in self.dict():
            # gets the required attributes like 'text_field'
            setattr(self, k, config_dict.get(k))
        
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
