"""
Config class
"""
from pydantic import BaseModel
from pydantic.schema import schema

class BaseConfig(BaseModel):
    def to_schema(self):
        return schema(self, title="config")
