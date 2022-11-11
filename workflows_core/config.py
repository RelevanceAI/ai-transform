"""
Config class
"""
from pydantic import BaseModel
from pydantic.schema import schema

class BaseConfig(BaseModel):
    @classmethod
    def to_schema(self):
        return self.schema_json()
