"""
Config class
"""
import argparse
from pydantic import BaseModel
from pydantic.schema import schema

class BaseConfig(BaseModel):
    @classmethod
    def to_schema(self):
        return self.schema_json()
    
    def read_from_argparser(self, argparser: argparse.ArgumentParser):
        # Enables behavior such
        # reads in required attributes from argparser
        for k in self.dict():
            # gets the required attributes like 'text_field'
            setattr(self, k, getattr(argparser, k))
        
    def get(self, value, default=None):
        if hasattr(self, value):
            return getattr(self, value)
        else:
            return default
