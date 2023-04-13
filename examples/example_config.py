# For a full example - see
# examples/workflows/sentiment_example.py
# In that example - we inherit the BaseConfig class
from ai_transform.config import BaseConfig
from typing import Optional
from pydantic import Field


class SentimentConfig(BaseConfig):
    # BaseConfig automatically handles authorizationToken, job_id, etc.
    # We put the SentimentConfig here so that we can auto-generate
    # a JSONSchema
    textFields: str = Field(..., description="The text field to run sentiment on.")
    alias: Optional[str] = Field(None, description="The alias for each sentiment component.")
    transform_chunksize: Optional[int] = Field(8, description="The amount to transform at any 1 time.")


result = SentimentConfig.to_schema()

print(result)
# shouhld output: {"title": "SentimentConfig", "type": "object", "properties": {"text_field": {"title": "Text Field", "type": "string"}}, "required": ["text_field"]}
