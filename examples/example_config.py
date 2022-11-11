from workflows_core.config import BaseConfig

class SentimentConfig(BaseConfig):
    text_field: str


result = SentimentConfig.to_schema()

print(result)
# shouhld output: {"title": "SentimentConfig", "type": "object", "properties": {"text_field": {"title": "Text Field", "type": "string"}}, "required": ["text_field"]}