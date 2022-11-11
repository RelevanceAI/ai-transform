from workflows_core.config import BaseConfig

class SentimentConfig(BaseConfig):
    text_field: str


result = SentimentConfig.to_schema()

print(result)
