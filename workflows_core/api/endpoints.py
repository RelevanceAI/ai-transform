import requests
from bs4 import BeautifulSoup

API_URL = "https://api-1e3042.stack.relevance.ai/latest/core/openapi_schema.json"
DOCUMENTATION = "https://api-1e3042.stack.relevance.ai/latest/core/documentation"


def main():
    endpoints = requests.get(url=API_URL).json()
    return


if __name__ == "__main__":
    main()
