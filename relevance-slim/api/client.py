from ..api.helpers import process_token


class Client:
    def __init__(self, token: str) -> None:
        self._credentials = process_token(token)
