from slim.utils.types import Credentials


def process_token(token: str) -> Credentials:
    return Credentials(*token.split(":"))