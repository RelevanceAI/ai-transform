import pytest


PROJECT: str = "None"
API_KEY: str = "None"
REGION: str = "None"
FIREBASE_UID: str = "None"

TOKEN = f"{PROJECT}:{API_KEY}:{REGION}:{FIREBASE_UID}"


@pytest.fixture(scope="session")
def test_token():
    return TOKEN
