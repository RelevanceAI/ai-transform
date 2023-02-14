from ai_transform.types import Credentials


def process_token(token: str) -> Credentials:
    return Credentials(*token.split(":"))


def requests_post_mock(url="", headers=None, json=None):
    """
    Use as follows to help the API team debug responses
    import requests
    response = requests_post_mock(
        url=f"https://api-{region}.stack.tryrelevance.com/latest/workflows/types/bulk_update",
        headers={"Authorization": auth},
        json={
            "updates": workflows[0:20],
            "version": "vtest"
        }
    )
    print(response)
    """
    # TODO: Add get request etc.
    if headers is None:
        headers = {}
    if json is None:
        json = {}
    return f"""
import requests
r = requests.post(url='{url}',headers={headers},json={json})
  """
