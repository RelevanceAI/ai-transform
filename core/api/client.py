from typing import Optional

from core.api.api import API
from core.api.helpers import process_token
from core.dataset import Dataset
from core.types import Schema
from core.errors import AuthException
from core.constants import WELCOME_MESSAGE


class Client:
    def __init__(self, token: str) -> None:

        self._credentials = process_token(token)
        self._api = API(credentials=self._credentials)

        try:
            self.list_datasets()["datasets"]
        except:
            raise AuthException
        else:
            print(WELCOME_MESSAGE.format(self._credentials.project))

    def list_datasets(self):
        return self._api._list_datasets()

    def create_dataset(
        self,
        dataset_id: str,
        schema: Optional[Schema] = None,
        upsert: bool = True,
    ) -> None:
        return self._api._create_dataset(
            dataset_id=dataset_id,
            schema={} if schema is None else schema,
            upsert=upsert,
        )

    def delete_dataset(self, dataset_id: str) -> None:
        return self._api._delete_dataset(dataset_id=dataset_id)

    def Dataset(self, dataset_id: str) -> "Dataset":
        self.create_dataset(dataset_id=dataset_id)
        return Dataset(api=self._api, dataset_id=dataset_id)
