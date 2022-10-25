from typing import Optional

from structlog import get_logger

from workflows_core.api.api import API
from workflows_core.api.helpers import process_token
from workflows_core.dataset.dataset import Dataset
from workflows_core.types import Schema
from workflows_core.constants import WELCOME_MESSAGE


logger = get_logger(__file__)


class Client:
    def __init__(self, token: str) -> None:

        self._credentials = process_token(token)
        self._token = token
        self._api = API(credentials=self._credentials)

        try:
            self.list_datasets()["datasets"]
        except Exception as e:
            logger.exception(e, stack_info=True)
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

    def Dataset(self, dataset_id: str) -> Dataset:
        self.create_dataset(dataset_id=dataset_id)
        return Dataset(api=self._api, dataset_id=dataset_id)
