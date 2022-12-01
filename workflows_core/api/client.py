from typing import Optional, Dict, Any

from workflows_core.api.api import API
from workflows_core.api.helpers import process_token
from workflows_core.dataset.dataset import Dataset
from workflows_core.types import Schema
from workflows_core.errors import AuthException
from workflows_core.constants import WELCOME_MESSAGE
from workflows_core.workflow.simple_workflow import SimpleWorkflow


class Client:
    def __init__(self, token: str) -> None:

        self._credentials = process_token(token)
        self._token = token
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

    def Dataset(self, dataset_id: str) -> Dataset:
        self.create_dataset(dataset_id=dataset_id)
        return Dataset(api=self._api, dataset_id=dataset_id)

    def SimpleWorkflow(
        self,
        workflow_name: str,
        job_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        additional_information: str = "",
        send_email: bool = True,
        worker_number: int = None,
        **kwargs
    ):
        return SimpleWorkflow(
            credentials=self._credentials,
            workflow_name=workflow_name,
            job_id=job_id,
            metadata=metadata,
            additional_information=additional_information,
            send_email=send_email,
            worker_number=worker_number,
            **kwargs,
        )
