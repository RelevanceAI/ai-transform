import logging

from typing import Optional, Dict, Any

from ai_transform.api.api import API
from ai_transform.api.helpers import process_token
from ai_transform.dataset.dataset import Dataset
from ai_transform.types import Schema
from ai_transform.errors import AuthException
from ai_transform.constants import WELCOME_MESSAGE
from ai_transform.workflow.context_manager import WorkflowContextManager


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Client:
    def __init__(self, token: str) -> None:

        self._credentials = process_token(token)
        self._token = token
        self._api = API(credentials=self.credentials)

        try:
            self.list_datasets()["datasets"]
        except:
            raise AuthException
        else:
            print(WELCOME_MESSAGE.format(self.credentials.project))

    @property
    def credentials(self):
        return self._credentials

    @property
    def api(self) -> API:
        return self._api

    def list_datasets(self):
        return self.api._list_datasets()

    def create_dataset(
        self,
        dataset_id: str,
        schema: Optional[Schema] = None,
        upsert: bool = True,
    ) -> None:
        return self.api._create_dataset(
            dataset_id=dataset_id,
            schema={} if schema is None else schema,
            upsert=upsert,
        )

    def delete_dataset(self, dataset_id: str) -> None:
        return self.api._delete_dataset(dataset_id=dataset_id)

    def Dataset(self, dataset_id: str) -> Dataset:
        self.create_dataset(dataset_id=dataset_id)
        return Dataset(api=self.api, dataset_id=dataset_id)

    def SimpleWorkflow(
        self,
        workflow_name: str,
        job_id: str,
        additional_information: str = "",
        send_email: bool = True,
        email: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
    ) -> WorkflowContextManager:
        return WorkflowContextManager(
            credentials=self.credentials,
            workflow_name=workflow_name,
            job_id=job_id,
            metadata=metadata,
            additional_information=additional_information,
            send_email=send_email,
            email=email,
        )

    def insert_temp_local_media(self, file_path: str):
        """
        Insert temporary local media.
        """
        data = self.api._get_temp_file_upload_url()
        upload_url = data["upload_url"]
        download_url = data["download_url"]
        with open(file_path, "rb") as fn_byte:
            media_content = bytes(fn_byte.read())
        response = self.api._upload_temporary_media(
            presigned_url=upload_url,
            media_content=media_content,
        )
        logger.debug(response.content)
        return {"download_url": download_url}

    def list_project_keys(self):
        return self.api._list_project_keys()

    def get_project_key(self, key: str, token: str):
        return self.api._get_project_key(key=key, token=token)

    def set_project_key(self, key: str, value: str):
        return self.api._set_project_key(key=key, value=value)

    def delete_project_key(self, key: str):
        return self.api._delete_project_key(key=key)

    def update_api_version(self, development_version: str, production_version: str):
        return self.api._update_version_aliases(development_version, production_version)
