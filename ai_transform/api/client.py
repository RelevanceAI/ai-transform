import os
import warnings

from typing import Optional, Dict, Any, Union

from ai_transform.api.api import API
from ai_transform.api.helpers import process_token
from ai_transform.dataset.dataset import Dataset
from ai_transform.types import Schema
from ai_transform.errors import AuthException
from ai_transform.constants import WELCOME_MESSAGE
from ai_transform.workflow.context_manager import WorkflowContextManager
from ai_transform.logger import ic


class Client:
    def __init__(self, token: str = None, authenticate: bool = True) -> None:
        if token is None:
            token = os.getenv("DEVELOPMENT_TOKEN")

        self._credentials = process_token(token)
        self._token = token
        self._api = API(credentials=self.credentials)

        if authenticate:
            try:
                self.list_datasets()["datasets"]
            except:
                raise AuthException
            else:
                print(WELCOME_MESSAGE.format(self.credentials.project))
        else:
            warnings.warn(
                "You have opted to not authenticate on client instantiation. Your token may or may not be valid."
            )

    @property
    def credentials(self):
        return self._credentials

    @property
    def api(self) -> API:
        return self._api

    def list_datasets(self):
        return self.api._list_datasets()

    def create_dataset(
        self, dataset_id: str, schema: Optional[Schema] = None, upsert: bool = True, expire: bool = False
    ) -> None:
        return self.api._create_dataset(
            dataset_id=dataset_id, schema={} if schema is None else schema, upsert=upsert, expire=expire
        )

    def delete_dataset(self, dataset_id: str) -> None:
        return self.api._delete_dataset(dataset_id=dataset_id)

    def Dataset(self, dataset_id: str, expire: bool = False) -> Dataset:
        self.create_dataset(dataset_id=dataset_id, expire=expire)
        return Dataset(api=self.api, dataset_id=dataset_id)

    def SimpleWorkflow(
        self,
        workflow_name: str,
        job_id: str,
        additional_information: str = "",
        send_email: bool = True,
        email: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
        output: dict = None,
    ) -> WorkflowContextManager:
        return WorkflowContextManager(
            credentials=self.credentials,
            workflow_name=workflow_name,
            job_id=job_id,
            metadata=metadata,
            additional_information=additional_information,
            send_email=send_email,
            email=email,
            output=output,
        )

    def insert_temp_local_media(self, file_path_or_bytes: Union[str, bytes], ext: str = None):
        """
        Insert temporary local media.
        """
        if isinstance(file_path_or_bytes, str):
            with open(file_path_or_bytes, "rb") as fn_byte:
                media_content = bytes(fn_byte.read())
            if ext is None:
                _, ext = os.path.splitext(file_path_or_bytes)
                ext = ext[1:] # remove leading `.`
            
        elif isinstance(file_path_or_bytes, bytes):
            media_content = file_path_or_bytes
            if ext is None:
                raise ValueError("Must set file ext i.e. `csv` or `png`")

        else:
            raise ValueError("`file_path_or_bytes` must be one of type `str` or `bytes`")

        data = self.api._get_temp_file_upload_url(ext)
        upload_url = data["upload_url"]
        download_url = data["download_url"]
            
        response = self.api._upload_temporary_media(presigned_url=upload_url, media_content=media_content)
        ic(response.content)
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

    def openai_completion(
        self,
        prompt: str,
        model: str = "text-davinci-003",
        workflows_admin_token: str = None,
        max_tokens: int = 20,
        temperature: float = 0.0,
    ):
        if workflows_admin_token is None:
            workflows_admin_token = os.getenv("WORKFLOWS_ADMIN_TOKEN")
        return self.api._openai_completion(workflows_admin_token, model, prompt, max_tokens, temperature)
