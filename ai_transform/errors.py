class AuthException(Exception):
    pass


class MaxRetriesError(Exception):
    pass


class WorkflowFailedError(Exception):
    pass


class UserFacingError(Exception):
    """This error is shown to the user"""

    def __init__(self, error_message: str, client=None, job_id: str = None, workflow_name: str = None, **kwargs):
        self.error_message = error_message
        self.client = client
        self.job_id = job_id
        self.workflow_name = workflow_name
        self.raise_error_with_client(**kwargs)
        super().__init__()

    def raise_error_with_client(self, **kwargs):
        self.client._api._set_workflow_status(
            job_id=self.job_id,
            user_errors=self.error_message,
            workflow_name=self.workflow_name,
            status="failed",
            **kwargs,
        )
