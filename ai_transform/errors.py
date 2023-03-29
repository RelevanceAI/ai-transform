class AuthException(Exception):
    pass


class MaxRetriesError(Exception):
    pass


class WorkflowFailedError(Exception):
    pass


class UserFacingError(Exception):
    """This error is shown to the user"""

    def __init__(
        self, error_message: str, client, job_id: str, workflow_name: str, **kwargs
    ):
        client._api._set_workflow_status(
            job_id=job_id,
            user_errors=error_message,
            workflow_name=workflow_name,
            status="failed",
            **kwargs
        )
        self.error_message = error_message
        super().__init__()
