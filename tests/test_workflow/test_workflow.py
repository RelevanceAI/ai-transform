from slim.engine import AbstractEngine
from slim.workflow import AbstractWorkflow
from slim.workflow.helpers import decode_workflow_token


class TestWorkflow:
    def test_workflow(self, test_engine: AbstractEngine):
        class ExampleWorkflow(AbstractWorkflow):
            pass

        workflow = ExampleWorkflow(test_engine)
        res = workflow.run()
        assert res == 0


class TestHelpers:
    def test_helpers(self, test_workflow_token: str):
        config = decode_workflow_token(test_workflow_token)
        assert isinstance(config, dict)
        assert "authorizationToken" in config
