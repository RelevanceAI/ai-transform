from slim.engine import AbstractEngine
from slim.workflow import AbstractWorkflow


class TestWorkflow:
    def test_workflow(self, test_engine: AbstractEngine):
        class ExampleWorkflow(AbstractWorkflow):
            pass

        workflow = ExampleWorkflow(test_engine)
        res = workflow.run()
        assert res == 0
