from workflows_core.engine.abstract_engine import AbstractEngine
from workflows_core.workflow.abstract_workflow import AbstractWorkflow


class TestWorkflow:
    def test_workflow(self, test_engine: AbstractEngine):
        workflow = AbstractWorkflow(
            name="test_workflow",
            engine=test_engine,
            job_id="test_job",
        )
        res = workflow.run()
        assert res is None
