import uuid

from typing import Type

from ai_transform.dataset.dataset import Dataset
from ai_transform.engine.stable_engine import StableEngine
from ai_transform.engine.small_batch_stable_engine import SmallBatchStableEngine

from ai_transform.operator.abstract_operator import AbstractOperator
from ai_transform.workflow.abstract_workflow import Workflow


def _random_id():
    return str(uuid.uuid4())


class TestStableEngine:
    def test_stable_engine(self, full_dataset: Dataset, test_operator: AbstractOperator):
        engine = StableEngine(full_dataset, test_operator, worker_number=0)
        workflow = Workflow(name=_random_id(), engine=engine, job_id=_random_id())
        workflow.run()
        assert engine.success_ratio == 1

    def test_small_batch_stable_engine(self, full_dataset: Dataset, test_operator: AbstractOperator):
        engine = SmallBatchStableEngine(full_dataset, test_operator)
        workflow = Workflow(name=_random_id(), engine=engine, job_id=_random_id())
        workflow.run()
        assert engine.success_ratio == 1


class TestStableEngineFilters:
    _SELECTED_FIELDS = ["sample_1_label", "sample_2_label", "sample_3_label"]

    def test_stable_engine_filters1(self, partial_dataset: Dataset, test_partial_operator: Type[AbstractOperator]):
        prev_health = partial_dataset.health()
        operator = test_partial_operator(self._SELECTED_FIELDS)

        engine = StableEngine(partial_dataset, operator, select_fields=self._SELECTED_FIELDS)
        workflow = Workflow(name=_random_id(), engine=engine, job_id=_random_id())
        workflow.run()

        post_health = partial_dataset.health()
        for input_field, output_field in zip(operator.input_fields, operator.output_fields):
            assert prev_health[input_field]["exists"] == post_health[output_field]["exists"]

        assert engine.success_ratio == 1

    def test_stable_engine_filters2(
        self, partial_dataset_with_outputs: Dataset, test_partial_operator: Type[AbstractOperator]
    ):
        prev_health = partial_dataset_with_outputs.health()
        operator = test_partial_operator(self._SELECTED_FIELDS)

        engine = StableEngine(
            partial_dataset_with_outputs, operator, select_fields=self._SELECTED_FIELDS, refresh=False
        )
        workflow = Workflow(name=_random_id(), engine=engine, job_id=_random_id())
        workflow.run()

        post_health = partial_dataset_with_outputs.health()
        for input_field, output_field in zip(operator.input_fields, operator.output_fields):
            assert prev_health[input_field]["exists"] == post_health[output_field]["exists"]

        assert engine.success_ratio == 1

    def test_stable_engine_filters3(
        self, simple_partial_dataset: Dataset, test_partial_operator: Type[AbstractOperator]
    ):
        prev_health = simple_partial_dataset.health()
        operator = test_partial_operator(["sample_1_label"])

        engine = StableEngine(simple_partial_dataset, operator, select_fields=["sample_1_label"], refresh=False)
        workflow = Workflow(name=_random_id(), engine=engine, job_id=_random_id())
        workflow.run()

        post_health = simple_partial_dataset.health()
        for input_field, output_field in zip(operator.input_fields, operator.output_fields):
            assert prev_health[input_field]["exists"] == post_health[output_field]["exists"]

        assert engine.success_ratio == 1
