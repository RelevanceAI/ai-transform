import ray
import pandas as pd
import pyarrow as pa

from functools import partial
from typing import Any, Dict, List, Optional

from workflows_core.types import Filter
from workflows_core.constants import ONE_MB
from workflows_core.dataset import Dataset
from workflows_core.engine import AbstractEngine

from ray.data.datasource import Datasource, ReadTask, Reader
from ray.data.context import DatasetContext
from ray.data._internal.util import _check_pyarrow_version
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


class _RelevanceDataSourceReader(Reader):
    def __init__(
        self,
        dataset: Dataset,
        chunksize: int,
        size: int,
        after_ids: List[str],
        select_fields: List[str],
        filters: List[Filter],
    ):

        self._dataset = dataset
        self._chunksize = chunksize
        self._size = size
        self._after_ids = after_ids
        self._select_fields = select_fields
        self._filters = filters

        self._schema = self._get_pyarrow_schema()

    def _get_pyarrow_schema(self):
        result = self._dataset._api._get_where(
            dataset_id=self._dataset._dataset_id,
            page_size=1,
            select_fields=self._select_fields,
            filters=self._filters,
        )
        schema = pa.Table.from_pylist(result["documents"]).schema
        return schema

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return ONE_MB * self._size

    def prepare_read(*args, **kwargs):
        pass

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        _check_pyarrow_version()
        read_tasks: List[ReadTask] = []

        def get_data(*args, **kwargs):
            result = self._dataset._api._get_where(
                dataset_id=self._dataset._dataset_id, *args, **kwargs
            )
            table = pa.Table.from_pylist(result["documents"])
            return [table]

        for after_id in self._after_ids:
            meta = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=self._schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    read_fn=partial(
                        get_data,
                        page_size=self._chunksize,
                        select_fields=self._select_fields,
                        filters=self._filters,
                        after_id=after_id,
                    ),
                    metadata=meta,
                )
            )

        return read_tasks


class RelevanceDatasource(Datasource):
    def __init__(
        self,
        dataset: Dataset,
        chunksize: int,
        size: int,
        after_ids: List[str],
        select_fields: List[str],
        filters: List[Filter],
    ):
        ctx = DatasetContext.get_current()
        ctx.enable_tensor_extension_casting = False

        self._dataset = dataset
        self._chunksize = chunksize
        self._size = size
        self._after_ids = after_ids
        self._select_fields = select_fields
        self._filters = filters

    def create_reader(self):
        return _RelevanceDataSourceReader(
            self._dataset,
            self._chunksize,
            self._size,
            self._after_ids,
            self._select_fields,
            self._filters,
        )

    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        ray_remote_args: Dict[str, Any],
        **write_args,
    ) -> List[ObjectRef[Any]]:
        """
        Do the writing
        """

        def write(block: Block) -> Any:
            documents = block.to_pylist()
            return self._dataset.insert_documents(documents=documents)

        write_tasks = []
        if ray_remote_args is not None:
            write_block = cached_remote_fn(write).options(**ray_remote_args)
        else:
            write_block = cached_remote_fn(write)
        for block in blocks:
            write_task = write_block.remote(block)
            write_tasks.append(write_task)
        return write_tasks

    def on_write_complete(self, write_results: List[Any]) -> None:
        # post-write hook
        print("complete")

    def on_write_failed(
        self, write_results: List[ObjectRef[Any]], error: Exception
    ) -> None:
        # post-write hook
        print("failed")


class RayEngine(AbstractEngine):
    def __init__(
        self,
        compute: str = "actors",
        device: str = "cuda:0",
        num_gpus: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._compute = compute
        self._device = device
        self._num_gpus = num_gpus

        after_ids = self._get_after_ids()
        self._data_sink = RelevanceDatasource(
            dataset=self.dataset,
            chunksize=self.chunksize,
            size=self.size,
            after_ids=after_ids,
            select_fields=self._select_fields,
            filters=self._filters,
        )
        self._data_source = ray.data.read_datasource(self._data_sink)

    def _get_after_ids(self):
        iterator = self.iterate(select_fields=["_id"])

        after_ids = [None]
        for chunk in iterator:
            after_ids.append([chunk[-1]["_id"]])

        return after_ids

    def apply(self) -> Any:

        results = self._data_source.map_batches(
            self.operator, compute=self._compute, num_gpus=self._num_gpus
        )
        results.write_datasource(self._data_sink)
        return
