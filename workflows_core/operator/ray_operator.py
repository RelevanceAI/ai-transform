import pandas as pd
import pyarrow as pa

from ray.data.block import Block
from workflows_core.operator import AbstractOperator


class AbstractRayOperator(AbstractOperator):
    def __call__(self, batch: pd.DataFrame) -> Block:
        batch = pd.json_normalize(batch.to_dict("records"))
        old = batch.copy()
        new = self.transform(batch)
        new = self._postprocess(new, old)
        return pa.Table.from_pandas(new)

    @staticmethod
    def _postprocess(new: pd.DataFrame, old: pd.DataFrame):
        import pdb

        pdb.set_trace()
        return new
