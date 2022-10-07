from copy import deepcopy
import pandas as pd
import pyarrow as pa

from ray.data.block import Block

from workflows_core.utils.document import Document
from workflows_core.utils.json_encoder import json_encoder
from workflows_core.operator.abstract_operator import AbstractOperator


class AbstractRayOperator(AbstractOperator):
    def __call__(self, batch: pd.DataFrame) -> Block:
        new = [
            Document(document) for document in json_encoder(batch.to_dict("records"))
        ]
        old = deepcopy(new)
        new = self.transform(new)
        new = self._postprocess(
            new, old
        )  # TODO: optimise avoiding conversion to python objects
        new = [dict(document) for document in new]
        return pa.Table.from_pylist(new)
