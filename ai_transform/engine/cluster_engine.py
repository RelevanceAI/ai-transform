import logging

from typing import Any

from ai_transform.engine.abstract_engine import AbstractEngine
from tqdm.auto import tqdm


logger = logging.getLogger(__file__)


class ClusterEngine(AbstractEngine):
    def __init__(self, show_progress_bar: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = show_progress_bar
        self._progress = tqdm(
            desc=repr(self.operator),
            total=self.num_chunks,
            disable=(not show_progress_bar),
        )

    def apply(self) -> Any:
        iterator = self.iterate()

        self.operator.pre_hooks(self._dataset)

        documents = []
        for chunk in iterator:
            documents += chunk

        new_batch = self._operate(documents)

        # Update this in series
        for chunk_index in self.api_progress(
            range(self.num_chunks), n_total=self.num_chunks
        ):
            start = chunk_index * self.pull_chunksize
            end = (chunk_index + 1) * self._pull_chunksize

            chunk = new_batch[start:end]

            if chunk_index < self.MAX_SCHEMA_UPDATE_LIMITER:
                update_schema = True
            else:
                update_schema = False

            self.update_chunk(
                chunk,
                ingest_in_background=True,
                # Update schema only on the first chunk otherwise it crashes the
                # schema update
                update_schema=update_schema,
            )

        self.set_success_ratio()

        self.operator.post_hooks(self._dataset)
