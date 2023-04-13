import logging

from typing import Any

from ai_transform.engine.abstract_engine import AbstractEngine

logger = logging.getLogger(__file__)


class InMemoryEngine(AbstractEngine):
    def __init__(self, show_progress_bar: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = show_progress_bar

    def apply(self) -> Any:
        iterator = self.get_iterator()

        self.operator.pre_hooks(self._dataset)

        documents = []
        for batch in iterator:
            documents += batch

        documents_to_insert = self._operate(documents)

        # Update this in series
        for batch_index, batch in enumerate(
            self.api_progress(
                AbstractEngine.chunk_documents(self.pull_chunksize, documents_to_insert),
                show_progress_bar=self._show_progress_bar,
            )
        ):
            if batch_index < self.MAX_SCHEMA_UPDATE_LIMITER:
                update_schema = True
            else:
                update_schema = False

            self.update_chunk(
                batch,
                ingest_in_background=True,
                # Update schema only on the first chunk otherwise it crashes the
                # schema update
                update_schema=update_schema,
            )

        self.operator.post_hooks(self._dataset)
