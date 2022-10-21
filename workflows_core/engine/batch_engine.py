import concurrent.futures

import logging
import traceback
from typing import Any

from workflows_core.engine.abstract_engine import AbstractEngine

from tqdm.auto import tqdm

logger = logging.getLogger(__file__)


class ClusterEngine(AbstractEngine):
    def __init__(self, show_progress_bar: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._show_progress_bar = show_progress_bar
        self._progress = tqdm(desc=repr(self.operator), total=self.num_chunks * 2)

    def read(self):
        return

    def write(self):
        return

    def apply(self) -> Any:

        iterator = self.iterate()

        documents = []
        for chunk in iterator:
            documents += chunk

        new_batch = self.operator(documents)

        payloads = []
        for i in range(self._num_chunks):
            payload = new_batch[i * self._chunksize : (i + 1) * self._chunksize]
            payloads.append(payload)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.update_chunk, payload) for payload in payloads
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    logging.error(e)
                    logging.error(traceback.format_exc())
                else:
                    logging.debug(result)

        return
