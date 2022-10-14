# Relevance Workflows Core

Below is a hierarchy diagram for all the moving parts of a workflow.

![hierarchy](hierarchy.png "Hierarchy")

## 🛠️ Installation

Fresh install

```{bash}
pip install RelevanceAI-Workflows-Core
```

to upgrade to the latest version

```{bash}
pip install --upgrade RelevanceAI-Workflows-Core
```

## 🏃Quickstart

To get started, please refer to the example scripts in `scripts/`

```python

import random
from workflows_core.api.client import Clien
from workflows_core.engine.stable_engine import StableEngine
from workflows_core.workflow.helpers import decode_workflow_token
from workflows_core.workflow import Workflow
from workflows_core.operator.abstract_operator import AbstractOperator
from workflows_core.utils.random import Document

class RandomOperator(AbstractOperator):
    def __init__(self, upper_bound: int=10):
        self.upper_bound = upper_bound
    
    def transform(self, documents):
        for d in documents:
            d['random_number'] = random.randint(0, self.upper_bound)


client = Client()
ds = client.Dataset("sample_dataset")
operator = RandomOperator()

engine = StableEngine(
    dataset=ds,
    operator=operator,
    chunksize=10,
    filters=[],
)
workflow = Workflow(engine)

workflow.run()
```

## Workflow IDs and Job IDs

Workflows have Workflow IDs such as sentiment  - for example:
sentiment.py is called sentiment and this is how the frontend triggers it.
Workflow Name is what we call the workflow like Extract Sentiment .
Each instance of a workflow is a job and these have job_id so we can track their status.

