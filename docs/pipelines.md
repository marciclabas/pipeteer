# Pipelines

Pipelines are **units of computation**. Every pipeline:

- Has an *input (read)* and an *output (write) queue*.
- Can be *composed* into larger pipelines.

## Pipeline ABC

The pipeline interface is roughly defined as:

```python
from multiprocessing import Process
from pipeteer import ReadQueue, WriteQueue

class Pipeline(Generic[A, B]):
  def run(self, Qin: ReadQueue[A], Qout: WriteQueue[B]) -> Process:
    ...
```