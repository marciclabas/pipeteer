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
  async def run(self, Qin: ReadQueue[A], Qout: WriteQueue[B]):
    ...
```

Composition in `pipeteer` works by connecting the output of one pipeline to the input of another, creating arbitrary graphs.

Let's see a few concrete pipelines.

---

## Activity

Activities perform a 1-to-1 transformation. You'll generally define them using the `@activity` decorator.

```python
from pipeteer import activity

@activity()
async def double(x: int) -> int:
  return 2*x
```

But don't be fooled! This is roughly equivalent to:
  
```python
from timedelta import timedelta
from pipeteer import Pipeline

class Double(Pipeline[int, int]):
  async def run(self, Qin: ReadQueue[A], Qout: WriteQueue[B]):
    while True:
      key, x = await Qin.wait_any(reserve=timedelta(minutes=2))
      await Qout.push(key, 2*x)
      await Qin.pop(key)
```

---

## Task

Tasks are a bit more general, giving you explicit control over the underlying queues. A common use case is for external APIs and human intervention:

```python
from multiprocessing import Process
from fastapi import FastAPI
from pipeteer import task, ReadQueue, WriteQueue

@task
def manual_review(Qin: ReadQueue[str], Qout: WriteQueue[bool]):
  app = FastAPI()

  @app.get('/tasks')
  async def get_tasks() -> list[tuple[str, str]]:
    return [t async for t in Qin.items()]

  @app.post('/tasks/{key}')
  async def review(key: str, approve: bool):
    await Qout.push(key, approve)
    await Qin.pop(key)

  return app
```

---

## Workflow

Workflows are pipelines that can perform arbitrary transformations. You'll generally define them using the `@workflow` decorator.

```python
from pipeteer import workflow, WorkflowContext

@workflow([double])
async def quad(x: int, ctx: WorkflowContext) -> int:
  x2 = await ctx.call(double, x)
  return await ctx.call(double, x2)
```

But again, this maps out to a `Qin` + `Qout` function! It's a bit more complex this time around, but the idea is the same:

> `workflow` is just a fancy way to compose pipelines: you could achieve the same by connecting queues manually.

### A note on durable execution

Workflow is an example of the "durable execution" concept. This is how it works:

1. You call `ctx.call(double, x)`
2. Under the hood, `pipeteer` pushes `x` to `double`'s input queue, and stops the execution.
3. When the result is received, `pipeteer` will re-run `quad`. When you `ctx.call`, it will immediately return the previously computed result.

This happens at every `ctx.call`: the function is re-run using the cached results.

---

You may have noticed that we `Qout.push`ed and then `Qin.pop`ed: but what if the second fails!? That's were [transactions come in](transactions.md).