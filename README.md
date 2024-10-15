# Pipeteer

> Simple, explicit durable execution framework.

> [**Read the docs**](https://marciclabas.github.io/pipeteer/)

## Why Pipeteer?

Use `pipeteer` if you need...

- **Persistance**: your app can stop or crash and resume at any time without losing progress
- **Observability**: you can see the state of your app at any time, and *modify it* programmatically at runtime
- **Exactly-once semantics**: your app can be stopped and resumed without dropping or duplicating work
- **Fault tolerance**: if a task fails, it'll keep working on other tasks and retry it later
- **Explicit data**: `pipeteer`'s high level API is a very thin abstraction over explicit communication using queues

## Proof of Concept

```python
from pipeteer import activity, workflow, Context, WorkflowContext

@activity()
async def double(x: int) -> int:
  return 2*x

@activity()
async def inc(x: int) -> int:
  return x + 1

@workflow([double, inc])
async def linear(x: int, ctx: WorkflowContext) -> int:
  x2 = await ctx.call(double, x)
  return await ctx.call(inc, x2)

if __name__ == '__main__':
  ctx = Context.sqlite('workflow.db')
  linear.run_all(ctx)
```
