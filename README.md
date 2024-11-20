# Pipeteer

> Simple, explicit durable execution framework.

> [**Read the docs**](https://marciclabas.github.io/pipeteer/)

# Welcome to Pipeteer

Pipeteer **simplifies the complexity of durable execution** whilst *not hiding the underlying persistence*.

## Why Pipeteer?

Use `pipeteer` if you need...

- **Persistance**: your app can stop or crash and resume at any time without losing progress
- **Observability**: you can see the state of your app at any time, and *modify it* programmatically at runtime
- **Exactly-once semantics**: your app can be stopped and resumed without dropping or duplicating work
- **Fault tolerance**: if a task fails, it'll keep working on other tasks and retry it later
- **Explicitness**: `pipeteer`'s high level API is a very thin abstraction over SQLModel (for persistance) and ZeroMQ (for inter-process communication)

## Proof of Concept

**Definition.** You can define a durable workflow this easy:

```python
from pipeteer import activity, workflow, WorkflowContext

@activity()
async def double(x: int) -> int:
  return 2*x

@workflow()
async def quad(x: int, ctx: WorkflowContext) -> int:
  x2 = await ctx.call(double, x)
  x4 = await ctx.call(double, x2)
  return x4
```

**Worker.** And here's how to run it:

```python
import asyncio
from pipeteer import DB, Context

db = DB.at('pipeline.db')
ctx = Context.of(db)

async def main():
  await asyncio.gather(
    double.run(ctx),
    quad.run(ctx),
    ctx.zmq.proxy(),
  )
```

**Input**. How to give it tasks?
  
```python
from pipeteer import DB, Context

db = DB.at('pipeline.db')
ctx = Context.of(db)

Input = quad.input(ctx)
with db.session as s:
  s.add(Input(key='task', value=1, output='my-output'))
  s.commit()

await quad.notify(ctx)
```

**Output**. How to get the results?

```python
from sqlmodel import select
from pipeteer import DB, Context

db = DB.at('pipeline.db')
ctx = Context.of(db)

Output = quad.output(ctx, 'my-output')
while True:
  with db.session as s:
    for entry in s.exec(select(Output)):
      print(f'Output: {entry.key} -> {entry.value}')
      s.delete(entry)
    s.commit()
  await ctx.wait('my-output')
```