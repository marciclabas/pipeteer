# Task

A `task` is the go-to way to define a completely arbitrary pipeline. You've got access to the underlying table with the tasks.

The most common use case is to expose an API for users to manipulate tasks:

## Example: Manual Approval

```python
from fastapi import FastAPI
from sqlmodel import select
from pipeteer import task, workflow, InputT, Push, Context, WorkflowContext

@task()
async def approve(Inp: InputT[str], push: Push[bool], ctx: Context):

  app = FastAPI()

  @app.get('/tasks')
  def get_tasks() -> list[tuple[str, str]]:
    with ctx.db.session as s:
      tasks = s.exec(select(Inp)).all()
      return [(task.key, task.value) for task in tasks]

  @app.get('/approve/{key}')
  async def approve(key: str):
    await push(key, True)

  @app.get('/reject/{key}')
  async def reject(key: str):
    await push(key, False)

  return app

@workflow()
async def wkf(task: str, ctx: WorkflowContext) -> bool:
  ok = await ctx.call(approve, task)
  return ok
```


## Running

How to run it? Completely up to you. For instance:

```python
import asyncio
from multiprocessing import Process
import uvicorn
from pipeteer import DB, Context

db = DB.at('pipe.db')
ctx = Context.of(db)

procs = [
  Process(target=uvicorn.run, args=[approve,run(ctx)]),
  Process(target=asyncio.run, args=[wkf.run(ctx)]),
]
for proc in procs:
  proc.start()
for proc in procs:
  proc.join()
```