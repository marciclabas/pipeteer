import asyncio
from multiprocessing import Process
from uuid import uuid4
from fastapi import FastAPI
import uvicorn
from pipeteer import activity, task, workflow, Context, WorkflowContext, ReadQueue, WriteQueue

@task()
def manual_validate(Qin: ReadQueue[str], Qout: WriteQueue[bool]):
  app = FastAPI()

  @app.get('/tasks')
  async def get_tasks() -> list[tuple[str, str]]:
    return [t async for t in Qin.items()]

  @app.post('/tasks/{key}')
  async def review(key: str, approve: bool):
    await Qout.push(key, approve)
    await Qin.pop(key)

  return app

@activity()
async def autopreprocess(x: str) -> str:
  ctx.log(f'Preprocessing {x}')
  return f'Preprocessed {x}'

@workflow([autopreprocess, manual_validate])
async def preprocess(x: str, ctx: WorkflowContext) -> str:
  while True:
    y = await ctx.call(autopreprocess, x)
    ok = await ctx.call(manual_validate, y)
    if ok:
      return y
    
def executor(path, artifact):
  if isinstance(artifact, FastAPI):
    return Process(target=uvicorn.run, args=(artifact,))
  else:
    return artifact()

if __name__ == '__main__':
  ctx = Context.sqlite('simplest.db')
  Qin = preprocess.input(ctx)
  Qout = ctx.backend.output(str)

  async def activitys():
    for i in range(5):
      ctx.log(f'Pushing {i}')
      await Qin.push(str(i), str(i))
      # await asyncio.sleep(0.1)
  
  def run():
    asyncio.run(activitys())

  proc = Process(target=run)
  proc.start()
  preprocess.run_all(Qout, ctx, executor=executor)
  proc.join()