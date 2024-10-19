from typing import Generic, TypeVar
from dataclasses import dataclass
import asyncio
from multiprocessing import Process
from fastapi import FastAPI
import uvicorn
from pipeteer import ReadQueue, WriteQueue, task, workflow, multitask, WorkflowContext, Context

T = TypeVar('T')

@dataclass
class Api(Generic[T]):
  Qin: ReadQueue[T]
  Qout: WriteQueue[T]

  async def items(self) -> list[tuple[str, T]]:
    return [x async for x in self.Qin.items()]
  
  async def resolve(self, key: str, val: T):
    ok = await self.Qin.has(key)
    if ok:
      await self.Qout.push(key, val)
      await self.Qin.pop(key)
    return ok


@task()
def api1(Qin: ReadQueue[str], Qout: WriteQueue[str]) -> Api[str]:
  return Api(Qin, Qout)

@task()
def api2(Qin: ReadQueue[int], Qout: WriteQueue[int]) -> Api[int]:
  return Api(Qin, Qout)

@multitask(api1, api2)
def multi_api(artif1: Api[str], artif2: Api[int]):
  app = FastAPI()

  @app.get('/items')
  async def items():
    xs1, xs2 = await asyncio.gather(artif1.items(), artif2.items())
    return xs1 + xs2
  
  @app.get('/resolve/1/{key}')
  async def resolve1(key: str, val: str):
    ok = await artif1.resolve(key, val)
    return 'OK' if ok else 'Not found'
  
  @app.get('/resolve/2/{key}')
  async def resolve2(key: str, val: int):
    ok = await artif2.resolve(key, val)
    return 'OK' if ok else 'Not found'
  
  return app

@workflow([multi_api])
async def api_wkf(x: str, ctx: WorkflowContext):
  x2 = await ctx.call(api1, x)
  x3 = await ctx.call(api2, len(x2))
  return str(x3)

def executor(path, artifact):
  if isinstance(artifact, FastAPI):
    return Process(target=uvicorn.run, args=(artifact,))
  else:
    return artifact()

if __name__ == '__main__':
  ctx = Context.sqlite('multi.db')
  Qin = api_wkf.input(ctx)
  Qout = ctx.backend.output(str)

  async def activities():
    for i in range(5):
      await asyncio.sleep(0.1)
      ctx.log(f'Pushing {i}')
      await Qin.push(str(i), str(i))
  
  def run():
    asyncio.run(activities())

  proc = Process(target=run)
  proc.start()
  api_wkf.run_all(Qout, ctx, executor=executor)
  proc.join()
