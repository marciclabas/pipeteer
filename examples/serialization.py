import asyncio
from datetime import timedelta
from multiprocessing import Process
from dataclasses import dataclass
from sqlmodel import Session
import random
from pipeteer import activity, workflow, DB, Context, WorkflowContext

@dataclass
class Task:
  hello: str
  value: list[int]

@dataclass
class Result:
  hello: str
  value: int

@activity(poll_interval=timedelta(minutes=1))
async def agg(x: Task) -> Result:
  return Result(hello=x.hello, value=sum(x.value))


@workflow(poll_interval=timedelta(minutes=1))
async def wkf(xs: list[Task], ctx: WorkflowContext) -> list[Result]:
  out = []
  for x in xs:
    if random.random() < 0.1:
      x = Task(hello='random', value=[-1, -2, -3])
    out.append(await ctx.call(agg, x))
  return out


if __name__ == '__main__':

  db = DB.at('pipe.db')
  ctx = Context.of(db)

  async def pusher():
    Input = wkf.input(ctx)
    pub = ctx.zmq.pub
    with Session(db.engine) as s:
      for i in range(5):
          print(f'Pushing {i}')
          s.add(Input(key=str(i), value=[
            Task(f'hello-{i}', [1, 2, 3]),
            Task(f'bye-{i}', [4, 5, 6]),
          ], output='output'))
      s.commit()
      await pub.send(wkf.id)

  coros = [
    pusher(),
    agg.run(ctx.prefix('agg')),
    wkf.run(ctx.prefix('wkf')),
    ctx.zmq.proxy(),
  ]
  procs = [Process(target=asyncio.run, args=[coro]) for coro in coros]

  for proc in procs:
    proc.start()

  for proc in procs:
    proc.join()