import asyncio
from datetime import timedelta
from multiprocessing import Process
from sqlmodel import Session
from pipeteer import activity, workflow, DB, Context, WorkflowContext

@activity(poll_interval=timedelta(minutes=1))
async def double(x: int) -> int:
  return 2*x

@activity(poll_interval=timedelta(minutes=1))
async def inc(x: int) -> int:
  return x + 1

@workflow(poll_interval=timedelta(minutes=1))
async def linear(x: int, ctx: WorkflowContext) -> int:
  x = await ctx.call(double, x)
  x = await ctx.call(inc, x)
  return x

@workflow(poll_interval=timedelta(minutes=1))
async def series(xs: list[int], ctx: WorkflowContext) -> int:
  acc = 0
  for x in xs:
    acc += await ctx.call(linear, x)
  return acc

if __name__ == '__main__':

  db = DB.at('complex.db')
  ctx = Context.of(db)

  async def pusher():
    Input = series.input(ctx)
    pub = ctx.zmq.pub
    with Session(db.engine) as s:
      for i in range(5):
          print(f'Pushing {i}')
          s.add(Input(key=str(i), value=[j+i for j in range(5)], output='output'))
      s.commit()
      await pub.send(series.id)

  coros = [
    pusher(),
    double.run(ctx.prefix('double')),
    inc.run(ctx.prefix('inc')),
    linear.run(ctx.prefix('linear')),
    series.run(ctx.prefix('series')),
    ctx.zmq.proxy(),
  ]
  procs = [Process(target=asyncio.run, args=[coro]) for coro in coros]

  for proc in procs:
    proc.start()

  for proc in procs:
    proc.join()
