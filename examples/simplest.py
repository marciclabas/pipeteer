import asyncio
from multiprocessing import Process
from uuid import uuid4
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
  x3 = await ctx.call(inc, x2)
  return x3

@workflow([linear])
async def series(xs: list[int], ctx: WorkflowContext) -> int:
  acc = 0
  for x in xs:
    acc += await ctx.call(linear, x)
  return acc

if __name__ == '__main__':
  ctx = Context.sqlite('simplest.db')
  Qin = series.input(ctx)
  Qout = ctx.backend.output(int)

  async def activitys():
    for i in range(5):
      ctx.log(f'Pushing {i}')
      await Qin.push(str(i), [1, 2, 3])
      # await asyncio.sleep(0.1)
  
  def run():
    asyncio.run(activitys())

  proc = Process(target=run)
  proc.start()
  series.run_all(Qout, ctx)
  proc.join()