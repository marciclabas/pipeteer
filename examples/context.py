import asyncio
from multiprocessing import Process
from uuid import uuid4
from pipeteer import task, workflow, Context, WorkflowContext

@task()
async def double(x: int, ctx: Context) -> int:
  ctx.log(f'Doubling {x}')
  return 2*x

@task()
async def inc(x: int) -> int:
  ctx.log(f'Incrementing {x}')
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

  async def tasks():
    for i in range(5):
      ctx.log(f'Pushing {i}')
      await Qin.push(str(i), [1, 2, 3])
      # await asyncio.sleep(0.1)
  
  def run():
    asyncio.run(tasks())

  proc = Process(target=run)
  proc.start()
  series.run_all(Qout, ctx)
  proc.join()