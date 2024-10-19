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
async def linear(x: int, ctx: WorkflowContext) -> tuple[int, int]:
  x1, x2 = await ctx.all(
    ctx.call(double, x),
    ctx.call(inc, x)
  )
  return x1, x2

if __name__ == '__main__':
  ctx = Context.sqlite('simplest.db')
  Qin = linear.input(ctx)
  Qout = ctx.backend.output(tuple[int, int])

  async def activities():
    for i in range(100):
      await asyncio.sleep(0.1)
      ctx.log(f'Pushing {i}')
      await Qin.push(str(i), i)
  
  def run():
    asyncio.run(activities())

  proc = Process(target=run)
  proc.start()
  linear.run_all(Qout, ctx)
  proc.join()