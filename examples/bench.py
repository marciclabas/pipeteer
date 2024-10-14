import asyncio
from multiprocessing import Process
from uuid import uuid4
from pipeteer import task, workflow, Context, WorkflowContext

@task()
async def double(x: int) -> int:
  return 2*x


@workflow([double])
async def bench(x: int, ctx: WorkflowContext) -> int:
  return await ctx.call(double, x)


if __name__ == '__main__':
  ctx = Context.sqlite('wkf.db')
  Qin = bench.input(ctx)
  Qout = ctx.backend.output(int)

  async def tasks():
    for i in range(100):
      ctx.log(f'Pushing {i}')
      await Qin.push(str(i), i)
      # await asyncio.sleep(0.1)
  
  def run():
    asyncio.run(tasks())

  proc = Process(target=run)
  proc.start()
  bench.run_all(Qout, ctx)
  proc.join()