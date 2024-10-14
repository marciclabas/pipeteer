import asyncio
from threading import Thread
from pipeteer import task, workflow, Context, WorkflowContext, Backend

class MyContext(Context):
  def log(self, msg: str):
    print(msg)

@task()
async def double(x: int, ctx: MyContext) -> int:
  ctx.log(f'Doubling {x}')
  return 2*x

@task()
async def inc(x: int, ctx: MyContext) -> int:
  ctx.log(f'Incrementing {x}')
  return x + 1

@workflow([double, inc])
async def linear(x: int, ctx: WorkflowContext) -> int: # 2x + 1
  x2 = await ctx.call(double, x)
  x3 = await ctx.call(inc, x2)
  return x3

@workflow([linear])
async def series(xs: list[int], ctx: WorkflowContext) -> int: # sum(2x + 1)
  acc = 0
  for x in xs:
    acc += await ctx.call(linear, x)
  return acc


if __name__ == '__main__':
  backend = Backend.sqlite('wkf.db')
  ctx = MyContext(backend)
  Qin = series.input(ctx)
  Qout = ctx.backend.output(int)

  async def tasks():
    for i in range(10000):
      await Qin.push(f'task-{i}', [1, 2, 3])
      await asyncio.sleep(5)

  thread = Thread(target=asyncio.run, args=(tasks(),)).start()
  series.run_all(Qout, ctx)