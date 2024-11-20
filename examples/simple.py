import asyncio
import random
from sqlmodel import select
from pipeteer import DB, Context, activity, workflow, WorkflowContext, Entry

@activity()
async def double(x: int) -> int:
  return 2*x

@workflow()
async def quad(x: int, ctx: WorkflowContext) -> int:
  x2 = await ctx.call(double, x)
  x2 = x2 if random.random() < 0.5 else 2*x2
  x4 = await ctx.call(double, x2)
  return x4

if __name__ == '__main__':
  db = DB.at('pipe.db')
  ctx = Context.of(db)

  async def inputs():
    Input = quad.input(ctx)
    await asyncio.sleep(2)
    with db.session as s:
      for i in range(10):
        print(f'Pushing {i}')
        s.add(Input(key=f'key-{i}', value=i))
      s.commit()
    await quad.notify(ctx)

  async def listener():
    Output = quad.output(ctx)
    while True:
      with db.session as s:
        for entry in s.exec(select(Output)):
          print(f'Output: {entry.key} -> {entry.value}')
          s.delete(entry)
        s.commit()
      await ctx.wait('output')

  async def run():
    await asyncio.gather(
      quad.run(ctx),
      double.run(ctx),
      ctx.zmq.proxy(),
      inputs(),
      listener()
    )

  asyncio.run(run())