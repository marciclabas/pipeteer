from multiprocessing import Process
from haskellian import trees, promise as P
from pipeteer import task, workflow, Context, WorkflowContext, Backend

class MyContext(Context):
  def log(self, *objs, level: str = 'INFO'):
    print(f'[{level}]', *objs)

@task()
async def double(x: int, ctx: MyContext) -> int:
  ctx.log(f'Doubling {x}...')
  return 2*x

@task()
async def inc(x: int, ctx: MyContext) -> int:
  ctx.log(f'Incrementing {x}...')
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
  Qout = backend.output(int)
  procs = series.run(Qout, ctx)

  @P.run
  async def run():
    await Qin.push('hello', [1, 2, 3])

  procs['client'] = lambda: Process(target=run) # type: ignore
  procs = trees.map(procs, lambda proc: proc())

  for path, proc in trees.flatten(procs):
    name = path and '/'.join(path) or 'root'
    print(f'Starting {name}...')
    proc.start()

  for _, proc in trees.flatten(procs):
    proc.join()