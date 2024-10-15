from typing_extensions import TypeVar, Generic, Callable, Awaitable
from dataclasses import dataclass
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, promise as P
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import ReadQueue, WriteQueue, Transaction
from pipeteer.util import param_type, num_params, Func1or2

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = Callable[[], Process]

@dataclass
class Activity(Pipeline[A, B, Ctx, Artifact], Generic[A, B, Ctx]):
  call: Callable[[A, Ctx], Awaitable[B]]
  reserve: timedelta | None = None

  def run(self, Qout: WriteQueue[B], ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    Qin = self.input(ctx, prefix=prefix)
    ctx = ctx.prefix(prefix + (self.name,))

    @P.run
    async def runner(Qin: ReadQueue[A], Qout: WriteQueue[B]):
      while True:
        try:
          k, x = await Qin.wait_any(reserve=self.reserve)
          y = await self.call(x, ctx)
          async with Transaction(Qin, Qout, autocommit=True):
            await Qout.push(k, y)
            await Qin.pop(k)

        except Exception as e:
          ctx.log(f'Error: {e}', level='ERROR')

    return { self.name: lambda: Process(target=runner, args=(Qin, Qout)) }

def activity(name: str | None = None, *, reserve: timedelta | None = timedelta(minutes=2)):
  def decorator(fn: Func1or2[A, Ctx, Awaitable[B]]) -> Pipeline[A, B, Ctx, Artifact]:
    return Activity(
      type=param_type(fn), reserve=reserve, name=name or fn.__name__,
      call=fn if num_params(fn) == 2 else (lambda x, _: fn(x)) # type: ignore
    )
      
  return decorator