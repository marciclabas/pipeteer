from typing_extensions import TypeVar, Generic, Callable, Awaitable
from dataclasses import dataclass
from datetime import timedelta
from multiprocessing import Process
from haskellian import Tree, promise as P
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import Queue, ReadQueue, WriteQueue, Transaction
from pipeteer.util import param_type, return_type, num_params, Func1or2

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = Callable[[], Process]

@dataclass
class Activity(Pipeline[A, B, Ctx, Artifact], Generic[A, B, Ctx]):
  call: Callable[[A, Ctx], Awaitable[B]]
  reserve: timedelta | None = None

  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Queue[A]:
    return ctx.backend.queue(prefix + (self.name,), self.Tin)

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

def activity(
  name: str | None = None, *,
  reserve: timedelta | None = timedelta(minutes=2),
):
  def decorator(fn: Func1or2[A, Ctx, Awaitable[B]]) -> Activity[A, B, Ctx]:
    Tin = param_type(fn)
    if Tin is None:
      raise TypeError(f'Activity {fn.__name__} must have a type hint for its input parameter')

    Tout = return_type(fn)
    if Tout is None:
      raise TypeError(f'Activity {fn.__name__} must have a type hint for its return value')

    return Activity(
      Tin=Tin or param_type(fn), Tout=Tout or return_type(fn), reserve=reserve, name=name or fn.__name__,
      call=fn if num_params(fn) == 2 else (lambda x, _: fn(x)) # type: ignore
    )
      
  return decorator