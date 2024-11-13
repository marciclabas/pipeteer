from typing_extensions import Mapping, TypeVar, Generic, Callable, Awaitable
from dataclasses import dataclass
import asyncio
from datetime import timedelta
from multiprocessing import Process
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import Queue, Transaction, Routed
from pipeteer.util import param_type, return_type, num_params, Func1or2

A = TypeVar('A')
B = TypeVar('B')
Ctx = TypeVar('Ctx', bound=Context)

@dataclass
class Activity(Pipeline[A, B, Ctx, Process], Generic[A, B, Ctx]):
  call: Callable[[A, Ctx], Awaitable[B]]
  reserve: timedelta | None = None

  def input(self, ctx: Ctx) -> Queue[Routed[A]]:
    return ctx.backend.queue(self.id, Routed[self.Tin])
  
  def observe(self, ctx: Ctx) -> Mapping[str, Queue]:
    return { 'input': self.input(ctx) }

  def run(self, ctx: Ctx) -> Process:
    async def loop():
      Qin = self.input(ctx)
      while True:
        try:
          k, x = await Qin.wait_any(reserve=self.reserve)
          ctx.log(f'Processing "{k}" with input {x["value"]}', level='DEBUG')
          try:
            y = await self.call(x['value'], ctx)
            Qout = ctx.backend.queue_at(x['url'], self.Tout)
            async with Transaction(Qin, Qout, autocommit=True):
              await Qout.push(k, y)
              await Qin.pop(k)

          except Exception as e:
            ctx.log(f'Error processing "{k}": {e}. Value: {x["value"]}', level='ERROR')

        except Exception as e:
          ctx.log(f'Error reading from input queue: {e}', level='ERROR')
      
    def runner():
      asyncio.run(loop())

    return Process(target=runner)

def activity(
  id: str | None = None, *,
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
      Tin=Tin or param_type(fn), Tout=Tout or return_type(fn), reserve=reserve, id=id or fn.__name__,
      call=fn if num_params(fn) == 2 else (lambda x, _: fn(x)) # type: ignore
    )
      
  return decorator