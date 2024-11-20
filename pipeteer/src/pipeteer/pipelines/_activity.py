from typing_extensions import TypeVar, Generic, Callable, Awaitable, Coroutine
from dataclasses import dataclass
import asyncio
from datetime import timedelta, datetime
from multiprocessing import Process
import traceback
from sqlmodel import select, Session, or_
from pipeteer.pipelines import Pipeline, Context, Entry
from pipeteer.util import param_type, return_type, num_params, Func1or2, race

A = TypeVar('A')
B = TypeVar('B')
Ctx = TypeVar('Ctx', bound=Context)

@dataclass
class Activity(Pipeline[A, B, Ctx, Coroutine], Generic[A, B, Ctx]):
  call: Callable[[A, Ctx], Awaitable[B]]
  reserve: timedelta | None = None
  poll_interval: timedelta = timedelta(seconds=1)

  def run(self, ctx: Ctx) -> Coroutine:
    async def loop():
      ctx.log('Running...', level='DEBUG')
      pub = ctx.zmq.pub
      sub = ctx.zmq.sub(self.id)
      Input = self.input(ctx)
      while True:
        try:
          with Session(ctx.db.engine) as s:
            free = or_(Input.ttl == None, Input.ttl < datetime.now()) # type: ignore
            if not (inp := s.exec(select(Input).where(free)).first()):
              await race([
                asyncio.sleep(self.poll_interval.total_seconds()),
                sub.wait()
              ])
              continue
            elif self.reserve:
              inp.ttl = datetime.now() + self.reserve
              s.commit()

            key, val, out = inp.key, inp.value, inp.output
          
          ctx.log(f'Processing "{key}"', level='DEBUG')

          try:
            y = await self.call(val, ctx)
            Output = ctx.db.table(out, Entry(self.Tout))

            with Session(ctx.db.engine) as s:
              s.add(Output(key=key, value=y))
              s.delete(inp)
              s.commit()

            await pub.send(out)

          except Exception:
            ctx.log(f'Error processing "{key}": {traceback.format_exc()}. Value: {val}', level='ERROR')

        except Exception:
          ctx.log(f'Error reading from input queue: {traceback.format_exc()}', level='ERROR')
      
    return loop()
  
def activity(
  id: str | None = None, *,
  reserve: timedelta | None = timedelta(minutes=2),
  poll_interval: timedelta = timedelta(minutes=2)
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
      call=fn if num_params(fn) == 2 else (lambda x, _: fn(x)), # type: ignore
      poll_interval=poll_interval
    )
      
  return decorator