from typing_extensions import TypeVar, Generic, Callable, Any, Awaitable, Protocol
from dataclasses import dataclass
from sqlmodel import Session
from pipeteer.pipelines import Pipeline, Context, Entry, InputT
from pipeteer.util import param_type, type_arg, num_params, Func2or3

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D', contravariant=True)
Ctx = TypeVar('Ctx', bound=Context)
Artifact = TypeVar('Artifact')

class Push(Protocol, Generic[D]):
  async def __call__(self, key: str, val: D) -> bool:
    ...

@dataclass
class Task(Pipeline[A, B, Ctx, Artifact], Generic[A, B, Ctx, Artifact]):
  call: Callable[[type[InputT[A]], Push[B], Ctx], Artifact]

  def run(self, ctx: Ctx, /):
    Input = self.input(ctx)
    async def push(key: str, val: B):
      with Session(ctx.db.engine) as s:
        if (inp := s.get(Input, key)) is None:
          return False
        else:
          out = inp.output
          Output = ctx.db.table(out, Entry(self.Tout))
          s.add(Output(key=key, value=val))
          s.delete(inp)
          s.commit()
      
      await ctx.zmq.pub.send(out)
      return True

    return self.call(Input, push, ctx)

def task(id: str | None = None):
  def decorator(fn: Func2or3[type[InputT[A]], Push[B], Ctx, Artifact]) -> Task[A, B, Ctx, Artifact]:
    Tin = type_arg(type_arg(param_type(fn, 0) or Any)) # type: ignore
    if Tin is None:
      raise TypeError(f'Task {fn.__name__} must have a type hint for its input type')
    
    Tout = type_arg(param_type(fn, 1) or Any) # type: ignore
    if Tout is None:
      raise TypeError(f'Task {fn.__name__} must have a type hint for its output type')
    
    return Task(
      id=id or fn.__name__,
      Tin=Tin, Tout=Tout,
      call=fn if num_params(fn) == 3 else (lambda Qin, Qout, _: fn(Qin, Qout)) # type: ignore
    )
      
  return decorator