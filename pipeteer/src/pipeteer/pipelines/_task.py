from typing_extensions import TypeVar, Generic, Callable, Any
from dataclasses import dataclass
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import Queue, ReadQueue, WriteQueue
from pipeteer.util import param_type, type_arg, num_params, Func2or3

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = TypeVar('Artifact')

@dataclass
class Task(Pipeline[A, B, Ctx, Artifact], Generic[A, B, Ctx, Artifact]):
  call: Callable[[ReadQueue[A], WriteQueue[B], Ctx], Artifact]

  def input(self, ctx: Ctx, *, prefix: tuple[str, ...] = ()) -> Queue[A]:
    return ctx.backend.queue(prefix + (self.name,), self.Tin)

  def run(self, Qout: WriteQueue[B], ctx: Ctx, /, *, prefix: tuple[str, ...] = ()) -> Artifact:
    Qin = self.input(ctx, prefix=prefix)
    return self.call(Qin, Qout, ctx)
  
def task(name: str | None = None):
  def decorator(fn: Func2or3[ReadQueue[A], WriteQueue[B], Ctx, Artifact]) -> Task[A, B, Ctx, Artifact]:
    Tin = type_arg(param_type(fn, 0) or Any) # type: ignore
    if Tin is None:
      raise TypeError(f'Task {fn.__name__} must have a type hint for its input type')
    
    Tout = type_arg(param_type(fn, 1) or Any) # type: ignore
    if Tout is None:
      raise TypeError(f'Task {fn.__name__} must have a type hint for its output type')
    
    return Task(
      name=name or fn.__name__,
      Tin=Tin, Tout=Tout,
      call=fn if num_params(fn) == 3 else (lambda Qin, Qout, _: fn(Qin, Qout)) # type: ignore
    )
      
  return decorator