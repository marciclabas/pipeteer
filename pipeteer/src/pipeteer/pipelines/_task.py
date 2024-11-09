from typing_extensions import TypeVar, Generic, Callable, Any
from dataclasses import dataclass
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import Queue, ReadQueue, WriteQueue, Routed, RoutedQueue, ops
from pipeteer.util import param_type, type_arg, num_params, Func2or3

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = TypeVar('Artifact')

@dataclass
class Task(Pipeline[A, B, Ctx, Artifact], Generic[A, B, Ctx, Artifact]):
  call: Callable[[ReadQueue[A], WriteQueue[B], Ctx], Artifact]

  def Qurls(self, ctx: Ctx) -> Queue[str]:
    return ctx.backend.queue(self.id+'-urls', str)
  
  def Qin(self, ctx: Ctx) -> Queue[A]:
    return ctx.backend.queue(self.id, self.Tin)

  def input(self, ctx: Ctx) -> WriteQueue[Routed[A]]:
    return ops.tee(
      self.Qurls(ctx).premap(lambda x: x['url']),
      self.Qin(ctx).premap(lambda x: x['value'])
    )
  
  def observe(self, ctx: Ctx):
    return { 'input': self.Qin(ctx), 'urls': self.Qurls(ctx) }
  
  def run(self, ctx: Ctx, /):
    Qin = self.Qin(ctx)
    Qout = RoutedQueue(self.Qurls(ctx), lambda url: ctx.backend.queue_at(url, self.Tout))
    return self.call(Qin, Qout, ctx)

def task(id: str | None = None):
  def decorator(fn: Func2or3[ReadQueue[A], WriteQueue[B], Ctx, Artifact]) -> Task[A, B, Ctx, Artifact]:
    Tin = type_arg(param_type(fn, 0) or Any) # type: ignore
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