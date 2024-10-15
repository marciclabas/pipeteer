from typing_extensions import TypeVar, Generic, Callable
from dataclasses import dataclass
from haskellian import Tree
from pipeteer.pipelines import Pipeline, Context
from pipeteer.queues import ReadQueue, WriteQueue
from pipeteer.util import param_type, type_arg, num_params, Func2or3

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
Ctx = TypeVar('Ctx', bound=Context)
Artifact = TypeVar('Artifact')

@dataclass
class Task(Pipeline[A, B, Ctx, Artifact], Generic[A, B, Ctx, Artifact]):
  call: Callable[[ReadQueue[A], WriteQueue[B], Ctx], Tree[Artifact]]

  def run(self, Qout: WriteQueue[B], ctx: Ctx, /, *, prefix: tuple[str, ...] = ()) -> Tree[Artifact]:
    Qin = self.input(ctx, prefix=prefix)
    return self.call(Qin, Qout, ctx)
  
def task(name: str | None = None):
  def decorator(fn: Func2or3[ReadQueue[A], WriteQueue[B], Ctx, Tree[Artifact]]) -> Task[A, B, Ctx, Artifact]:
    return Task(
      name=name or fn.__name__,
      type=type_arg(param_type(fn)),
      call=fn if num_params(fn) == 3 else (lambda Qin, Qout, _: fn(Qin, Qout)) # type: ignore
    )
      
  return decorator